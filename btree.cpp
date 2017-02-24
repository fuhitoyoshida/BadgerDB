/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <string.h>
#include <cstring>
#include <cstdio>
#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_not_pinned_exception.h"

//#define DEBUG

namespace badgerdb
{

	// -----------------------------------------------------------------------------
	// BTreeIndex::BTreeIndex -- Constructor
	// -----------------------------------------------------------------------------


	BTreeIndex::BTreeIndex(const std::string & relationName,
			std::string & outIndexName,
			BufMgr *bufMgrIn,
			const int attrByteOffset,
			const Datatype attrType)
	{
		//set basic fields
		this->bufMgr = bufMgrIn;
		this->scanExecuting = false; 
		this->attributeType = attrType;
		this->attrByteOffset = attrByteOffset;

		// set occupancy
		if (attrType == INTEGER) {
			this->leafOccupancy = INTARRAYLEAFSIZE;
			this->nodeOccupancy = INTARRAYNONLEAFSIZE;
		}
		else if (attrType == DOUBLE) {
			this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
			this->nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
		}
		else if (attrType == STRING) {
			this->leafOccupancy = STRINGARRAYLEAFSIZE;
			this->nodeOccupancy = STRINGARRAYNONLEAFSIZE; 
		}

		// find the index file name
		std::ostringstream idxStr;
		idxStr << relationName << '.' << attrByteOffset;
		outIndexName = idxStr.str();

		Page* metaPage; // header page
		Page* rootPage; // root page
		IndexMetaInfo* metaInfo;

		//open file
		if (File::exists(outIndexName)) {
			file = new BlobFile(outIndexName, false);
			headerPageNum = file->getFirstPageNo();
			bufMgr->readPage(file, headerPageNum, metaPage);
			metaInfo = (IndexMetaInfo*)metaPage;
			rootPageNum = metaInfo->rootPageNo;
			onlyRoot = (metaInfo->rootPageNo == 2);
			bufMgr->unPinPage(file, headerPageNum, false);

		}
		//create new file
		else {
			file = new BlobFile(outIndexName, true);
			onlyRoot = true;
			bufMgr->allocPage(file, headerPageNum, metaPage);
			bufMgr->allocPage(file, rootPageNum, rootPage);

			//set metaPage
			metaInfo = (IndexMetaInfo*)metaPage;
			metaInfo->attrByteOffset = attrByteOffset;
			metaInfo->attrType = attributeType;
			metaInfo->rootPageNo = rootPageNum;
			strcpy(metaInfo->relationName, relationName.c_str());
			//Initialize right sibling
			if(attrType == INTEGER) {
				((LeafNodeInt*)rootPage)->rightSibPageNo = 0;

			}
			else if(attrType == DOUBLE) {
				((LeafNodeDouble*)rootPage)->rightSibPageNo = 0;
			}
			else {
				((LeafNodeString*)rootPage)->rightSibPageNo = 0;
			}
			//unpin
			bufMgr->unPinPage(file, headerPageNum, true);
			bufMgr->unPinPage(file, rootPageNum, true);
			FileScan* scr = new FileScan(relationName, bufMgr);
			while(1) {
				RecordId outRid;

				try{
					//scan one by one and find the key
					scr->scanNext(outRid);
					std::string record = scr->getRecord();
					const char* recordC = record.c_str();
					void* key = (void*)(recordC+attrByteOffset);
					//insert one by one
					insertEntry(key,outRid);
				}
				catch(EndOfFileException e) {
					break;
				}
			}
			bufMgr->flushFile(file);
			delete scr;

		}
	}



	// -----------------------------------------------------------------------------
	// BTreeIndex::~BTreeIndex -- destructor
	// -----------------------------------------------------------------------------

	BTreeIndex::~BTreeIndex()
	{
		this->bufMgr->flushFile(this->file);
		this->scanExecuting = false;
		delete this->file;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertEntry
	// -----------------------------------------------------------------------------

	const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
	{

		if (this->attributeType == INTEGER) {
			RIDKeyPair<int> newPair;
			newPair.set(rid, *((int*)(key)));
			//only root node in the tree
			if (onlyRoot){


				Page* leafPage;
				LeafNodeInt* leafNode;
				//find the node
				bufMgr->readPage(file, this->rootPageNum, leafPage);
				PageId rootNo = rootPageNum;
				leafNode = (LeafNodeInt*) leafPage;


				if (leafNode->ridArray[leafOccupancy-1].page_number == 0 ) {
					insertLeaf<struct LeafNodeInt,RIDKeyPair<int>> (leafNode,newPair);
				}
				else {
					// splite leaf node
					PageKeyPair<int> splitPage;
					splitLeaf<struct LeafNodeInt,PageKeyPair<int>,RIDKeyPair<int>>(leafNode, newPair, splitPage);

					// create a new root since the old one is splitted
					createNewRoot<struct LeafNodeInt, struct NonLeafNodeInt,PageKeyPair<int>,RIDKeyPair<int>>(rootPageNum, splitPage, 1);

				}
				bufMgr->unPinPage(this->file, rootNo, true);


			}
			//not only one node in tree
			else {	
				Page* rootPage;
				PageKeyPair<int>newPagePair;
				newPagePair.set(0,newPair.key);
				//find the correct node
				start<int, struct LeafNodeInt,struct NonLeafNodeInt,PageKeyPair<int>,RIDKeyPair<int>>(this->rootPageNum, newPagePair, newPair);
				PageId rootNo = rootPageNum;
				bufMgr->readPage(file, rootPageNum, rootPage);

				// if split happens
				if (newPagePair.pageNo != 0) {
					createNewRoot<struct LeafNodeInt,struct NonLeafNodeInt,PageKeyPair<int>,RIDKeyPair<int>>(rootPageNum, newPagePair, 0);
				}
				bufMgr->unPinPage(this->file, rootNo, true);
			}
		}
		// other attribute types
		else if (this->attributeType == DOUBLE) {
			RIDKeyPair<double> newPair;
			newPair.set(rid, *((double*)(key)));
			if (onlyRoot) {
				Page* leafPage;
				LeafNodeDouble* leafNode;

				bufMgr->readPage(file, this->rootPageNum, leafPage);
				PageId rootNo = rootPageNum;
				leafNode = (LeafNodeDouble*) leafPage;

				// If rootLeaf is not full
				if (leafNode->ridArray[leafOccupancy-1].page_number == 0 ) {
					insertLeaf<LeafNodeDouble,RIDKeyPair<double>> (leafNode,newPair);
				}
				else {
					// splite leaf node
					PageKeyPair<double> splitPage;
					splitLeaf<LeafNodeDouble,PageKeyPair<double>,RIDKeyPair<double>>(leafNode, newPair, splitPage);

					// create a new root
					createNewRoot<LeafNodeDouble,NonLeafNodeDouble,PageKeyPair<double>,RIDKeyPair<double>>(rootPageNum, splitPage, 1);

				}
				bufMgr->unPinPage(this->file, rootNo, true);
			}
			else {
				Page* rootPage;
				PageKeyPair<double>newPagePair;
				newPagePair.set(0,newPair.key);
				start<double, struct LeafNodeDouble,struct NonLeafNodeDouble,PageKeyPair<double>,RIDKeyPair<double>>(this->rootPageNum, newPagePair, newPair);

				PageId rootNo = rootPageNum;
				bufMgr->readPage(file, rootPageNum, rootPage);

				if (newPagePair.pageNo!= 0) {
					createNewRoot<struct LeafNodeDouble,struct NonLeafNodeDouble,PageKeyPair<double>,RIDKeyPair<double>>(rootPageNum, newPagePair, 0);

				}
				bufMgr->unPinPage(this->file, rootNo, true);
			}
		}
		else {
			RIDKeyPair<char*> newPair;
			char* s = (char*)malloc(STRINGSIZE);
			snprintf(s, STRINGSIZE,"%s",(char*)key);
			newPair.set(rid, s);
			if(onlyRoot){
				Page* leafPage;
				LeafNodeString* leafNode;

				bufMgr->readPage(file, this->rootPageNum, leafPage);
				PageId rootNo = rootPageNum;
				leafNode = (LeafNodeString*) leafPage;

				// If rootLeaf is not full
				if (leafNode->ridArray[leafOccupancy-1].page_number == 0 ) {
					insertLeaf<LeafNodeString,RIDKeyPair<char*>> (leafNode,newPair);
				}
				else {
					// splite leaf node
					PageKeyPair<char*> splitPage;
					splitLeaf<LeafNodeString,PageKeyPair<char*>,RIDKeyPair<char*>>(leafNode, newPair, splitPage);

					// create a new root
					createNewRoot<LeafNodeString,NonLeafNodeString,PageKeyPair<char*>,RIDKeyPair<char*>>(rootPageNum, splitPage, 1);

				}
				bufMgr->unPinPage(this->file, rootNo, true);
			}
			else{
				Page* rootPage;
				PageKeyPair<char*>newPagePair;
				newPagePair.set(0,newPair.key);
				PageId rootNo = this->rootPageNum;
				start<char*, struct LeafNodeString,struct NonLeafNodeString,PageKeyPair<char*>,RIDKeyPair<char*>>(rootPageNum, newPagePair, newPair);

				bufMgr->readPage(file, rootPageNum, rootPage);

				if (newPagePair.pageNo!= 0) {
					createNewRoot<struct LeafNodeString,struct NonLeafNodeString,PageKeyPair<char*>,RIDKeyPair<char*>>(rootPageNum, newPagePair, 0);

				}
				bufMgr->unPinPage(this->file, rootNo, true);
			}
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::startScan
	// -----------------------------------------------------------------------------

	const void BTreeIndex::startScan(const void* lowValParm,
			const Operator lowOpParm,
			const void* highValParm,
			const Operator highOpParm)
	{
		if((lowOpParm != GT && lowOpParm !=GTE)||(highOpParm != LT && highOpParm != LTE)){
			//only support GT/GTE and LT/LTE
			throw BadOpcodesException();
		}

		//for INT, DOUBLE and STRING cases: if lowValue>highValue, throw exception
		//				    if scanning, stop current attempt
		if(attributeType == INTEGER){
			if((*(int*)(lowValParm)) > (*(int*)highValParm)){
				throw BadScanrangeException();
			}
			if(scanExecuting){
				endScan(); 
			}
			scanExecuting = true;
			lowOp = lowOpParm;
			highOp = highOpParm;

			lowValInt = *((int*)lowValParm);
			highValInt = *((int*)highValParm);
			search<int,struct LeafNodeInt,struct NonLeafNodeInt, class PageKeyPair<int>,class RIDKeyPair<int>>(lowValInt);
		}
		else if(attributeType == DOUBLE){ 
			if((*(double*)lowValParm) > (*(double*)highValParm)){
				throw BadScanrangeException();
			}
			if(scanExecuting){
				endScan();
			}
			scanExecuting = true;
			lowOp = lowOpParm;
			highOp = highOpParm;

			lowValDouble = *((double*)lowValParm);
			highValDouble = *((double*)highValParm);
			search<double,struct LeafNodeDouble,struct NonLeafNodeDouble, class PageKeyPair<double>,class RIDKeyPair<double>>(lowValDouble);
		}
		else{   
			if(strncmp(lowValString.c_str(),highValString.c_str(),STRINGSIZE)>0){
				throw BadScanrangeException();
			}
			if(scanExecuting){
				endScan();
			}
			scanExecuting = true;
			lowOp = lowOpParm;
			highOp = highOpParm;

			lowValString = std::string((char*)lowValParm, STRINGSIZE);
			highValString = std::string((char*)highValParm, STRINGSIZE);
			char* lowVal = (char*)malloc(STRINGSIZE);
			snprintf(lowVal, STRINGSIZE, "%s",lowValString.c_str());
			search<char*,struct LeafNodeString,struct NonLeafNodeString, class PageKeyPair<char*>,class RIDKeyPair<char*>>(lowVal);
		}

	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::search
	// -----------------------------------------------------------------------------
	template<class T,class LT,class NT,class PP,class RP> void BTreeIndex::search(T lowVal) 
	{
		Page* currPage;
		PageId currNo;
		NT* currNode;

		//case 1, if the root is leaf -- scan current page which is the only page in tree
		if (onlyRoot){
			this->currentPageNum = this->rootPageNum;		
			bufMgr->readPage(file, currentPageNum, currentPageData);	
			nextEntry = leafPos<T,LT>(rootPageNum,lowVal);
			if (nextEntry == -1) {
				throw IndexScanCompletedException();
			}
			return;
		}

		//case 2, if root is not leaf, search to find the right position
		currNo = this->rootPageNum;
		bufMgr->readPage(file,currNo,currPage);
		bufMgr->unPinPage(this->file,currNo,false);
		currNode = (NT*) currPage;


		while (currNode->level != 1) {
			int pos = nonLeafPos<T,NT>(currNo,lowVal);
			currNo = currNode->pageNoArray[pos];
			bufMgr->readPage(file,currNo,currPage);
			currNode = (NT*)currPage;
			bufMgr->unPinPage(this->file,currNo,false);
		}

		int pos = nonLeafPos<T,NT>(currNo,lowVal);
		if (pos == -1) {
			throw IndexScanCompletedException();
		}
		this->currentPageNum = currNode->pageNoArray[pos];

		nextEntry = leafPos<T,LT>(currentPageNum,lowVal);
		if (nextEntry == -1) {
			throw IndexScanCompletedException();
		}

		bufMgr->readPage(file,currentPageNum,currentPageData);
		bufMgr->unPinPage(file, currentPageNum, false);

	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::scanNext
	// -----------------------------------------------------------------------------

	const void BTreeIndex::scanNext(RecordId& outRid) 
	{if(!scanExecuting) {
				    throw ScanNotInitializedException();
			    }
	if(currentPageNum ==0) {
		throw IndexScanCompletedException();
	}
	if(attributeType == INTEGER) {
		LeafNodeInt* curr = (LeafNodeInt*) currentPageData;
		if(highOp == LTE && curr->keyArray[nextEntry] > highValInt) {
			throw IndexScanCompletedException();
		}
		if(highOp == LT && curr->keyArray[nextEntry] >= highValInt) {
			throw IndexScanCompletedException();
		}
		outRid = curr->ridArray[nextEntry];
		if(nextEntry == leafOccupancy-1 ||  curr->ridArray[nextEntry+1].page_number == 0) {
			if(curr->rightSibPageNo == 0) {

				currentPageNum = 0;
			}
			else{
				currentPageNum = curr->rightSibPageNo;
				bufMgr->readPage(file,currentPageNum, currentPageData);
				nextEntry = 0;

				bufMgr->unPinPage(file,currentPageNum,false);
			}
		}
		else{
			nextEntry++;
		}
	}
	else if(attributeType == DOUBLE){
		LeafNodeDouble* curr = (LeafNodeDouble*) currentPageData;
		if(highOp == LTE && curr->keyArray[nextEntry] > highValDouble) {
			throw IndexScanCompletedException();
		}
		if(highOp == LT && curr->keyArray[nextEntry] >= highValDouble) {
			throw IndexScanCompletedException();
		}
		outRid = curr->ridArray[nextEntry];
		if(nextEntry == leafOccupancy-1 ||  curr->ridArray[nextEntry+1].page_number == 0) {
			if(curr->rightSibPageNo == 0) {

				currentPageNum = 0;
			}
			else{
				currentPageNum = curr->rightSibPageNo;
				bufMgr->readPage(file,currentPageNum, currentPageData);
				nextEntry = 0;
				bufMgr->unPinPage(file,currentPageNum,false);

			}
		}
		else{
			nextEntry++;
		}


	}
	else{
		LeafNodeString* curr = (LeafNodeString*) currentPageData;
		char* s = (char*)malloc(STRINGSIZE);
		snprintf(s,STRINGSIZE, "%s",highValString.c_str());
		if(highOp == LTE && strcmp(curr->keyArray[nextEntry],s)>0) {
			throw IndexScanCompletedException();
		}
		if(highOp == LT && strcmp(curr->keyArray[nextEntry],s)>=0) {
			throw IndexScanCompletedException();
		}
		outRid = curr->ridArray[nextEntry];
		if(nextEntry == leafOccupancy-1 ||  curr->ridArray[nextEntry+1].page_number == 0) {
			if(curr->rightSibPageNo == 0) {

				currentPageNum = 0;
			}
			else{
				currentPageNum = curr->rightSibPageNo;
				bufMgr->readPage(file,currentPageNum, currentPageData);
				nextEntry = 0;
				bufMgr->unPinPage(file,currentPageNum,false);

			}
		}
		else{
			nextEntry++;
		}
	}


	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------
	//
	const void BTreeIndex::endScan() 
	{
		// if currently no scan in process, throw exception
		if (!scanExecuting) {
			throw ScanNotInitializedException();
		}
		// try the best to unpin all the pages and ignore exceptions
		try{
			if(this->currentPageNum !=0){
				bufMgr->unPinPage(this->file, this->currentPageNum, false);
			}
		}
		catch(PageNotPinnedException e){
		}

		try{
			bufMgr->unPinPage(this->file,this->rootPageNum,true);
		}
		catch(PageNotPinnedException e){
		}


		scanExecuting = false;
	}
		

	//---------------------------------------------------------------------------------
	//BTreeIndex::compare
	//---------------------------------------------------------------------------------

	template<class T> int BTreeIndex::compare(T a, T b) {
		if (a > b) {
			return 1;
		}
		else if (a < b) {
			return -1;
		}
		else{
			return 0;
		}
	}
	//for STRING case
	template<> int BTreeIndex::compare<char*>(char* a, char* b) {
		return strcmp((char*)a, (char*)b);
	}


	//--------------------------------------------------------------------------
	//BtreeIndex::leafPos
	//--------------------------------------------------------------------------

	template<class T, class LT> int BTreeIndex::leafPos(PageId currNo, T lowVal){
		int pos = 0;
		Page* currPage;
		T curr;
		bufMgr->readPage(file,currNo,currPage);
		LT* currNode = (LT*) currPage;

		while (pos < leafOccupancy && currNode->ridArray[pos].page_number != 0) {
			curr = currNode->keyArray[pos];
			if(lowOp == GT){
				if (attributeType == STRING) {
					if (compare(curr, lowVal) > 0) {
						bufMgr->unPinPage(this->file,currNo,false);
						return pos;
					}
				}
				else {
					if (compare<T>(curr, lowVal) > 0) {
						bufMgr->unPinPage(this->file,currNo,false);
						return pos;
					}
				}
			}
			else if(lowOp == GTE){
				if (attributeType == STRING) {
					if (compare(curr, lowVal) >= 0) {
						bufMgr->unPinPage(this->file,currNo,false);
						return pos;
					}
				}
				else {
					if (compare<T>(curr, lowVal) >= 0) {
						bufMgr->unPinPage(this->file,currNo,false);
						return pos;
					}
				}
			}
			pos++;	
		}
		bufMgr->unPinPage(this->file,currNo,false);
		if(pos==leafOccupancy || currNode->ridArray[pos].page_number == 0) {
			pos--;
		}
		return pos;
	}




	//----------------------------------------------------------------------------
	//BTreeIndex::nonLeafPos
	//----------------------------------------------------------------------------
	template<class T,class NT> int BTreeIndex::nonLeafPos(PageId currNo, T lowVal){
		int pos = 0;
		Page* currPage;
		bufMgr->readPage(file,currNo,currPage);
		NT* currNode = (NT*) currPage;
		T curr;

		while (pos < nodeOccupancy && currNode->pageNoArray[pos] != 0) {
			curr = currNode->keyArray[pos];
			if (attributeType == STRING) {
				if(compare(curr, lowVal) > 0) {
					bufMgr->unPinPage(this->file,currNo,false);
					return pos;
				}
			}
			else {
				if(compare<T>(curr, lowVal) > 0) {
					bufMgr->unPinPage(this->file,currNo,false);
					return pos;
				}
			}
			pos++;
		}

		bufMgr->unPinPage(this->file,currNo,false);
		if(currNode->pageNoArray[pos] == 0) {
			pos--;
		}
		return pos;

	}


		
	// -----------------------------------------------------------------------------
	// BTreeIndex::insertID
	// ----------------------------------------------------------------------------

	void BTreeIndex::insertID (void* a, void* b) {
		if (attributeType == INTEGER) {
			*((int*)a) = *((int*)b);
		}
		else if (attributeType == DOUBLE) {
			*(double*)a = *(double*)b;
		}		
	}


	// -----------------------------------------------------------------------------
	// BTreeIndex::insertS
	// ----------------------------------------------------------------------------
	template<class T> void BTreeIndex::insertS(T a, T b) {

	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertS
	// ----------------------------------------------------------------------------
	template<> void BTreeIndex::insertS <char*> (char* a, char* b) {
		strncpy(a, b, STRINGSIZE);
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertLeaf
	// ----------------------------------------------------------------------------
	template<class LT,class RP> void BTreeIndex::insertLeaf(LT* node, RP pair){

		int pos = 0;
		int i = 0;
		RP curr;
		
		//search through to find the position
    		for(;pos<leafOccupancy;pos++) {
			if((node->ridArray[pos].page_number == 0)) break;
			curr.set(node->ridArray[pos],node->keyArray[pos]);
                    
                        if(compare(curr.key,pair.key)>=0){
                                break;
                        }
                }
		
		//make a space for the value to insert and then insert
		for (i = leafOccupancy-1; i > pos; i--){
			node->ridArray[i] = node->ridArray[i-1];
			if (attributeType == STRING) {
				insertS(node->keyArray[i], node->keyArray[i-1]);
			}
			else {
				insertID(&(node->keyArray[i]), &(node->keyArray[i-1]) );
			}
		}
		node->ridArray[pos] = pair.rid;

		if (attributeType == STRING) {
			insertS(node->keyArray[pos], pair.key);
		}
		else {
			insertID(&(node->keyArray[pos]), &(pair.key) );
		}

	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertNonLeaf
	// ----------------------------------------------------------------------------
	template<class NT,class PP> void BTreeIndex::insertNonLeaf(NT* node, PP pair){
		//for nonLeaf cases
		int pos = 0;
		int pn_pos = 0;
		int key_pos = 0;
		PP curr;

		for(;pos<leafOccupancy;pos++) {
			if((node->pageNoArray[pos] == 0)) break;
			curr.set(node->pageNoArray[pos],node->keyArray[pos]);
                    
                        if(compare(curr.key,pair.key)>=0){
                                break;
                        }
                }

		
		for(int i = nodeOccupancy-1; i > pos; i--){
			if (attributeType == STRING) {
				insertS(node->keyArray[i], node->keyArray[i-1] );
			}
			else {
				insertID(&(node->keyArray[i]), &(node->keyArray[i-1]) );
			}
			node->pageNoArray[i+1] = node->pageNoArray[i];	
		}

		if (node->pageNoArray[pos] == 0) {
			key_pos = pos-1;
			pn_pos = pos;
		}
		else {
			key_pos = pos;
			pn_pos = pos+1;
		}
		node->pageNoArray[pn_pos] = pair.pageNo;

		if (attributeType == STRING) {
			insertS(node->keyArray[key_pos], pair.key);
		}
		else {
			insertID(&(node->keyArray[key_pos]), &(pair.key) );
		}

	}


	// -----------------------------------------------------------------------------
	// BTreeIndex::splitLeaf
	// ----------------------------------------------------------------------------
	template<class LT,class PP,class RP> void BTreeIndex::splitLeaf(LT* leafNode, RP RIDPair, PP& newPair) {
		PageId newPageNo;
		Page* newPage;
		LT* newLeafNode;
		int half = leafOccupancy/2+1;
		bufMgr->allocPage(this->file, newPageNo, newPage); 
		newLeafNode = (LT*)newPage; 

		for (int i = half; i < leafOccupancy; i++) {
			newLeafNode->ridArray[i-half] = leafNode->ridArray[i];
			leafNode->ridArray[i].page_number = 0;
			if (attributeType == STRING) {
				insertS(newLeafNode->keyArray[i-half], leafNode->keyArray[i]);
			}
			else {
				insertID(&(newLeafNode->keyArray[i-half]), &(leafNode->keyArray[i]));
			}
		}

		newLeafNode->rightSibPageNo = leafNode->rightSibPageNo;
		leafNode->rightSibPageNo = newPageNo;

		newPair.set(newPageNo, newLeafNode->keyArray[0]);
		if (compare(RIDPair.key, newPair.key) < 0 ){
			insertLeaf<LT,RP>(leafNode,RIDPair);
		}
		else {
			insertLeaf<LT,RP>(newLeafNode,RIDPair);
		}

		bufMgr->unPinPage(file, newPageNo, true);

	}


	// -----------------------------------------------------------------------------
	// BTreeIndex::splitNonLeaf
	// ----------------------------------------------------------------------------
	template<class NT,class PP> void BTreeIndex::splitNonLeaf(NT* nonLeafNode, PP returnP, PP& newPKPair) {
		PageId newPageNo;
		Page* newPage;
		NT* newNonLeafNode;
		int mid = nodeOccupancy/2+1;
		bufMgr->allocPage(file, newPageNo, newPage);
		newNonLeafNode = (NT*)newPage;

		// new node has same level with spliteed node
		newNonLeafNode->level = nonLeafNode->level; 

		for (int i = mid; i < nodeOccupancy; i++) {
			newNonLeafNode->pageNoArray[i-mid] = nonLeafNode->pageNoArray[i];
			if (i != mid) 
				nonLeafNode->pageNoArray[i] = 0;
			if (attributeType == STRING) {
				insertS(newNonLeafNode->keyArray[i-mid], nonLeafNode->keyArray[i] );
			}
			else {
				insertID(&(newNonLeafNode->keyArray[i-mid]), &(nonLeafNode->keyArray[i]) );
			}
		}
		newNonLeafNode->pageNoArray[nodeOccupancy-mid] = nonLeafNode->pageNoArray[nodeOccupancy];
		nonLeafNode->pageNoArray[nodeOccupancy] = 0;

		newPKPair.set(newPageNo,newNonLeafNode->keyArray[0]);

		if (returnP.key<newPKPair.key){
			insertNonLeaf <NT,PP> (nonLeafNode, returnP);
		}
		else{
			insertNonLeaf <NT,PP> (newNonLeafNode, returnP);
		}

		bufMgr->unPinPage(file, newPageNo, true);

	} 


	// -----------------------------------------------------------------------------
	// BTreeIndex::createNewRoot
	// ----------------------------------------------------------------------------
	template<class LT,class NT,class PP,class RP> void BTreeIndex::createNewRoot(PageId oldNo, PP newPair, int level){
		Page* newRootPage;
		PageId newRootPageNo;
		Page* headerPage;
		NT* newRootNode;
		IndexMetaInfo * meta;

		bufMgr->allocPage(file, newRootPageNo, newRootPage); 
		newRootNode = (NT*)newRootPage;
		newRootNode->pageNoArray[0] = oldNo;
		newRootNode->pageNoArray[1] = newPair.pageNo;
		onlyRoot = false;
		newRootNode->level = level;
		if (attributeType == STRING) {
			insertS(newRootNode->keyArray[0], newPair.key);
		}
		else {
			insertID(&(newRootNode->keyArray[0]),&(newPair.key));
		}

		
		rootPageNum = newRootPageNo;
		
		bufMgr->unPinPage(file, newRootPageNo, true);
		bufMgr->readPage(file, headerPageNum, headerPage);
		meta = (IndexMetaInfo*)headerPage;
		meta->rootPageNo = this->rootPageNum;
		bufMgr->unPinPage(file, headerPageNum, true);

	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::start
	// -----------------------------------------------------------------------------
	template<class T, class LT,class NT,class PP,class RP> void BTreeIndex::start(PageId currPageNo, PP& newPPair, RP newRPair) 
	{
		
		PageId childPageNo;
		Page* childPage;
		
		Page* currPage;
		NT* currNode;
		PP curr;
		PP newPKPair;
		PP returnPPair;


		bufMgr->readPage(file, currPageNo, currPage);
		currNode = (NT*) currPage;

		int pos = 0;
		for(;pos<leafOccupancy;pos++) {
			if((currNode->pageNoArray[pos] == 0)) break;
			curr.set(currNode->pageNoArray[pos],currNode->keyArray[pos]);
                        if(compare(curr.key,newRPair.key)>=0){
                                break;
                        }
                }

		if (currNode->pageNoArray[pos] == 0 && pos > 0) pos--;

		childPageNo = currNode->pageNoArray[pos]; 

		// check level, if currNode is at level 1 just insert entry into leaf node
		if (currNode->level == 1) {
			// check if leaf node is full, if it is need to split leaf node
			bufMgr->readPage(file, childPageNo, childPage);
			LT* childLeafNode = (LT*) childPage;

			if ((childLeafNode->ridArray[leafOccupancy-1]).page_number == 0) {
				insertLeaf<LT,RP>(childLeafNode, newRPair);
			}
			else {
				splitLeaf<LT,PP,RP>(childLeafNode, newRPair, returnPPair);
				if (currNode->pageNoArray[nodeOccupancy] == 0) {
					insertNonLeaf<NT,PP>(currNode,returnPPair);
				}
				// if current non-leaf node is full, split the leaf and insert into right node
				else {	
					splitNonLeaf<NT,PP>(currNode, returnPPair, newPKPair);
					newPPair = newPKPair;
				}
			}
			bufMgr->unPinPage(file,childPageNo,true); 
			bufMgr->unPinPage(file,currPageNo,true);
			return;
		}
		PP newChildPPair;
		// if currNode is at level 0
		bufMgr->unPinPage(file,currPageNo,false); 
		start<T, LT, NT, PP, RP> (childPageNo, newChildPPair, newRPair);

		Page* newReadCurr;
		bufMgr->readPage(file,currPageNo,newReadCurr);

		if (newChildPPair.pageNo != 0) {
			returnPPair.set(newChildPPair.pageNo, newChildPPair.key);

			if (currNode->pageNoArray[nodeOccupancy]==0) {
				// if currNode is not full
				insertNonLeaf<NT,PP>(currNode,returnPPair);
			}
			else {
				// if currNode is full
				splitNonLeaf<NT,PP>(currNode, returnPPair, newPKPair);
				newPPair = newPKPair;
			}
		}
		bufMgr->unPinPage(file, currPageNo, (newChildPPair.pageNo !=0) ); 

	}

} // end namespace badgerdb


