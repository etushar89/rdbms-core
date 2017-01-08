/*
 * index_mgr_tree_op.c
 *
 *  Created on: Dec 4, 2015
 *      Author: heisenberg
 */

#include "dberror.h"
#include "btree_mgr.h"
#include "malloc.h"
#include "string.h"

extern void * Rootnode;

RC deleteBtree(char* idxId) {
	destroyPageFile(idxId);
	return RC_OK;
}

RC createBtree(char* idxId, DataType keyType, int n) {
	unsigned int offset, entries, blocks, temp, block, key;

	offset = 0;
	entries = 0;
	blocks = 0;
	temp = 0;
	block = 0;

	//Sanity checks
	if (idxId == NULL || strlen(idxId) == 0) {
		THROW(RC_REC_MGR_INVALID_TBL_NAME, "Invalid index id");
	}

	if (n == 0) {
		THROW(RC_INVALID_HANDLE, "Number of keys is invalid");
	}

	createPageFile(idxId);

	BM_BufferPool *bPool = MAKE_POOL();
	BM_PageHandle *page = MAKE_PAGE_HANDLE();

	initBufferPool(bPool, idxId, 3, RS_FIFO, NULL);

	pinPage(bPool, page, 0);

	key = DT_INT;

	// metadata of Index file
	memmove(page->data, &block, sizeof(block));
	offset = offset + sizeof(block);

	// update blocks
	memmove(page->data + offset, &blocks, sizeof(blocks));
	offset = offset + sizeof(blocks);

	// update number of entries
	memmove(page->data + offset, &entries, sizeof(entries));
	offset = offset + sizeof(entries);

	// update the key
	memmove(page->data + offset, &key, sizeof(key));
	offset = offset + sizeof(key);

	// update the block r
	memmove(page->data + offset, &temp, sizeof(temp));
	offset = offset + sizeof(temp);

	// update the maximum number of keys in a node
	memmove(page->data + offset, &n, sizeof(n));

	//updateBtreeMetadata(page->data, keyType, n, 0, 0);

	markDirty(bPool, page);

	unpinPage(bPool, page);

	forceFlushPool(bPool);

	shutdownBufferPool(bPool);

	free(bPool);

	return RC_OK;
}

RC closeBtree(BTreeHandle *tree) {

	//Sanity Checks
	if (tree == NULL) {
		//TODO Err code
		THROW(RC_INVALID_HANDLE, "Index handle is invalid");
	}

	// get the management data of the tree
	Btree_stat *root;
	root = tree->mgmtData;

	// shut down the buffer pool
	shutdownBufferPool(root->fileInfo);

	// free the allocated memory
	free(root->fileInfo);
	free(root->mgmtData);
	tree->idxId = NULL;
	free(tree);

	// all ok
	return RC_OK;
}

RC openBtree(BTreeHandle** tree, char* idxId) {
	unsigned int offset, entries, blocks, temp, block, key;
	unsigned int order;

	DataType dt;

	BM_BufferPool *bPool = MAKE_POOL();
	BM_PageHandle *page = MAKE_PAGE_HANDLE();
	Btree_stat *info = ((Btree_stat *) malloc(sizeof(Btree_stat)));

	offset = 0;
	entries = 0;
	temp = 0;
	blocks = 0;
	block = 0;

	// create an inmemory image of the tree
	*tree = (BTreeHandle *) malloc(sizeof(BTreeHandle));
	(*tree)->idxId = idxId;

	initBufferPool(bPool, idxId, 3, RS_FIFO, NULL);

	// pin the metadat page of thi=e index file
	pinPage(bPool, page, 0);

	// read the metadata sequentially
	offset = offset + sizeof(int);

	// number of blovks
	memcpy(&blocks, page->data + offset, sizeof(blocks));
	offset = offset + sizeof(blocks);
	info->num_nodes = blocks;

	// number of entries
	memcpy(&entries, page->data + offset, sizeof(entries));
	offset = offset + sizeof(entries);
	info->num_inserts = entries;

	// key
	memcpy(&key, page->data + offset, sizeof(key));
	offset = offset + sizeof(key);

	memcpy(&temp, page->data + offset, sizeof(temp));
	offset = offset + sizeof(temp);

	// order of the keys
	memcpy(&order, page->data + offset, sizeof(order));
	info->order = order;

	// all done. Unpin the metadata page
	unpinPage(bPool, page);

	(*tree)->keyType = DT_INT;
	(*tree)->mgmtData = info;
	info->fileInfo = bPool;
	(*tree)->keyType = dt;
	info->mgmtData = createNode(*tree);
	Rootnode = info->mgmtData;

	// free the buffer pool
	free(bPool);

	return RC_OK;
}

