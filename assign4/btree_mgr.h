#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"

#define OFFSET_NODE_ROOT_PTR	0
#define OFFSET_NODE_LEAF_FLAG	OFFSET_NODE_ROOT_PTR + 4
#define OFFSET_NODE_KEY_COUNT	OFFSET_NODE_LEAF_FLAG + 2

#define OFFSET_NON_LEAF_NODE_ENTRIES	OFFSET_NODE_KEY_COUNT + 4

#define OFFSET_NODE_SIBLING_PTR	OFFSET_NODE_KEY_COUNT + 4
#define OFFSET_NODE_ENTRIES	OFFSET_NODE_SIBLING_PTR + 4

typedef struct Scankey {
	struct Btree *currentNode;
	int recnumber;
} Scankey;

typedef struct Btree {
	int *keys;
	struct Btree *parent;
	struct Btree **pointers;
	RID *records;
	bool is_leaf;
	int num_keys;
	int blkNum;
	struct Btree *next;
	struct Btree *prev;
} Btree;

typedef struct Btree_stat {
	void *mgmtData;
	void *fileInfo;
	int num_nodes;
	int num_inserts;
	int order;
} Btree_stat;

// structure for accessing btrees
typedef struct BTreeHandle {
	DataType keyType;
	char *idxId;
	void *mgmtData;
} BTreeHandle;

typedef struct BT_ScanHandle {
	BTreeHandle *tree;
	void *mgmtData;
} BT_ScanHandle;

// init and shutdown index manager
extern RC initIndexManager(void *mgmtData);
extern RC shutdownIndexManager();

// create, destroy, open, and close an btree index
extern RC createBtree(char *idxId, DataType keyType, int n);
extern RC openBtree(BTreeHandle **tree, char *idxId);
extern RC closeBtree(BTreeHandle *tree);
extern RC deleteBtree(char *idxId);

// access information about a b-tree
extern RC getNumNodes(BTreeHandle *tree, int *result);
extern RC getNumEntries(BTreeHandle *tree, int *result);
extern RC getKeyType(BTreeHandle *tree, DataType *result);

// index access
extern RC findKey(BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey(BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey(BTreeHandle *tree, Value *key);
extern RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry(BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan(BT_ScanHandle *handle);

// debug and test functions
extern char *printTree(BTreeHandle *tree);

#endif // BTREE_MGR_H
