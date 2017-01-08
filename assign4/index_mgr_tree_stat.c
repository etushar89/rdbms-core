/*
 * index_mgr_tree_stat.c
 *
 *  Created on: Dec 4, 2015
 *      Author: heisenberg
 */

#include "dberror.h"
#include "btree_mgr.h"
#include "malloc.h"
#include "string.h"

RC getNumEntries(BTreeHandle *tree, int *result) {
	Btree_stat *root;
	root = tree->mgmtData;
	*result = root->num_inserts;
	return RC_OK;
}

RC getNumNodes(BTreeHandle *tree, int *result) {
	Btree_stat *root;
	root = tree->mgmtData;
	*result = root->num_nodes;
	return RC_OK;
}

RC getKeyType(BTreeHandle *tree, DataType *result) {
	*result = DT_INT;
	return RC_OK;
}
