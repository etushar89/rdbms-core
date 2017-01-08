/*
 * index_mgr_tree_key.c
 *
 *  Created on: Dec 4, 2015
 *      Author: heisenberg
 */
#include "dberror.h"
#include "btree_mgr.h"
#include "malloc.h"
#include "string.h"

extern void * Rootnode;

// Open the Btree for access
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {

	Btree *node = NULL;
	Btree_stat *statInfo = NULL;
	Scankey *key = NULL;

	statInfo = tree->mgmtData;
	node = statInfo->mgmtData;

	// traverse till the end
	for (node = statInfo->mgmtData; node->pointers[0] != NULL;
			node = node->pointers[0]) {
	}

	(*handle) = (BT_ScanHandle *) malloc(sizeof(BT_ScanHandle));
	if (*handle == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"not enough memory for resource allocation");
	}

	key = (Scankey *) malloc(sizeof(Scankey));
	if (key == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"not enough memory for resource allocation");
	}

	key->recnumber = 0;
	key->currentNode = node;

	// update the tree structure
	(*handle)->tree = tree;

	// set the management data
	(*handle)->mgmtData = (void *) key;

	// all ok
	return RC_OK;
}

// insert the specified key in the Btree
// the tree needs to be opened first by the caller
RC insertKey(BTreeHandle* tree, Value* key, RID rid) {

	Btree_stat *root = tree->mgmtData;

	Btree *node = root->mgmtData;
	int i;

	if (!root->num_nodes) {

		// tree is empty
		root->num_nodes++;

		// create a new nodes
		createNew(node, key, rid);

		// the toptal number of nodes change
		root->num_inserts = root->num_inserts + 1;

		// update the statistic info of the tree
		updateStat(tree, root);

		return RC_OK;
	}

	// find a place where the new node should go
	node = find_node_to_insert(root, key);

	// traverse till the end
	Btree* temp1 = Rootnode;

	while (temp1) {

		for (i = 0; i < temp1->num_keys; i++) {

			// check if theres a match
			if (temp1->keys[i] == key->v.intV) {

				// update the stat info
				updateStat(tree, root);
				return RC_OK;
			}

		}

		temp1 = temp1->next;
	}

	// check which side should we traverse
	if (node->num_keys < root->order) {

		// this is a leaf node
		insertLeaf(node, key, rid);

		root->num_inserts = root->num_inserts + 1;

		// update the stat info
		updateStat(tree, root);

		return RC_OK;
	}

	// check if we got aq match
	if (node->num_keys == root->order) {

		// split the current node and insert
		splitInsert(tree, root, node, key, rid);

		root->num_inserts = root->num_inserts + 1;

		// update the stat info
		updateStat(tree, root);

		return RC_OK;
	}

	// ok
	return RC_OK;
}

// cleanup and close the Btree's scan handle
RC closeTreeScan(BT_ScanHandle* handle) {
	free(handle->mgmtData);
	free(handle->tree);
	return RC_OK;
}

// delete the specified key from the Btree
RC deleteKey(BTreeHandle *tree, Value *key) {

	Btree* leaf = NULL;
	// get the desired leaf
	leaf = find_leaf(tree, key);

	// get the left node
	Btree* childLeft = NULL;
	childLeft = leaf->prev;

	// get the tree stastistics data
	Btree_stat *info = NULL;
	info = tree->mgmtData;

	if (leaf != NULL) {

		if (childLeft == NULL) {

			// no child
			// delete the entry and adjust the tree if needed
			delete_entry(tree, leaf, key);

			// done
			return RC_OK;
		}

		if (key->v.intV == leaf->keys[0]) {

			// the key exists in this leaf node
			// delete it
			delete_entry(tree, leaf, key);

			// check if there is underflow in the node capacity
			if (leaf->num_keys == 0) {

				// merge with the left sibling
				childLeft->next = leaf->next;

				// check if we need to propagate this change up the tree
				delete_parent_nodes_inital(info, leaf, key);

				// done
				return RC_OK;
			}

			// update the prent node
			updateFirst(leaf, key);

			return RC_OK;

		} else {

			delete_entry(tree, leaf, key);

			if (leaf) {

				// see of there is underflow
				if (checkUnderflow(info, leaf)) {

					// if yes, merge with left sibling
					if (childLeft) {

						// check the number of keys even after splitting
						if (childLeft->num_keys > splitNode(info->order)
								&& childLeft->num_keys != info->order) {

							// we need to redistribute the keys
							// to balance out
							redistribute(info, childLeft, leaf);

						} else {
							// just merge
							merge_nodes(info, childLeft, leaf);
						}
					}
				}
			}

			return RC_OK;
		}
	}

	// all ok
	return RC_OK;
}

// get the next entry from the Btree scan buffer
RC nextEntry(BT_ScanHandle *handle, RID *result) {

	// get the management data from the handle
	Scankey *key = handle->mgmtData;

	// get the current node
	Btree* root = key->currentNode;

	int totalRecs = key->recnumber;

	// traverse
	if (root) {

		if (root->num_keys > totalRecs) {

			totalRecs = totalRecs + 1;
			// get the desired record 
			*result = root->records[totalRecs - 1];

		}

		// check if total records are equal to # of keys we have
		if (root->num_keys == totalRecs) {

			// reset the number of records we have
			totalRecs = 0;

			// go to the next node in the sequence
			root = root->next;
		}

		// update the record count
		key->recnumber = totalRecs;

		// update the current node
		key->currentNode = root;

		// if the result is not fetched
		if (!result)
			return RC_IM_NO_MORE_ENTRIES;

		return RC_OK;
	}

	// tree is empty
	// no entries exist
	else
		return RC_IM_NO_MORE_ENTRIES;
}

// lookup for specified key in the Btree and return its value i.e. RID
RC findKey(BTreeHandle *tree, Value *key, RID *result) {

	int i = 0;

	Btree_stat *info = NULL;
	Btree* root = NULL;
	Btree* t = NULL;

	// Get the metadata and stuff
	info = tree->mgmtData;
	root = info->mgmtData;

	// traverse the tree till the leaf level
	t = root;

	while (!t->is_leaf) {
		for (i = 0; i < root->num_keys; ++i) {
			if (key->v.intV < root->keys[i]) {
				// change the level
				t = root->pointers[i];
				break;
			} else
				t = root->pointers[i + 1];
		}
	}

	// we are at the leaf now.
	// traverse linearly at leaf level and compare each key value
	for (i = 0; i < t->num_keys; ++i) {
		if (t->keys[i] == key->v.intV) {
			// got the key 
			*result = t->records[i];
			return RC_OK;
		}
	}

	// key not found
	return RC_IM_KEY_NOT_FOUND;
}
