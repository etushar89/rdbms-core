#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "dt.h"
#include "tables.h"
#include "record_mgr.h"
#include "btree_mgr.h"

void * Rootnode = NULL;
Btree *queue = NULL;

RC update(char *data, DataType keyType, int n, int noNodes, int type);
RC insertRoot(BTreeHandle *tree, Btree_stat *root, Btree *old_node, int key);
RC updateStat(BTreeHandle *bhandle, Btree_stat* stat);

// Initialize the Index manager
// All the set up related activities can be added here
RC initIndexManager(void* mgmtData) {

	printf("\n Init manager is ready");

	return RC_OK;
}

// shut down the index manager 
RC shutdownIndexManager() {
	return RC_OK;
}

// Create a node for the Btree
Btree* createNode(BTreeHandle* tree) {

	unsigned int blockNo;

	Btree *new_node = ((Btree *) malloc(sizeof(Btree)));
	if (new_node == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	Btree_stat *stat = tree->mgmtData;
	BM_PageHandle *page = MAKE_PAGE_HANDLE();

	pinPage(stat->fileInfo, page, 0);

	blockNo = -1;
	memcpy(&blockNo, page->data, sizeof(int));

	blockNo++;

	// set the values for each field of the new node
	new_node->records = malloc(stat->order * sizeof(void *));
	new_node->keys = malloc(stat->order * sizeof(int));
	new_node->pointers = malloc((stat->order + 1) * sizeof(void *));

	new_node->blkNum = blockNo;
	new_node->next = NULL;
	new_node->is_leaf = true;
	new_node->num_keys = 0;
	new_node->parent = NULL;
	new_node->prev = NULL;

	// update the tree metadata
	update(page->data, tree->keyType, stat->order, 0, 1);

	// mark it dirty to reflect the changes
	markDirty(stat->fileInfo, page);

	// unpin the page
	unpinPage(stat->fileInfo, page);

	// flush the data off disk
	forceFlushPool(stat->fileInfo);

	free(page);

	return new_node;
}

// Internal function for updating the metadata of the Btree
RC update(char *data, DataType keyType, int n, int numNodes, int nodeType) {
	unsigned int offset, entries, blocks, temp, block, key;

	offset = 0;
	entries = 0;
	blocks = 0;
	temp = 0;
	block = 0;

	if (nodeType == 0) {
		// metadata of Index file
		memmove(data, &block, sizeof(block));
		offset = offset + sizeof(block);

		// update blocks
		memmove(data + offset, &blocks, sizeof(blocks));
		offset = offset + sizeof(blocks);

		// update number of entries
		memmove(data + offset, &entries, sizeof(entries));
		offset = offset + sizeof(entries);

		// update the key
		memmove(data + offset, &key, sizeof(key));
		offset = offset + sizeof(key);

		// update the block r
		memmove(data + offset, &temp, sizeof(temp));
		offset = offset + sizeof(temp);

		// update the maximum number of keys in a node
		memmove(data + offset, &n, sizeof(n));
	}

	else if (nodeType == 1) {
		// just add the data
		memcpy(&block, data, sizeof(block));
		block = block + 1;
	}

	else if (nodeType == 2) {
		offset = offset + sizeof(int);

		// non - leaf nodes
		// add number of nodes
		memmove(data + offset, &numNodes, sizeof(numNodes));
		offset = offset + sizeof(numNodes);

		// add number of entries
		memcpy(&entries, data + offset, sizeof(entries));
		entries = entries + 1;

		// rearrange them
		memmove(data + offset, &entries, sizeof(entries));
	}

	return RC_OK;
}

// create a new node
RC createNew(Btree *root, Value* key, RID rid) {

	root->is_leaf = true;
	root->keys[0] = key->v.intV;
	root->records[0] = rid;
	root->parent = NULL;
	root->num_keys++;
	return RC_OK;

}

// insert at the leaf level
RC insertLeaf(Btree *root, Value* key, RID rid) {

	int idx, i;

	for (idx = 0; idx < root->num_keys && root->keys[idx] < key->v.intV;
			idx++) {
	}

	i = 0;
	// traverse through the tree
	for (i = root->num_keys; i > index; --i) {

		// get the desired key and its RID
		root->keys[i] = root->keys[i - 1];
		root->records[i] = root->records[i - 1];
	}

	root->keys[idx] = key->v.intV;
	root->records[idx] = rid;

	// update the total key count
	root->num_keys = root->num_keys + 1;

	// all ok
	return RC_OK;
}

// create a parent node
RC insertParent(Btree *root, int key) {

	int idx, i;

	for (idx = 0; idx < root->num_keys && root->keys[idx] < key; idx++) {
	}

	i = 0;
	for (i = root->num_keys; i > idx; --i) {

		// get the desired key and its RID
		root->keys[i] = root->keys[i - 1];
		root->records[i] = root->records[i - 1];
	}

	// set the parent data
	root->records[idx].page = 0;
	root->records[idx].slot = 0;
	root->keys[idx] = key;

	// update the total key count
	root->num_keys = root->num_keys + 1;

	// all ok
	return RC_OK;

}

// display the tree in depth-first preorder format
RC print(BTreeHandle* tree) {

	Btree *root;
	Btree_stat *btstat;
	btstat = tree->mgmtData;
	root = btstat->mgmtData;
	int i = 0;

	while (root) {

		// traverse at the root level
		for (i = 0; i < root->num_keys; i++) {
			printf(" %d", root->keys[i]);
		}

		printf("\t");
		root = root->next;
	}

	printf("\n");

	// get the management data
	root = btstat->mgmtData;

	while (root->pointers[0]) {
		root = root->pointers[0];
	}

	// trvarse till the lead
	while (root) {
		i = 0;
		while (i < root->num_keys) {
			printf(" %d", root->keys[i]);
			i++;
		}
		printf("\t");
		root = root->next;
	}

	printf("\n");

	// all ok
	return RC_OK;
}

// split the node at the middle
int splitNode(int node_len) {

	return ((node_len % 2) == 0) ? node_len / 2 : node_len + 1 / 2;

}

// get the parent node of the the given element
Btree* getParent(Btree *node, int key) {

	Btree* curr_node = node->parent;
	Btree* parent = curr_node;

	// till the tree is alive
	while (curr_node) {

		// check if we are at the leaf
		if (!curr_node->is_leaf) {

			// this is a non-leaf node
			if (key < curr_node->keys[0]) {
				// we got the parent
				return parent;
			}
		}

		parent = curr_node;
		curr_node = curr_node->next;
	}

	// return the parent
	return parent;
}

//Inserts parent node
RC insert_parent(BTreeHandle* tree, Btree_stat *root, Btree *prevNode,
		Btree *newNode, int newKey) {
	Btree *parentNode;
	if (prevNode->parent == NULL) {
		parentNode = createNode(tree);
		root->num_nodes += 1;
		parentNode->pointers[0] = prevNode;
		parentNode->pointers[1] = newNode;
		root->mgmtData = parentNode;
	} else {
		parentNode = getParent(prevNode, newKey);
		int k = parentNode->num_keys;
		parentNode->pointers[k + 1] = newNode;
	}
	parentNode->is_leaf = 0;

	//Point both nodes to parent
	prevNode->parent = parentNode;
	newNode->parent = prevNode->parent;

	if (parentNode->num_keys < root->order) {
		parentNode->pointers[parentNode->num_keys + 1] = newNode;
		insertParent(parentNode, newKey);
	}

	if (parentNode->num_keys == root->order) {
		insertRoot(tree, root, parentNode, newKey);
	}

	//All OK
	return RC_OK;
}

RC splitInsert(BTreeHandle* tree, Btree_stat *root, Btree *oldNode, Value* key,
		RID rid) {

	int i = 0, j = 0;
	Btree *newNode, *temp;
	int index = 0;
	int *temp_array_keys;
	RID *temp_array_pointers;
	int split_pos;
	int num_newNode = 0, new_key = 0;

	Btree_stat* stat = tree->mgmtData;

	if (!oldNode->next) {

		newNode = createNode(tree);
		stat->num_nodes = stat->num_nodes + 1;
		oldNode->next = newNode;
		newNode->prev = oldNode;

		temp_array_pointers = malloc((stat->order + 1) * sizeof(void));
		temp_array_keys = malloc((stat->order + 1) * sizeof(int));

		for (index = 0;
				index < oldNode->num_keys && oldNode->keys[index] < key->v.intV;
				index++) {
		}

		do {
			if (j == index) {
				j++;
			}
			temp_array_keys[j] = oldNode->keys[i];
			temp_array_pointers[j++] = oldNode->records[i++];
		} while (i < oldNode->num_keys);

		temp_array_keys[index] = key->v.intV;
		temp_array_pointers[index] = rid;
		split_pos = 1 + splitNode(stat->order);
		oldNode->num_keys = 0;
		j = 0;
		i = 0;

		while (i < split_pos) {

			oldNode->keys[i] = temp_array_keys[i];
			oldNode->records[i] = temp_array_pointers[i];
			oldNode->num_keys++;
			j++;
			i++;
		}
		num_newNode = stat->order + 1 - split_pos;

		i = 0;
		while (i < num_newNode) {
			newNode->keys[i] = temp_array_keys[j];
			newNode->records[i] = temp_array_pointers[j];
			newNode->num_keys++;
			j++;
			i++;
		}
		newNode->parent = oldNode->parent;
		new_key = newNode->keys[0];
		insert_parent(tree, root, oldNode, newNode, new_key);

	} else {
		temp = oldNode->next;
		int k = 0;
		k = oldNode->num_keys;
		if (temp->num_keys < stat->order
				&& oldNode->keys[k - 1] < key->v.intV) {
			newNode = temp;
			insertLeaf(newNode, key, rid);
			return RC_OK;
		} else {
			newNode = createNode(tree);
			stat->num_nodes++;
			oldNode->next = newNode;
			newNode->prev = oldNode;
			newNode->next = temp;
			temp->prev = newNode;
		}
		temp_array_keys = malloc((stat->num_inserts + 1) * sizeof(int));
		temp_array_pointers = malloc((stat->num_inserts + 1) * sizeof(void));
		while (index < oldNode->num_keys && oldNode->keys[index] < key->v.intV) {
			index++;
		}

		do {
			if (j == index) {
				j++;
			}
			temp_array_keys[j] = oldNode->keys[i];
			temp_array_pointers[j] = oldNode->records[i];
			i++;
			j++;
		} while (i < oldNode->num_keys);
		temp_array_keys[index] = key->v.intV;
		temp_array_pointers[index] = rid;
		split_pos = 1 + splitNode(stat->order);
		oldNode->num_keys = 0;
		j = 0;
		for (i = 0; i < split_pos; i++) {
			oldNode->keys[i] = temp_array_keys[i];
			oldNode->records[i] = temp_array_pointers[i];
			oldNode->num_keys++;
			j++;
		}
		num_newNode = stat->order + 1 - split_pos;
		i = 0;
		while (i < num_newNode) {

			newNode->keys[i] = temp_array_keys[j];
			newNode->records[i] = temp_array_pointers[j];
			newNode->num_keys++;
			j++;
			i++;
		}

		root->mgmtData = newNode;
		newNode->parent = oldNode->parent;
		new_key = newNode->keys[0];
		insert_parent(tree, root, oldNode, newNode, new_key);
	}
	free(temp_array_pointers);
	return RC_OK;
}

//Inserts root node
RC insertRoot(BTreeHandle *tree, Btree_stat *root, Btree *oldNode, int key) {
	Btree *newNode;
	newNode = createNode(tree);
	root->num_nodes++;
	int index = 0;
	int i = 0;
	int j = 0;
	int *temp_array_keys;
	int splitPos;
	int num_new_node = 0;
	int new_key = 0;
	newNode->is_leaf = 0;
	oldNode->next = newNode;
	newNode->prev = oldNode;
	temp_array_keys = malloc((root->order + 1) * sizeof(int));
	while (oldNode->keys[index] < key && index < oldNode->num_keys) {
		index++;
	}
	do {
		if (j == index)
			j++;
		temp_array_keys[j] = oldNode->keys[i];
		i++;
		j++;
	} while (i < oldNode->num_keys);
	temp_array_keys[index] = key;
	splitPos = 1 + splitNode(root->order);
	oldNode->num_keys = 0;
	j = 0;
	for (i = 0; i < splitPos - 1; i++) {
		oldNode->keys[i] = temp_array_keys[i];
		oldNode->num_keys++;
		j++;
	}
	j++;
	new_key = temp_array_keys[splitPos - 1];
	num_new_node = root->order + 1 - splitPos;
	for (i = 0; i < num_new_node; i++) {
		newNode->keys[i] = temp_array_keys[j];
		newNode->num_keys++;
		j++;
	}
	root->mgmtData = newNode;
	insert_parent(tree, root, oldNode, newNode, new_key);

	//All OK
	return RC_OK;
}

// find appropriate place for the new node in the tree
Btree* find_node_to_insert(Btree_stat *root, Value* key) {

	Btree* node = Rootnode;
	Btree* parent = node;

	// till the tree is not exhanusted
	while (node) {

		// if we are the leaf level
		if (node->is_leaf) {

			// compare the ley
			if (key->v.intV < node->keys[0]) {

				// returnt its parent
				return parent;
			}
		}

		parent = node;
		node = node->next;
	}

	// return the parent
	return parent;
}

// update the statistics data
RC updateStat(BTreeHandle *handle, Btree_stat* info) {

	// create a page
	BM_PageHandle *page = MAKE_PAGE_HANDLE();

	// pin the metadata page
	pinPage(info->fileInfo, page, 0);

	// use the update metadat function
	update(page->data, handle->keyType, info->order, info->num_nodes, 2);

	// mark the page dirty and unpin it
	markDirty(info->fileInfo, page);

	unpinPage(info->fileInfo, page);

	forceFlushPool(info->fileInfo);

	free(page);

	// all ok
	return RC_OK;
}

// check for underflow in the node
bool checkUnderflow(Btree_stat* stat, Btree * node) {

	int order = stat->order;
	int keys = node->num_keys;

	// if the order is even
	if (order % 2 == 0) {
		if (keys < (order) / 2)
			return true;
	} else {
		if (keys < (order + 1) / 2)
			return true;
	}

	// no underflow condition
	return false;
}

RC find_insert_after_redistribute(Btree *root, int key, RID *rid) {
	int index = 0;
	int i = 0;
	for (index = 0; index < root->num_keys && root->keys[index] < key;
			index++) {

	}
	i = root->num_keys;
	while (i > index) {
		root->keys[i] = root->keys[i - 1];
		root->records[i] = root->records[i - 1];
		i--;
	}
	root->keys[index] = key;
	root->records[index] = *rid;
	root->num_keys++;
	return RC_OK;
}

RC updateParentNode(Btree *right_node, int key) {
	right_node = right_node->parent;
	while (right_node) {
		if (right_node->is_leaf != true) {
			right_node->keys[0] = key;
		}
		right_node = right_node->parent;
	}
	return RC_OK;
}

// redistribute the nodes as needed
RC redistribute(Btree_stat *stat, Btree *left, Btree *right) {
	int i = 0;
	int idx = left->num_keys - 1;
	;
	int key = left->keys[idx];
	;

	RID *rid = NULL;

	*rid = left->records[idx];

	left->records[idx].page = 0;
	left->records[idx].slot = 0;
	left->keys[idx] = NULL;
	left->num_keys--;

	// balance out after redistribution
	find_insert_after_redistribute(right, key, rid);

	// update the parent node
	updateParentNode(right, key);

	// all ok
	return RC_OK;
}

// delete a node entry
RC delete_entry(BTreeHandle *tree, Btree *node, Value *key) {

	int i = 0, j;

	for (i = 0; node->keys[i] != key->v.intV; ++i) {
	}

	if (node->num_keys > 1) {

		for (j = i; j < node->num_keys - 1; j++) {

			// get the node details
			node->records[j] = node->records[j + 1];
			node->keys[j] = node->keys[j + 1];
			node->pointers[j] = node->pointers[j + 1];
		}

		node->pointers[j] = node->pointers[j + 1];

		// update the total number of keys
		node->num_keys = node->num_keys - 1;

	} else {

		// reset the key values
		node->records[i].slot = 0;
		node->records[i].page = 0;
		node->keys[i] = NULL;
		node->pointers[i] = NULL;

		node->num_keys = node->num_keys - 1;

		if (node->num_keys != 0) {

			// merge the nodes if the num of keys is non zero
			merge_nodes(node->prev, node);
		}

		node = NULL;
	}

	// all done
	return RC_OK;
}

Btree* find_leaf(BTreeHandle *tree, Value *key) {
	int i = 0;
	Btree *root, *temp;
	Btree_stat *btstat;
	btstat = tree->mgmtData;
	root = btstat->mgmtData;
	root = btstat->mgmtData;
	temp = root;
	while (temp->is_leaf == false) {
		for (i = 0; i < root->num_keys; i++) {
			if (key->v.intV < root->keys[i]) {
				temp = root->pointers[i];
				break;
			} else {
				temp = root->pointers[i + 1];
			}
		}
	}
	return temp;
}

RC updateParent(Btree *child) {
	Btree *parent;
	int i = 0, j;
	parent = child->parent;
	if (parent) {
		i = 0;
		while (i < parent->num_keys) {
			if (parent->keys[i] == child->keys[0]) {
				break;
			}
			i++;
		}

		j = i;
		while (j < parent->num_keys) {
			parent->keys[j] = parent->keys[j + 1];
			parent->pointers[j] = parent->pointers[j + 1];
			j++;
		}
		if (parent->num_keys == 0) {
			merge_nodes(parent->prev, parent);
			return RC_OK;
		}
		updateParent(parent);
	}
	return RC_OK;
}

// merge the two nodes
RC merge_nodes(Btree_stat *stat, Btree *left, Btree *right) {

	int nleft, nright, index, i, j = 0;

	if (left) {

		nleft = stat->order - left->num_keys;
		index = left->num_keys;
		nright = right->num_keys;

		if (nleft >= nright) {

			for (i = index; i < nright + 1; i++) {
				left->keys[i] = right->keys[j];
				left->records[i] = right->records[j++];
			}

			// update the parent
			updateParent(right);

			left->next = right->next;

			right = NULL;
		}
	}

	// all ok
	return RC_OK;
}

// internal function for delete the parent node
RC delete_parent_nodes_inital(Btree_stat *info, Btree *root, Value *key) {

	int i = 0, j;

	// sanity checks
	if (info == NULL || root == NULL)
		printf("\n invalid stat and root");

	// the root should exisst
	if (root) {

		while (key->v.intV != root->keys[i])
			i = i + 1;

		// the total number of keys should be greater than 1
		if (root->num_keys > 1) {

			// traverse through the nodes
			for (j = i; j < root->num_keys - 1; j++) {

				// get the keys details
				root->records[j] = root->records[j + 1];
				root->keys[j] = root->keys[j + 1];
				root->pointers[j + 1] = root->pointers[j + 2];
			}

			// decrement key count
			root->num_keys = root->num_keys - 1;

		} else {

			// reset the values
			root->records[i].slot = 0;
			root->records[i].page = 0;
			root->keys[i] = NULL;
			root->pointers[i + 1] = NULL;

			// the key count should be non zero
			if (root->num_keys) {

				// merge the nodes
				merge_nodes(info, root->prev, root);
			}
		}

		// recurse till we balance out the tree
		delete_parent_nodes_inital(info, root->parent, key);
	}

	// all ok
	return RC_OK;
}

RC updateFirst(Btree *child, Value *key) {
	Btree *parent;
	int i = 0;
	parent = child->parent;
	if (parent) {

		// traverse through the tree
		for (i = 0; i < parent->num_keys; i++) {

			// check if we have a match
			if (parent->keys[i] == key->v.intV) {

				parent->keys[i] = child->keys[0];
			}
		}

		// recurse till the tree is balanced
		updateFirst(parent, key);
	}
	return RC_OK;
}
