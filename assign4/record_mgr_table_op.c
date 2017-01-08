/*
 * record_mgr_table_op.c
 *
 *  Created on: Nov 1, 2015
 *      Author: heisenberg
 */

#include "dberror.h"
#include "malloc.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "tables.h"
#include "string.h"

#define OFFSET_TOTAL_PAGE	0
#define OFFSET_TUPLE_COUNT	OFFSET_TOTAL_PAGE + 4
#define OFFSET_RECORD_SIZE	OFFSET_TUPLE_COUNT + 4
#define OFFSET_PHYS_RECORD_SIZE	OFFSET_RECORD_SIZE + 4
#define OFFSET_SLOT_CAP_PAGE	OFFSET_PHYS_RECORD_SIZE + 4
#define OFFSET_AVAIL_BYTES_LAST_PAGE	OFFSET_SLOT_CAP_PAGE + 4
#define OFFSET_FREE_SPACE_PAGE	OFFSET_AVAIL_BYTES_LAST_PAGE + 4
#define OFFSET_FREE_SPACE_SLOT	OFFSET_FREE_SPACE_PAGE + 4
#define	OFFSET_TBL_NAME_SIZE	OFFSET_FREE_SPACE_SLOT + 4
#define	OFFSET_SCHEMA_SIZE	OFFSET_TBL_NAME_SIZE + 4
#define	OFFSET_VAR_DATA	OFFSET_SCHEMA_SIZE + 4

#define INIT_FREE_PAGE	1
#define INIT_NUM_RECORDS	0
#define INIT_FREE_SLOT	0
#define INIT_PAGE_TOTAL	2
#define INIT_LAST_PAGE_AVL_BYTES	PAGE_SIZE

#define PRIVATE static

#define TBL_FILE_EXT	".tbl"
#define TBL_INDEX_EXT	".idx"

#define RECORD_OFF_PAGE_ID	0
#define RECORD_OFF_SLOT_ID	RECORD_OFF_PAGE_ID + 4
#define RECORD_OFF_NULL_MAP	RECORD_OFF_SLOT_ID + 4
#define RECORD_OFF_DATA	RECORD_OFF_NULL_MAP + 2

PRIVATE inline void updateTableMetadata(RM_TableData *);
PRIVATE void initAttrOffsets(Schema *schema);

RC createIndex(char *name);
void exit(int __status);

/**
 * Creates table file on disk. Schema is written to metadata page (index 0).
 *
 * name = name of the table to be created
 * schema = schema of the table to be created
 */
RC createTable(char *name, Schema *schema) {

	//Sanity checks
	if (name == NULL || strlen(name) == 0) {
		THROW(RC_REC_MGR_INVALID_TBL_NAME, "Invalid table name");
	}

	if (schema == NULL) {
		THROW(RC_INVALID_HANDLE, "Schema handle is invalid");
	}

	char* tblFile = (char*) malloc(strlen(name) + strlen(TBL_FILE_EXT) + 1);
	memset(tblFile, '\0', strlen(name) + strlen(TBL_FILE_EXT) + 1);

	if (tblFile == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	memcpy(tblFile, name, strlen(name));
	strcat(tblFile, TBL_FILE_EXT);

	createPageFile(tblFile);

	SM_FileHandle tblFileH;
	openPageFile(tblFile, &tblFileH);
	ensureCapacity(INIT_PAGE_TOTAL, &tblFileH);

	//Write table metadata page
	SM_PageHandle page = (SM_PageHandle) malloc(PAGE_SIZE);
	memset(page, '\0', PAGE_SIZE);

	unsigned int pageCnt = INIT_PAGE_TOTAL;
	memcpy(page + OFFSET_TOTAL_PAGE, (void *) &pageCnt, sizeof(pageCnt));

	unsigned int tupleCnt = INIT_NUM_RECORDS;
	memcpy(page + OFFSET_TUPLE_COUNT, (void *) &tupleCnt, sizeof(tupleCnt));

	unsigned int recordSize = getRecordSize(schema);
	memcpy(page + OFFSET_RECORD_SIZE, (void *) &recordSize, sizeof(recordSize));

	unsigned int physRecordSize = getSerPhysRecordSize(schema);
	memcpy(page + OFFSET_PHYS_RECORD_SIZE, (void *) &physRecordSize,
			sizeof(physRecordSize));

	unsigned int slotCapPerPage = PAGE_SIZE / physRecordSize;
	memcpy(page + OFFSET_SLOT_CAP_PAGE, (void *) &slotCapPerPage,
			sizeof(slotCapPerPage));

	unsigned int lastPageAvailBytes = INIT_LAST_PAGE_AVL_BYTES;
	memcpy(page + OFFSET_AVAIL_BYTES_LAST_PAGE, (void *) &lastPageAvailBytes,
			sizeof(lastPageAvailBytes));

	unsigned int freeSpacePage = INIT_FREE_PAGE;
	memcpy(page + OFFSET_FREE_SPACE_PAGE, (void *) &freeSpacePage,
			sizeof(freeSpacePage));

	unsigned int freeSpaceSlot = INIT_FREE_SLOT;
	memcpy(page + OFFSET_FREE_SPACE_SLOT, (void *) &freeSpaceSlot,
			sizeof(freeSpaceSlot));

	unsigned int tblNameSize = strlen(name);
	memcpy(page + OFFSET_TBL_NAME_SIZE, (void *) &tblNameSize,
			sizeof(tblNameSize));

	unsigned int schemaSize = getSerSchemaSize(schema);
	SerBuffer *ss = malloc(sizeof(SerBuffer));
	ss->next = 0;
	ss->size = 0;
	ss->data = malloc(schemaSize);
	memset(ss->data, '\0', schemaSize);

	serializeSchemaBin(schema, ss);
	memcpy(page + OFFSET_SCHEMA_SIZE, (void *) &schemaSize, sizeof(schemaSize));

	memcpy(page + OFFSET_VAR_DATA, (void *) name, tblNameSize);

	memcpy(page + OFFSET_VAR_DATA + tblNameSize, (void *) ss->data, schemaSize);

	writeBlock(0, &tblFileH, page);

	free(ss->data);
	free(ss);
	free(page);

	//Initialize slots in 1st page
	page = (SM_PageHandle) malloc(PAGE_SIZE);
	memset(page, '\0', PAGE_SIZE);
	writeBlock(1, &tblFileH, page);
	free(page);

	closePageFile(&tblFileH);
	free(tblFile);

	// check if primary key exists
	if (schema->keySize == 1) {
		createIndex(name);
	}

	//All OK
	return RC_OK;
}

/**
 * Opens a table created by createTable API
 *
 * rel = table handle to be initialized for client interacting with table name
 * name = name of the table to be opened
 *
 */
RC openTable(RM_TableData *rel, char *name) {

	//Sanity Checks
	if (name == NULL || strlen(name) == 0) {
		THROW(RC_REC_MGR_INVALID_TBL_NAME, "Invalid table name");
	}

	if (rel == NULL) {
		THROW(RC_INVALID_HANDLE, "Table handle is invalid");
	}

	char* tblFile = (char*) malloc(strlen(name) + strlen(TBL_FILE_EXT) + 1);

	if (tblFile == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	//	memcpy(tblFile, name, strlen(name));
	strcpy(tblFile, name);
	strcat(tblFile, TBL_FILE_EXT);

	rel->mgmtData = (RM_TableMgmtData *) malloc(sizeof(RM_TableMgmtData));
	((RM_TableMgmtData *) rel->mgmtData)->bPool = (BM_BufferPool *) malloc(
			sizeof(BM_BufferPool));
	initBufferPool(((RM_TableMgmtData *) rel->mgmtData)->bPool, tblFile, 3,
			RS_FIFO, NULL);

	BM_PageHandle *tableInfoPage = (BM_PageHandle *) malloc(
			sizeof(BM_PageHandle));

	if (tableInfoPage == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	pinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, tableInfoPage, 0);

	unsigned int *data =
			(unsigned int*) (&tableInfoPage->data[OFFSET_TOTAL_PAGE]);
	((RM_TableMgmtData *) rel->mgmtData)->pageCount = *data;

	data = (unsigned int*) (&tableInfoPage->data[OFFSET_TUPLE_COUNT]);
	((RM_TableMgmtData *) rel->mgmtData)->tupleCount = *data;

	data = (unsigned int*) (&tableInfoPage->data[OFFSET_RECORD_SIZE]);
	((RM_TableMgmtData *) rel->mgmtData)->recordSize = *data;

	data = (unsigned int*) (&tableInfoPage->data[OFFSET_PHYS_RECORD_SIZE]);
	((RM_TableMgmtData *) rel->mgmtData)->physicalRecordSize = *data;

	data = (unsigned int*) (&tableInfoPage->data[OFFSET_SLOT_CAP_PAGE]);
	((RM_TableMgmtData *) rel->mgmtData)->slotCapacityPage = *data;

	data = (unsigned int*) (&tableInfoPage->data[OFFSET_AVAIL_BYTES_LAST_PAGE]);
	((RM_TableMgmtData *) rel->mgmtData)->availBytesLastPage = *data;

	data = (unsigned int*) (&tableInfoPage->data[OFFSET_FREE_SPACE_PAGE]);
	((RM_TableMgmtData *) rel->mgmtData)->firstFreeSlot.page = *data;

	data = (unsigned int*) (&tableInfoPage->data[OFFSET_FREE_SPACE_SLOT]);
	((RM_TableMgmtData *) rel->mgmtData)->firstFreeSlot.slot = *data;

	data = (unsigned int*) (&tableInfoPage->data[OFFSET_TBL_NAME_SIZE]);
	((RM_TableMgmtData *) rel->mgmtData)->tblNameSize = *data;

	data = (unsigned int*) (&tableInfoPage->data[OFFSET_SCHEMA_SIZE]);
	((RM_TableMgmtData *) rel->mgmtData)->serSchemaSize = *data;

	char* tblName = (char*) (&tableInfoPage->data[OFFSET_VAR_DATA]);
	rel->name = (char*) malloc(
			((RM_TableMgmtData *) rel->mgmtData)->tblNameSize);
	memcpy(rel->name, tblName,
			((RM_TableMgmtData *) rel->mgmtData)->tblNameSize);

	char* schemaData = (char*) (&tableInfoPage->data[OFFSET_VAR_DATA
													 + ((RM_TableMgmtData *) rel->mgmtData)->tblNameSize]);
	((RM_TableMgmtData *) rel->mgmtData)->serSchema = (char*) malloc(
			((RM_TableMgmtData *) rel->mgmtData)->serSchemaSize);
	memcpy(((RM_TableMgmtData *) rel->mgmtData)->serSchema, schemaData,
			((RM_TableMgmtData *) rel->mgmtData)->serSchemaSize);

	SerBuffer buff;
	buff.data = ((RM_TableMgmtData *) rel->mgmtData)->serSchema;
	buff.size = ((RM_TableMgmtData *) rel->mgmtData)->serSchemaSize;

	deserializeSchemaBin(&buff, &rel->schema);

	unpinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, tableInfoPage);

	initAttrOffsets(rel->schema);

	free(tableInfoPage);
	free(tblFile);

	// open the index file as well if exists
	//All OK
	return RC_OK;
}

/**
 * Closes opened table
 *
 * rel = handle of the previously opened table
 */
RC closeTable(RM_TableData *rel) {

	//Sanity Checks
	if (rel == NULL) {
		THROW(RC_INVALID_HANDLE, "Table handle is invalid");
	}

	updateTableMetadata(rel);

	free(rel->name);
	freeSchema(rel->schema);

	RC ret = shutdownBufferPool(((RM_TableMgmtData *) rel->mgmtData)->bPool);

	free(((RM_TableMgmtData *) rel->mgmtData)->bPool);
	free(((RM_TableMgmtData *) rel->mgmtData)->serSchema);
	free(rel->mgmtData);

	return ret;
}

/**
 * Deletes the table data file from disk
 *
 * name = name of the table to be deleted
 */
RC deleteTable(char *name) {

	//Sanity Checks
	if (name == NULL || strlen(name) == 0) {
		THROW(RC_REC_MGR_INVALID_TBL_NAME, "Invalid table name");
	}

	char* tblFile = (char*) malloc(strlen(name) + strlen(TBL_FILE_EXT) + 1);

	if (tblFile == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	//	memcpy(tblFile, name, strlen(name));
	//	strcat(tblFile, TBL_FILE_EXT);

	strcpy(tblFile, name);
	strcat(tblFile, TBL_FILE_EXT);

	RC ret = destroyPageFile(tblFile);

	free(tblFile);

	return ret;
}

/**
 * Creates and returns the schema handle for client's use
 *
 * numAttr = total number of attributes in table
 * attrNames = char* array of names of attributes in table
 * dataTypes = data type array of each attribute in table
 * typeLength = int array of length of each attribute in table. Only used for String type.
 * keySize = number of key attributes from schema
 * keys = int array of indices of key attributes
 *
 */
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
		int *typeLength, int keySize, int *keys) {

	//Sanity Checks
	if (numAttr == 0 || attrNames == NULL || dataTypes == NULL) {
		printf("One or more schema options are invalid");
		exit(RC_REC_MGR_INVALID_SCHEMA);
	}

	Schema *ret = malloc(sizeof(Schema));
	if (ret == NULL) {
		printf("Not enough memory available for resource allocation");
		exit(RC_NOT_ENOUGH_MEMORY);
	}

	//Set schema fields
	ret->numAttr = numAttr;
	ret->attrNames = attrNames;
	ret->dataTypes = dataTypes;
	ret->typeLength = typeLength;

	int i;
	//Iterate through array of all the attribute types and set data type lengths for non-numeric attributes
	for (i = 0; i < ret->numAttr; i++) {
		switch (ret->dataTypes[i]) {
		case DT_INT:
			ret->typeLength[i] = sizeof(int);
			break;
		case DT_FLOAT:
			ret->typeLength[i] = sizeof(float);
			break;
		case DT_BOOL:
			ret->typeLength[i] = sizeof(bool);
			break;
		case DT_STRING:
			break;
		}
	}

	ret->keySize = keySize;
	ret->keyAttrs = keys;

	initAttrOffsets(ret);

	return ret;
}

/**
 * Frees up resources used by schema handle
 *
 * schema = schema handle to be freed
 */
RC freeSchema(Schema *schema) {

	//Sanity Checks
	if (schema == NULL || schema->attrNames == NULL || schema->dataTypes == NULL) {
		THROW(RC_INVALID_HANDLE, "Schema handle is invalid");
	}

	int i;
	//Free all the attribute names individually
	for (i = 0; i < schema->numAttr; i++) {
		free(schema->attrNames[i]);
	}

	free(schema->attrNames);

	free(schema->dataTypes);

	if (schema->typeLength != NULL)
		free(schema->typeLength);

	if (schema->keyAttrs != NULL)
		free(schema->keyAttrs);

	free(schema->attrOffsets);

	//Finally free whole schema
	free(schema);

	//All OK
	return RC_OK;
}

/*inline void initPageSlots(char* page, Schema *schema, unsigned int pageID,
 unsigned int slotCapPerPage) {

 unsigned int i = 0;
 int nextSlot = 0;
 int localPageID = pageID;

 unsigned int recordValSize = getRecordSize(schema);
 unsigned int physRecordSize = getSerPhysRecordSize(schema);
 unsigned int recordBase = 0;

 for (; i < slotCapPerPage; i++) {
 recordBase = i * physRecordSize;

 Record r;
 r.id.page = localPageID;
 r.id.slot = i;
 r.nullMap = 0x7FFF;
 r.data = malloc(recordValSize);
 memset(r.data, '\0', recordValSize);

 SerBuffer *ss = malloc(sizeof(SerBuffer));
 ss->next = 0;
 ss->size = 0;
 ss->data = malloc(physRecordSize);
 memset(ss->data, '\0', physRecordSize);

 nextSlot = i + 1;

 if (i == slotCapPerPage - 1) {
 localPageID = -1;
 nextSlot = -1;
 }

 memcpy(r.data, (void *) &localPageID, sizeof(localPageID));
 memcpy(r.data + sizeof(localPageID), (void *) &nextSlot,
 sizeof(nextSlot));

 serializeRecordBin(&r, recordValSize, ss);
 memcpy(page + recordBase, ss->data, physRecordSize);

 free(r.data);
 free(ss->data);
 free(ss);
 }
 }*/

/**
 * Private utility function to write back updated table meta data to meta data page.
 *
 * rel = Handle to table whose meta data is to be written
 */
PRIVATE inline void updateTableMetadata(RM_TableData *rel) {
	BM_PageHandle *tableInfoPage = (BM_PageHandle *) malloc(
			sizeof(BM_PageHandle));
	pinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, tableInfoPage, 0);

	unsigned int pageCnt = ((RM_TableMgmtData *) rel->mgmtData)->pageCount;
	memcpy(tableInfoPage->data + OFFSET_TOTAL_PAGE, (void *) &pageCnt,
			sizeof(pageCnt));

	unsigned int tupleCnt = ((RM_TableMgmtData *) rel->mgmtData)->tupleCount;
	memcpy(tableInfoPage->data + OFFSET_TUPLE_COUNT, (void *) &tupleCnt,
			sizeof(tupleCnt));

	unsigned int lastPageAvailBytes =
			((RM_TableMgmtData *) rel->mgmtData)->availBytesLastPage;
	memcpy(tableInfoPage->data + OFFSET_AVAIL_BYTES_LAST_PAGE,
			(void *) &lastPageAvailBytes, sizeof(lastPageAvailBytes));

	unsigned int freeSpacePage =
			((RM_TableMgmtData *) rel->mgmtData)->firstFreeSlot.page;
	memcpy(tableInfoPage->data + OFFSET_FREE_SPACE_PAGE,
			(void *) &freeSpacePage, sizeof(freeSpacePage));

	unsigned int freeSpaceSlot =
			((RM_TableMgmtData *) rel->mgmtData)->firstFreeSlot.slot;
	memcpy(tableInfoPage->data + OFFSET_FREE_SPACE_SLOT,
			(void *) &freeSpaceSlot, sizeof(freeSpaceSlot));

	markDirty(((RM_TableMgmtData *) rel->mgmtData)->bPool, tableInfoPage);
	unpinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, tableInfoPage);

	free(tableInfoPage);
}

PRIVATE void initAttrOffsets(Schema *schema) {
	schema->attrOffsets = malloc(sizeof(unsigned int) * schema->numAttr);

	int i = 0;
	unsigned int attrOff = 0;
	for (; i < schema->numAttr; i++) {
		schema->attrOffsets[i] = attrOff;
		switch (schema->dataTypes[i]) {
		case DT_INT:
			attrOff += sizeof(int);
			break;
		case DT_FLOAT:
			attrOff += sizeof(float);
			break;
		case DT_STRING:
			attrOff += schema->typeLength[i];
			break;
		case DT_BOOL:
			attrOff += sizeof(bool);
			break;
		}
	}
}

// PRIMARY KEY CODE
int getIndexRecSize() {
	return (sizeof(int) * 3);
}

int getTotalTuplesPerPage() {
	int page_size = PAGE_SIZE - 5;

	return page_size / getIndexRecSize();
}

int getPageNumber(int pk) {
	int totalTuplesPerPage = getTotalTuplesPerPage();

	return pk / totalTuplesPerPage;
}

RC createIndex(char *name) {
	//Sanity checks
	if (name == NULL || strlen(name) == 0) {
		THROW(RC_REC_MGR_INVALID_TBL_NAME, "Invalid index file name");
	}

	char* idxFile = (char*) malloc(strlen(name) + strlen(TBL_INDEX_EXT) + 1);
	memset(idxFile, '\0', strlen(name) + strlen(TBL_INDEX_EXT) + 1);

	if (idxFile == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	memcpy(idxFile, name, strlen(name));
	strcat(idxFile, TBL_INDEX_EXT);

	createPageFile(idxFile);

	SM_FileHandle tblFileH;
	openPageFile(idxFile, &tblFileH);
	ensureCapacity(INIT_PAGE_TOTAL, &tblFileH);

	SM_PageHandle page = (SM_PageHandle) malloc(PAGE_SIZE);
	memset(page, '\0', PAGE_SIZE);

	writeBlock(0, &tblFileH, page);
	free(page);

	//Initialize slots in 1st page
	page = (SM_PageHandle) malloc(PAGE_SIZE);
	memset(page, '\0', PAGE_SIZE);
	writeBlock(1, &tblFileH, page);
	free(page);

	closePageFile(&tblFileH);
	free(idxFile);

	//All OK
	return RC_OK;
}

/*RC openIndex(char *name, int size) {
 //Sanity Checks
 if (name == NULL || strlen(name) == 0) {
 THROW(RC_REC_MGR_INVALID_TBL_NAME, "Invalid table name");
 }

 if (rel == NULL) {
 THROW(RC_INVALID_HANDLE, "Table handle is invalid");
 }

 char* idxFile = (char*) malloc(strlen(name) + strlen(TBL_INDEX_EXT) + 1);

 if (idxFile == NULL) {
 THROW(RC_NOT_ENOUGH_MEMORY,
 "Not enough memory available for resource allocation");
 }

 //      memcpy(tblFile, name, strlen(name));
 strcpy(idxFile, name);
 strcat(idxFile, TBL_INDEX_EXT);

 BM_BufferPool * bPool = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
 initBufferPool(((RM_TableMgmtData *) rel->mgmtData)->bPool, idxFile, 3,
 RS_FIFO, NULL);

 BM_PageHandle *tableInfoPage = (BM_PageHandle *) malloc(
 sizeof(BM_PageHandle));

 if (tableInfoPage == NULL) {
 THROW(RC_NOT_ENOUGH_MEMORY,
 "Not enough memory available for resource allocation");
 }

 pinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, tableInfoPage, 0);

 unpinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, tableInfoPage);

 free(tableInfoPage);

 free(tblFile);

 //All OK
 return RC_OK;
 }*/

RC closeIndex(RM_TableData *rel) {

	//Sanity Checks
	if (rel == NULL) {
		THROW(RC_INVALID_HANDLE, "Table handle is invalid");
	}

	free(rel->name);
	freeSchema(rel->schema);

	RC ret = shutdownBufferPool(((RM_TableMgmtData *) rel->mgmtData)->bPool);

	free(((RM_TableMgmtData *) rel->mgmtData)->bPool);
	free(((RM_TableMgmtData *) rel->mgmtData)->serSchema);
	free(rel->mgmtData);

	return ret;

}

RC deleteIndex(char *name) {

	//Sanity Checks
	if (name == NULL || strlen(name) == 0) {
		THROW(RC_REC_MGR_INVALID_TBL_NAME, "Invalid table name");
	}

	char* tblFile = (char*) malloc(strlen(name) + strlen(TBL_INDEX_EXT) + 1);

	if (tblFile == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	//memcpy(tblFile, name, strlen(name)-1);
	//strcat(tblFile, TBL_INDEX_EXT);

	strcpy(tblFile, name);
	strcat(tblFile, TBL_INDEX_EXT);
	RC ret = destroyPageFile(tblFile);
	free(tblFile);

	return ret;
}

bool checkIfPKExists(char* name, int size, int pk) {
	int page_no = getPageNumber(pk);
	int recordSize = getIndexRecSize();
	int base;
	int mod;
	bool found = false;

	if (page_no == 0) {
		base = 5;
		mod = (PAGE_SIZE - 5) / recordSize;
	} else {
		base = 5;
		mod = (PAGE_SIZE - 5) / recordSize;
	}

	if (name == NULL || strlen(name) == 0) {
		THROW(RC_REC_MGR_INVALID_TBL_NAME, "Invalid table name");
	}

	char* idxFile = (char*) malloc(size + strlen(TBL_INDEX_EXT) + 1);
	memset(idxFile, '\0', size + strlen(TBL_FILE_EXT) + 1);
	if (idxFile == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	memcpy(idxFile, name, size);
	strcat(idxFile, TBL_INDEX_EXT);

	if (page_no >= 0) {
		SM_FileHandle idxFileH;
		openPageFile(idxFile, &idxFileH);
		int* rec;
		int offset;

		if (page_no <= idxFileH.totalNumPages) {
			SM_PageHandle page = (SM_PageHandle) malloc(PAGE_SIZE);
			memset(page, '\0', PAGE_SIZE);
			readBlock(page_no, &idxFileH, page);

			// go to the slot corresponding to the value of given primary key
			// pk serves as a slot number
			offset = base + recordSize * (pk % mod);
			rec = (int *) (page + offset);
			if (*rec == 0) {
				// slot is empty
				// pk is unique
				found = false;
			} else {
				found = true;
			}

			free(page);
		} else
			found = false;

		closePageFile(&idxFileH);
	}

	free(idxFile);
	return found;
}

RC addPrimaryKey(char* name, int size, int pk, RID id) {
	int page_no = getPageNumber(pk);
	int recordSize = getIndexRecSize();
	int base, mod;

	if (page_no == 0) {
		base = 5;
		mod = (PAGE_SIZE - 5) / recordSize;
	} else {
		base = 5;
		mod = (PAGE_SIZE - 5) / recordSize;
	}

	if (name == NULL || strlen(name) == 0) {
		THROW(RC_REC_MGR_INVALID_TBL_NAME, "Invalid table name");
	}

	char* idxFile = (char*) malloc(size + strlen(TBL_INDEX_EXT) + 1);
	memset(idxFile, '\0', size + strlen(TBL_FILE_EXT) + 1);

	if (idxFile == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	memcpy(idxFile, name, size);
	strcat(idxFile, TBL_INDEX_EXT);

	if (page_no >= 0) {
		SM_FileHandle idxFileH;
		openPageFile(idxFile, &idxFileH);
		int *rec;
		int offset;

		SM_PageHandle page = (SM_PageHandle) malloc(PAGE_SIZE);

		// check if the page already exists
		if (page_no > idxFileH.totalNumPages) {
			// page does not exist
			// create it
			ensureCapacity(page_no, &idxFileH);
		}

		readBlock(page_no, &idxFileH, page);

		// pk serves as a slot number
		offset = base + recordSize * (pk % mod);
		rec = (int*) (page + offset);
		rec[0] = pk;
		rec[1] = id.page;
		rec[2] = id.slot;

		writeBlock(page_no, &idxFileH, page);

		closePageFile(&idxFileH);

		free(page);
	}

	free(idxFile);
	return RC_OK;
}

