/*
 * record_mgr_record_op.c
 *
 *  Created on: Nov 1, 2015
 *      Author: heisenberg
 */

#include "dberror.h"
#include "malloc.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "tables.h"
#include "string.h"

#define PRIVATE static

PRIVATE RC writeRecord(RM_TableData *rel, Record *record);
PRIVATE RC readRecord(RM_TableData *rel, RID id, Record **record);

bool checkIfPKExists(char* name, int size, int pk);
RC addPrimaryKey(char* name, int size, int pk, RID id);

/**
 * Returns total number of records/tuples present in table pointed by rel
 *
 * rel = table handle
 *
 */
int getNumTuples(RM_TableData *rel) {

	//Sanity Check
	if (rel == NULL) {
		THROW(RC_INVALID_HANDLE, "Table handle is invalid");
	}

	return ((RM_TableMgmtData *) rel->mgmtData)->tupleCount;
}

/**
 * Inserts new record into the table
 *
 * rel = table handle
 * record = record to be inserted
 *
 */
RC insertRecord(RM_TableData *rel, Record *record) {

	Value* val = NULL;
	bool pk = false;

	//Sanity Checks
	if (rel == NULL) {
		THROW(RC_INVALID_HANDLE, "Table handle is invalid");
	}

	if (record == NULL) {
		THROW(RC_INVALID_HANDLE, "Record handle is invalid");
	}

	// Check if this table has primary key defined
	if (rel->schema->keySize == 1) {
		pk = true;
		// ensure pk constraint is not violated
		getAttr(record, rel->schema, rel->schema->keyAttrs[0], &val);
		if (checkIfPKExists(rel->name,
				((RM_TableMgmtData *) rel->mgmtData)->tblNameSize,
				val->v.intV)) {
			// this value already exists
			freeVal(val);
			THROW(RC_DUPLICATE_KEY, "Can not insert duplicate key");
		}
	}

	//Check if all slots in last page are full, if yes, add new page
	if (((RM_TableMgmtData *) rel->mgmtData)->firstFreeSlot.slot
			== ((RM_TableMgmtData *) rel->mgmtData)->slotCapacityPage) {
		//Add new page
		BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));

		if (page == NULL) {
			THROW(RC_NOT_ENOUGH_MEMORY,
					"Not enough memory available for resource allocation");
		}

		//Request +1th page
		pinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, page,
				((RM_TableMgmtData *) rel->mgmtData)->pageCount);
		//Mark it dirty
		markDirty(((RM_TableMgmtData *) rel->mgmtData)->bPool, page);
		unpinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, page);
		free(page);

		((RM_TableMgmtData *) rel->mgmtData)->firstFreeSlot.page =
				((RM_TableMgmtData *) rel->mgmtData)->pageCount++;
		//Point first free slot to first slot in new page
		((RM_TableMgmtData *) rel->mgmtData)->firstFreeSlot.slot = 0;
	}

	//Point new record to first free slot and page available
	record->id.page = ((RM_TableMgmtData *) rel->mgmtData)->firstFreeSlot.page;
	record->id.slot =
			((RM_TableMgmtData *) rel->mgmtData)->firstFreeSlot.slot++;

	//Write new record to its assigned page and slot
	RC rc = writeRecord(rel, record);

	// add the primary key in the index file
	if (pk) {
		//Update primary key index
		addPrimaryKey(rel->name,
				((RM_TableMgmtData *) rel->mgmtData)->tblNameSize, val->v.intV,
				record->id);
		freeVal(val);
	}

	//All OK
	if (rc == RC_OK)
		((RM_TableMgmtData *) rel->mgmtData)->tupleCount++;

	return rc;
}

/**
 * Deletes a record from table
 *
 * rel = table handle
 * id = id of the record to be deleted
 *
 */
RC deleteRecord(RM_TableData *rel, RID id) {

	//Sanity Checks
	if (rel == NULL) {
		THROW(RC_INVALID_HANDLE, "Table handle is invalid");
	}

	Record *record;
	//Read the record to be deleted
	createRecord(&record, rel->schema);
	RC rc = readRecord(rel, id, &record);
	if (rc != RC_OK) {
		THROW(RC_REC_MGR_DELETE_REC_FAILED, "Delete record failed");
	}

	//Set tomstone bit
	record->nullMap |= 1 << 15;

	//Write updated record
	rc = writeRecord(rel, record);
	freeRecord(record);

	if (rc == RC_OK)
		((RM_TableMgmtData *) rel->mgmtData)->tupleCount--;
	else {
		THROW(RC_REC_MGR_DELETE_REC_FAILED, "Delete record failed");
	}

	return rc;
}

/**
 * Writes updated record to table
 *
 * rel = table handle
 * record = updated record to be written
 *
 */
RC updateRecord(RM_TableData *rel, Record *record) {

	Value* val;
	bool pk = false;

	//Sanity Checks
	if (rel == NULL) {
		THROW(RC_INVALID_HANDLE, "Table handle is invalid");
	}

	if (record == NULL) {
		THROW(RC_INVALID_HANDLE, "Record handle is invalid");
	}

	// Check if this table has primary key defined
	if (rel->schema->keySize == 1) {
		pk = true;
		// ensure pk constraint is not violated
		getAttr(record, rel->schema, rel->schema->keyAttrs[0], &val);
		if (checkIfPKExists(rel->name,
				((RM_TableMgmtData *) rel->mgmtData)->tblNameSize,
				val->v.intV)) {
			// this value already exists
			freeVal(val);
			THROW(RC_DUPLICATE_KEY, "Can not insert duplicate key");
		}
	}

	//Write updated record
	RC rc = writeRecord(rel, record);

	// add the primary key in the index file
	if (pk) {
		addPrimaryKey(rel->name,
				((RM_TableMgmtData *) rel->mgmtData)->tblNameSize, val->v.intV,
				record->id);
		freeVal(val);
	}

	return rc;
}

/**
 * Reads a record from table
 *
 * rel = table handle
 * id = id of the record to be read
 * record = updated record to be written
 *
 */
RC getRecord(RM_TableData *rel, RID id, Record *record) {

	//Sanity Checks
	if (rel == NULL) {
		THROW(RC_INVALID_HANDLE, "Table handle is invalid");
	}

	if (record == NULL) {
		THROW(RC_INVALID_HANDLE, "Record handle is invalid");
	}

	return readRecord(rel, id, &record);
}

/**
 * Starts scan to fetch records with matching condition
 *
 * rel = table handle
 * scan = scan handle
 * cond = condition to be matched
 */
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

	//Sanity Checks
	if (rel == NULL) {
		THROW(RC_INVALID_HANDLE, "Table handle is invalid");
	}

	// Get total number of pages this table has
	int pages = ((RM_TableMgmtData *) rel->mgmtData)->pageCount;
	int tupleCount = ((RM_TableMgmtData *) rel->mgmtData)->tupleCount;
	RID firstFreeSlot = ((RM_TableMgmtData *) rel->mgmtData)->firstFreeSlot;
	int i, j, tuplesRead, slot;
	RID id;
	Value* result;
	Record** r;
	BM_PageHandle *pageData = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	RM_ScanIterator *iter = (RM_ScanIterator*) malloc(sizeof(RM_ScanIterator));

	if (pageData == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	scan->rel = rel;

	iter->totalRecords = 0;
	iter->lastRecordRead = -1;
	iter->records = (Record**) malloc(sizeof(Record*) * tupleCount);

	scan->mgmtData = (RM_ScanIterator*) iter;

	r = iter->records;
	for (i = 0; i < tupleCount; ++i) {
		r[i] = NULL;
	}

	j = 0;
	tuplesRead = 0;

	//Loop through pages and get the desired records
	for (i = 1; i < pages; i++) {
		slot = 0;
		id.page = i;

		while (slot < ((RM_TableMgmtData *) rel->mgmtData)->slotCapacityPage) {

			createRecord(&r[j], rel->schema);
			id.slot = slot;

			RC rc = readRecord(rel, id, &r[j]);
			if (rc != RC_OK) {
				THROW(RC_REC_MGR_DELETE_REC_FAILED, "Delete record failed");
			}

			// ignore the deleted records (tombstones)
			if (!(r[j]->nullMap & (1 << 15))
					&& !(firstFreeSlot.page == i && firstFreeSlot.slot == slot)) {
				evalExpr(r[j], rel->schema, cond, &result);
				if (result->v.boolV) {
					j++;
					tuplesRead++;
				}
				freeVal(result);

			}
			slot++;
			freeRecord(r[j]);
		}
	}

	iter->totalRecords = j;

	free(pageData);

	//All OK
	return RC_OK;
}

/**
 * Fetch next matching record satisfying condition expression
 *
 * scan = scan handle
 * record = next matching record returned
 */
RC next(RM_ScanHandle *scan, Record *record) {
	RM_ScanIterator* iter = (RM_ScanIterator*) scan->mgmtData;
	Record* r = iter->records[iter->lastRecordRead + 1];

	if ((iter->lastRecordRead + 1) < iter->totalRecords && r != NULL) {
		record->id.page = r->id.page;
		record->id.slot = r->id.slot;
		record->nullMap = r->nullMap;
		memcpy(record->data, r->data, getRecordSize(scan->rel->schema));

		iter->lastRecordRead++;

		return RC_OK;
	}

	return RC_RM_NO_MORE_TUPLES;
}

/**
 * Close scan handle and release resources
 *
 * scan = scan handle
 */
RC closeScan(RM_ScanHandle *scan) {
	int i;
	RM_ScanIterator* iter = (RM_ScanIterator*) scan->mgmtData;

	for (i = 0; i < iter->totalRecords; ++i) {
		if (iter->records[i]) {
			freeRecord(iter->records[i]);
			iter->records[i] = NULL;
		}
	}

	free(iter->records);
	free(iter);

	//All OK
	return RC_OK;
}

/**
 * This API allows conditional updates of the records
 *
 * rel = Table handle
 * cond = Conditional expression to be matched
 * op = Function pointer pointing to update function to be called in case a matching record is found
 */
RC updateScan(RM_TableData *rel, Expr *cond, void (*op)(Schema*, Record *)) {

	//Sanity Checks
	if (rel == NULL) {
		THROW(RC_INVALID_HANDLE, "Table handle is invalid");
	}

	// Get total number of pages this table has
	int pages = ((RM_TableMgmtData *) rel->mgmtData)->pageCount;
	RID firstFreeSlot = ((RM_TableMgmtData *) rel->mgmtData)->firstFreeSlot;
	int i, j, tuplesRead, slot;
	RID id;
	Value *result;
	Record* r;
	BM_PageHandle *pageData = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));

	if (pageData == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	j = 0;
	tuplesRead = 0;

	// loop through pages and get the desired records
	for (i = 1; i < pages; i++) {
		slot = 0;
		id.page = i;
		while (slot < ((RM_TableMgmtData *) rel->mgmtData)->slotCapacityPage) {
			createRecord(&r, rel->schema);
			id.slot = slot;
			RC rc = readRecord(rel, id, &r);
			if (rc != RC_OK) {
				THROW(RC_REC_MGR_DELETE_REC_FAILED, "Delete record failed");
			}

			// ignore the deleted records (tombstones) and free slots
			if ((!(r->nullMap & (1 << 15))
					|| ((firstFreeSlot.page == i)
							&& (firstFreeSlot.slot == slot)))) {
				// check if record satisfies the given condition
				evalExpr(r, rel->schema, cond, &result);
				if (result->v.boolV) {
					op(rel->schema, r);
					updateRecord(rel, r);
					j++;
					tuplesRead++;
				}
				freeVal(result);
			}

			freeRecord(r);
			slot++;
		}
	}

	free(pageData);

	//All OK
	return RC_OK;
}

/**
 * Private utility function to read records from physical storage.
 * It deserializes records stored in page file and returns as Record object.
 *
 */
PRIVATE RC readRecord(RM_TableData *rel, RID id, Record **record) {
	SerBuffer *ss = malloc(sizeof(SerBuffer));
	ss->next = 0;
	ss->size = 0;
	ss->data = malloc(((RM_TableMgmtData *) rel->mgmtData)->physicalRecordSize);

	//Calculate the offset of the slot in page file from where record is to be read
	unsigned int slotOffset =
			((RM_TableMgmtData *) rel->mgmtData)->physicalRecordSize * id.slot;

	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));

	if (page == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	//Pin the page where record slot is present
	pinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, page, id.page);
	unpinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, page);

	//Copy slot data from page to de-serialzation bufffer
	memcpy(ss->data, page->data + slotOffset,
			((RM_TableMgmtData *) rel->mgmtData)->physicalRecordSize);

	//Deserialize the record just read
	deserializeRecordBin(ss, ((RM_TableMgmtData *) rel->mgmtData)->recordSize,
			record);

	free(page);
	free(ss->data);
	free(ss);

	//All OK
	return RC_OK;
}

/**
 * Writes record to the underlying physical storage file.
 * It is first serialized and then written.
 *
 */
PRIVATE RC writeRecord(RM_TableData *rel, Record *record) {

	SerBuffer *ss = malloc(sizeof(SerBuffer));
	ss->next = 0;
	ss->size = 0;
	ss->data = malloc(((RM_TableMgmtData *) rel->mgmtData)->physicalRecordSize);

	//Serialize the record into binary format
	serializeRecordBin(record, ((RM_TableMgmtData *) rel->mgmtData)->recordSize,
			ss);

	//Calculate offset of the slow in page file where record is to be written
	unsigned int slotOffset =
			((RM_TableMgmtData *) rel->mgmtData)->physicalRecordSize
			* record->id.slot;

	BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));

	if (page == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	//Pin the page where record is to be written
	pinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, page, record->id.page);

	//Copy serialized record to page slot
	memcpy(page->data + slotOffset, ss->data,
			((RM_TableMgmtData *) rel->mgmtData)->physicalRecordSize);

	//Mark page as dirty as record has been written to it
	markDirty(((RM_TableMgmtData *) rel->mgmtData)->bPool, page);
	unpinPage(((RM_TableMgmtData *) rel->mgmtData)->bPool, page);

	//	free(page->data);
	free(page);
	free(ss->data);
	free(ss);

	//All OK
	return RC_OK;
}

/**
 * Returns the size (in int) of the fixed sized records defined in schema
 *
 * schema = input schema
 */
int getRecordSize(Schema *schema) {

	//Sanity Checks
	if (schema == NULL) {
		THROW(RC_INVALID_HANDLE, "Schema handle is invalid");
	}

	int size = 0;

	int i;
	//Iterate through array of all the attribute types and increment size accordingly
	for (i = 0; i < schema->numAttr; i++) {
		switch (schema->dataTypes[i]) {
		case DT_INT:
			size += sizeof(int);
			break;
		case DT_FLOAT:
			size += sizeof(float);
			break;
		case DT_STRING:
			size += schema->typeLength[i];
			break;
		case DT_BOOL:
			size += sizeof(bool);
			break;
		}
	}

	return size;
}

/**
 * Creates and returns the record handle to be used by client
 *
 * record = record handle to be initialized
 * schema = schema on which record is to be based
 */
RC createRecord(Record **record, Schema *schema) {

	//Sanity Checks
	if (schema == NULL) {
		THROW(RC_INVALID_HANDLE, "Schema handle is invalid");
	}

	*record = (Record *) malloc(sizeof(Record));

	if (record == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	int recordLen = getRecordSize(schema);

	//Reset the NULL map
	(*record)->nullMap = 0x0000;
	(*record)->data = (char*) malloc(recordLen);
	memset((*record)->data, '\0', recordLen);

	if ((*record)->data == NULL) {
		THROW(RC_NOT_ENOUGH_MEMORY,
				"Not enough memory available for resource allocation");
	}

	//All OK
	return RC_OK;
}

/**
 *	Reads an attribute's value from record data
 *
 *	record = record from which value is to be read
 *	schema = table schema handle
 *	attrNum = index of the attribute to be read
 *	value = value to be returned
 *
 */
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {

	//Sanity Checks
	if (record == NULL) {
		THROW(RC_INVALID_HANDLE, "Record handle is invalid");
	}

	if (schema == NULL) {
		THROW(RC_INVALID_HANDLE, "Schema handle is invalid");
	}

	if (attrNum < 0 || attrNum >= schema->numAttr) {
		THROW(RC_REC_MGR_INVALID_ATTR, "Schema handle is invalid");
	}

	int *i;
	float *f;
	bool *b;
	char* data;

	switch (schema->dataTypes[attrNum]) {
	case DT_INT:
		i = (int*) (&record->data[schema->attrOffsets[attrNum]]);
		MAKE_VALUE(*value, DT_INT, *i);
		break;
	case DT_FLOAT:
		f = (float*) (&record->data[schema->attrOffsets[attrNum]]);
		MAKE_VALUE(*value, DT_FLOAT, *f);
		break;
	case DT_BOOL:
		b = (bool*) (&record->data[schema->attrOffsets[attrNum]]);
		MAKE_VALUE(*value, DT_BOOL, *b);
		break;
	case DT_STRING:
		data = (char*) malloc(schema->typeLength[attrNum] + 1);
		memset(data, '\0', schema->typeLength[attrNum] + 1);
		memcpy(data, (char*) (&record->data[schema->attrOffsets[attrNum]]),
				schema->typeLength[attrNum]);
		(*value) = (Value *) malloc(sizeof(Value));
		(*value)->dt = DT_STRING;
		(*value)->v.stringV = (char *) malloc(strlen(data) + 1);
		memcpy((*value)->v.stringV, data, strlen(data) + 1);
		free(data);
		break;
	}

	//All OK
	return RC_OK;
}

/**
 *	Check if an attribute value is NULL in record
 *
 *	rel = table handle
 *	record = record from which attribute is to be checked for NULLability
 *	attrNum = index of the attribute to be checked for NULLability
 *
 */
bool isNULLAttr(RM_TableData *rel, Record *record, int attrNum) {

	//Sanity Checks
	if (record == NULL) {
		THROW(RC_INVALID_HANDLE, "Record handle is invalid");
	}

	if (attrNum < 0 || attrNum >= rel->schema->numAttr) {
		THROW(RC_REC_MGR_INVALID_ATTR, "Schema handle is invalid");
	}

	//Check if NULL bit is set for attribute at index attrNum
	return (record->nullMap >> attrNum) & 1;
}

/**
 *	Sets attribute value in a record.
 *
 *	record = record whose value is to be set
 *	schema = table schema handle
 *	attrNum = index of the attribute whose value is to be set
 *	value = value to be assigned
 *
 */
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {

	//Sanity Checks
	if (record == NULL) {
		THROW(RC_INVALID_HANDLE, "Record handle is invalid");
	}

	if (schema == NULL) {
		THROW(RC_INVALID_HANDLE, "Schema handle is invalid");
	}

	if (attrNum < 0 || attrNum >= schema->numAttr) {
		THROW(RC_REC_MGR_INVALID_ATTR, "Schema handle is invalid");
	}

	if (value != NULL) {
		if (value->dt != schema->dataTypes[attrNum]) {
			THROW(RC_REC_MGR_INVALID_ATTR,
					"Value type doesn't match the type set in schema");
		}

		if (schema->dataTypes[attrNum] == DT_STRING) {
			if (strlen(value->v.stringV) > schema->typeLength[attrNum]) {
				THROW(RC_REC_MGR_INVALID_ATTR,
						"String is longer than defined in schema");
			}
			memcpy(record->data + schema->attrOffsets[attrNum],
					value->v.stringV, schema->typeLength[attrNum]);
		} else if (schema->dataTypes[attrNum] == DT_INT) {
			memcpy(record->data + schema->attrOffsets[attrNum],
					(void *) &value->v.intV, schema->typeLength[attrNum]);
		} else if (schema->dataTypes[attrNum] == DT_BOOL) {
			memcpy(record->data + schema->attrOffsets[attrNum],
					(void *) &value->v.boolV, schema->typeLength[attrNum]);
		} else if (schema->dataTypes[attrNum] == DT_FLOAT) {
			memcpy(record->data + schema->attrOffsets[attrNum],
					(void *) &value->v.floatV, schema->typeLength[attrNum]);
		}

		record->nullMap |= 0 << attrNum;
	} else {

		record->nullMap |= 1 << attrNum;
	}

	//All OK
	return RC_OK;
}

/**
 * Frees up record handle
 *
 * record = record to be freed
 */
RC freeRecord(Record *record) {

	//Sanity Checks
	if (record == NULL || record->data == NULL) {
		THROW(RC_INVALID_HANDLE, "Record handle is invalid");
	}

	free(record->data);
	free(record);

	//All OK
	return RC_OK;
}
