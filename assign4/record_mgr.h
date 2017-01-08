#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"

// Bookkeeping for scans
typedef struct RM_ScanHandle {
	RM_TableData *rel;
	void *mgmtData;
} RM_ScanHandle;

typedef struct RM_ScanIterator {
	int totalRecords;
	int lastRecordRead;
	Record** records;
} RM_ScanIterator;

// table and manager
extern RC initRecordManager(void *mgmtData);
extern RC shutdownRecordManager();
extern RC createTable(char *name, Schema *schema);
extern RC openTable(RM_TableData *rel, char *name);
extern RC closeTable(RM_TableData *rel);
extern RC deleteTable(char *name);
extern int getNumTuples(RM_TableData *rel);

extern RC deleteIndex(char *name);

// handling records in a table
extern RC insertRecord(RM_TableData *rel, Record *record);
extern RC deleteRecord(RM_TableData *rel, RID id);
extern RC updateRecord(RM_TableData *rel, Record *record);
extern RC updateScan(RM_TableData *rel, Expr *cond,
		void (*op)(Schema*, Record *));
extern RC getRecord(RM_TableData *rel, RID id, Record *record);

// scans
extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next(RM_ScanHandle *scan, Record *record);
extern RC closeScan(RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize(Schema *schema);
extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
		int *typeLength, int keySize, int *keys);
extern RC freeSchema(Schema *schema);

// dealing with records and attribute values
extern RC createRecord(Record **record, Schema *schema);
extern RC freeRecord(Record *record);
extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value);

extern bool isNULLAttr(RM_TableData *rel, Record *record, int attrNum);

#endif // RECORD_MGR_H
