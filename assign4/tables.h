#ifndef TABLES_H
#define TABLES_H

#include "dt.h"
#include "buffer_mgr.h"

// Data Types, Records, and Schemas
typedef enum DataType {
	DT_INT = 0, DT_STRING = 1, DT_FLOAT = 2, DT_BOOL = 3
} DataType;

typedef struct Value {
	DataType dt;
	union v {
		int intV;
		char *stringV;
		float floatV;
		bool boolV;
	} v;
} Value;

typedef struct RID {
	int page;
	int slot;
} RID;

typedef struct Record {
	RID id;
	short nullMap;
	char *data;
} Record;

// information of a table schema: its attributes, datatypes, 
typedef struct Schema {
	int numAttr;
	char **attrNames;
	DataType *dataTypes;
	int *typeLength;
	int *keyAttrs;
	int keySize;
	unsigned int *attrOffsets;
} Schema;

// TableData: Management Structure for a Record Manager to handle one relation
typedef struct RM_TableData {
	char *name;
	Schema *schema;
	void *mgmtData;
} RM_TableData;

typedef struct RM_TableMgmtData {
	BM_BufferPool *bPool;
	unsigned int tupleCount;
	unsigned int pageCount;
	unsigned int recordSize;
	unsigned int availBytesLastPage;
	unsigned int tblNameSize;
	RID firstFreeSlot;
	unsigned int serSchemaSize;
	unsigned int physicalRecordSize;
	unsigned int slotCapacityPage;
	char* serSchema;
} RM_TableMgmtData;

typedef struct SerBuffer {
	char *data;
	int next;
	size_t size;
} SerBuffer;

#define MAKE_STRING_VALUE(result, value)				\
		do {									\
			(result) = (Value *) malloc(sizeof(Value));				\
			(result)->dt = DT_STRING;						\
			(result)->v.stringV = (char *) malloc(strlen(value) + 1);		\
			strcpy((result)->v.stringV, value);					\
		} while(0)

#define MAKE_VALUE(result, datatype, value)				\
		do {									\
			(result) = (Value *) malloc(sizeof(Value));				\
			(result)->dt = datatype;						\
			switch(datatype)							\
			{									\
			case DT_INT:							\
			(result)->v.intV = value;					\
			break;								\
			case DT_FLOAT:							\
			(result)->v.floatV = value;					\
			break;								\
			case DT_BOOL:							\
			(result)->v.boolV = value;					\
			break;								\
			}									\
		} while(0)

// debug and read methods
extern Value *stringToValue(char *value);
extern char *serializeTableInfo(RM_TableData *rel);
extern char *serializeTableContent(RM_TableData *rel);
extern char *serializeSchema(Schema *schema);
extern char *serializeRecord(Record *record, Schema *schema);
extern char *serializeAttr(Record *record, Schema *schema, int attrNum);
extern char *serializeValue(Value *val);

extern inline unsigned int getSerSchemaSize(Schema *);
extern inline unsigned int getSerPhysRecordSize(Schema *);
extern inline void serializeSchemaBin(Schema *, SerBuffer *);
extern inline void deserializeSchemaBin(SerBuffer *, Schema **);
extern inline unsigned int getSerPhysRecordSize(Schema *);
extern inline void serializeRecordBin(Record *, unsigned int, SerBuffer *);
extern inline void deserializeRecordBin(SerBuffer *, unsigned int, Record **);

#endif
