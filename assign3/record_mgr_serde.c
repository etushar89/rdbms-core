/*
 * record_mgr_serde.c
 *
 *  Created on: Nov 1, 2015
 *      Author: heisenberg
 */

#include "dberror.h"
#include "malloc.h"
#include "record_mgr.h"
#include "tables.h"
#include "string.h"

unsigned int getSerSchemaSize(Schema *);

inline void serialize_intBin(int, SerBuffer *);
inline void serialize_uintBin(unsigned int, SerBuffer *);
inline void serialize_shortBin(short, SerBuffer *);
inline void serialize_charBin(char, SerBuffer *);
inline void serialize_stringBuf(char *, unsigned int, SerBuffer *);
inline void serialize_boolBin(bool, SerBuffer *);
inline void serialize_floatBin(float, SerBuffer *);

inline int deserialize_intBin(SerBuffer* in);
inline unsigned int deserialize_uintBin(SerBuffer* in);
inline short deserialize_shortBin(SerBuffer* in);
inline char deserialize_charBin(SerBuffer* in);
inline void deserialize_stringBuf(SerBuffer* in, unsigned int size, char* out);
inline bool deserialize_boolBin(SerBuffer* in);
inline float deserialize_floatBin(SerBuffer* in);

/**
 *	Returns physical size of the serialized schema
 *
 */
inline unsigned int getSerSchemaSize(Schema *schema) {
	unsigned int schemaSerSize = 0;
	schemaSerSize += sizeof(schema->numAttr);
	int i = 0;
	for (; i < schema->numAttr; i++) {
		schemaSerSize += sizeof(schema->dataTypes[i]);
		schemaSerSize += sizeof(schema->typeLength[i]);
		schemaSerSize += sizeof(strlen(schema->attrNames[i]));
		schemaSerSize += strlen(schema->attrNames[i]);
	}
	schemaSerSize += sizeof(schema->keySize);
	i = 0;
	for (; i < schema->keySize; i++) {
		schemaSerSize += sizeof(schema->keyAttrs[i]);
	}
	return schemaSerSize;
}

/**
 * 	Serializes schema structure into binary format.
 * 	This is storage or network transfer friendly.
 */
inline void serializeSchemaBin(Schema *schema, SerBuffer *output) {
	int i = 0;
	serialize_intBin(schema->numAttr, output);
	for (i = 0; i < schema->numAttr; i++) {
		serialize_intBin(schema->dataTypes[i], output);
		serialize_intBin(schema->typeLength[i], output);
		serialize_uintBin(strlen(schema->attrNames[i]), output);
		serialize_stringBuf(schema->attrNames[i], strlen(schema->attrNames[i]),
				output);
	}
	serialize_intBin(schema->keySize, output);
	for (i = 0; i < schema->keySize; i++) {
		serialize_intBin(schema->keyAttrs[i], output);
	}
}

/**
 * Deserializes schema structure from binary data.
 */
inline void deserializeSchemaBin(SerBuffer *input, Schema **schema) {
	input->next = 0;
	*schema = malloc(sizeof(Schema));
	(*schema)->numAttr = deserialize_intBin(input);
	(*schema)->dataTypes = malloc(sizeof(DataType) * (*schema)->numAttr);
	(*schema)->typeLength = (int*) malloc(sizeof(int) * (*schema)->numAttr);
	(*schema)->attrNames = malloc(sizeof(char*) * (*schema)->numAttr);
	int i = 0;
	for (i = 0; i < (*schema)->numAttr; i++) {
		(*schema)->dataTypes[i] = deserialize_intBin(input);
		(*schema)->typeLength[i] = deserialize_intBin(input);
		unsigned int attrNameSize = deserialize_uintBin(input);
		(*schema)->attrNames[i] = malloc(attrNameSize + 2);
		deserialize_stringBuf(input, attrNameSize, (*schema)->attrNames[i]);
		((*schema)->attrNames[i])[attrNameSize + 1] = '\0';
	}
	(*schema)->keySize = deserialize_intBin(input);
	(*schema)->keyAttrs = malloc(sizeof(int) * (*schema)->keySize);
	for (i = 0; i < (*schema)->keySize; i++) {
		(*schema)->keyAttrs[i] = deserialize_intBin(input);
	}
}

/**
 *	Returns size of the record for it's serialized presentation.
 */
inline unsigned int getSerPhysRecordSize(Schema *schema) {
	return 2 * sizeof(int) + sizeof(short) + getRecordSize(schema);
}

/**
 *	Serializes record into binary format.
 *	This is storage or network transfer friendly.
 */
inline void serializeRecordBin(Record *record, unsigned int valRecordLen,
		SerBuffer *output) {
	serialize_intBin(record->id.page, output);
	serialize_intBin(record->id.slot, output);
	serialize_shortBin(record->nullMap, output);
	serialize_stringBuf(record->data, valRecordLen, output);
}

/**
 *	Deserializes record from binary buffer.
 *
 */
inline void deserializeRecordBin(SerBuffer *input, unsigned int valRecordLen,
		Record **record) {
	input->next = 0;
	(*record)->id.page = deserialize_intBin(input);
	(*record)->id.slot = deserialize_intBin(input);
	(*record)->nullMap = deserialize_shortBin(input);
	deserialize_stringBuf(input, valRecordLen, (*record)->data);
}

//////////////// Serializers for basic data types ////////////////
inline void serialize_intBin(int i, SerBuffer *output) {
	memcpy(output->data + output->next, (void *) &i, sizeof(i));
	output->next += sizeof(i);
	output->size += sizeof(i);
}

inline void serialize_uintBin(unsigned int i, SerBuffer *output) {
	memcpy(output->data + output->next, (void *) &i, sizeof(i));
	output->next += sizeof(i);
	output->size += sizeof(i);
}

inline void serialize_shortBin(short s, SerBuffer *output) {
	memcpy(output->data + output->next, (void *) &s, sizeof(s));
	output->next += sizeof(s);
	output->size += sizeof(s);
}

inline void serialize_charBin(char c, SerBuffer *output) {
	memcpy(output->data + output->next, (void *) &c, sizeof(c));
	output->next += sizeof(c);
	output->size += sizeof(c);
}

inline void serialize_stringBuf(char* str, unsigned int size, SerBuffer *output) {
	memcpy(output->data + output->next, (void *) str, size);
	output->next += size;
	output->size += size;
}

inline void serialize_boolBin(bool b, SerBuffer *output) {
	memcpy(output->data + output->next, (void *) &b, sizeof(bool));
	output->next += sizeof(b);
	output->size += sizeof(b);
}

inline void serialize_floatBin(float f, SerBuffer *output) {
	memcpy(output->data + output->next, (void *) &f, sizeof(float));
	output->next += sizeof(f);
	output->size += sizeof(f);
}

//////////////// De-serializers for basic data types ////////////////
inline int deserialize_intBin(SerBuffer* in) {
	int *data = (int*) (in->data + in->next);
	in->next += sizeof(int);
	return *data;
}

inline unsigned int deserialize_uintBin(SerBuffer* in) {
	unsigned int *data = (unsigned int*) (in->data + in->next);
	in->next += sizeof(unsigned int);
	return *data;
}

inline short deserialize_shortBin(SerBuffer* in) {
	short *data = (short*) (in->data + in->next);
	in->next += sizeof(short);
	return *data;
}

inline char deserialize_charBin(SerBuffer* in) {
	char *data = (char*) (in->data + in->next);
	in->next += sizeof(char);
	return *data;
}

inline void deserialize_stringBuf(SerBuffer* in, unsigned int size, char* out) {
	memcpy(out, in->data + in->next, size);
	in->next += size;
}

inline bool deserialize_boolBin(SerBuffer* in) {
	bool *data = (bool*) (in->data + in->next);
	in->next += sizeof(bool);
	return *data;
}

inline float deserialize_floatBin(SerBuffer* in) {
	float *data = (float*) (in->data + in->next);
	in->next += sizeof(float);
	return *data;
}
