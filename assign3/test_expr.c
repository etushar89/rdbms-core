#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "tables.h"
#include "test_helper.h"

// helper macros
#define OP_TRUE(left, right, op, message)		\
		do {							\
			Value *result;					\
			MAKE_VALUE(result, DT_INT, -1);			\
			op(left, right, result);				\
			bool b = result->v.boolV;				\
			freeVal(result);					\
			ASSERT_TRUE(b,message);				\
		} while (0)

#define OP_FALSE(left, right, op, message)		\
		do {							\
			Value *result;					\
			MAKE_VALUE(result, DT_INT, -1);			\
			op(left, right, result);				\
			bool b = result->v.boolV;				\
			freeVal(result);					\
			ASSERT_TRUE(!b,message);				\
		} while (0)

// test methods
static void testValueSerialize(void);
static void testOperators(void);
static void testExpressions(void);
static void testOPTrue(char* l, char* r, RC (*fptr)(Value *, Value *, Value *),
		char* m);
static void testOPFalse(char* l, char* r, RC (*fptr)(Value *, Value *, Value *),
		char* m);
static void testOPTrueV(Value* lv, Value* rv, char* m);

char *testName;

// main method
int main(void) {
	testName = "";

	testValueSerialize();
	testOperators();
	testExpressions();

	return 0;
}

// ************************************************************ 
void testValueSerialize(void) {
	testName = "test value serialization and deserialization";

	Value *v = stringToValue("i10");
	char *c = serializeValue(v);
	ASSERT_EQUALS_STRING(c, "10", "create Value 10");
	free(c);
	freeVal(v);

	v = stringToValue("f5.3");
	c = serializeValue(v);
	ASSERT_EQUALS_STRING(c, "5.300000", "create Value 5.3");
	free(c);
	freeVal(v);

	v = stringToValue("sHello World");
	c = serializeValue(v);
	ASSERT_EQUALS_STRING(c, "Hello World", "create Value Hello World");
	free(c);
	freeVal(v);

	v = stringToValue("bt");
	c = serializeValue(v);
	ASSERT_EQUALS_STRING(c, "true", "create Value true");
	free(c);
	freeVal(v);

	v = stringToValue("btrue");
	c = serializeValue(v);
	ASSERT_EQUALS_STRING(c, "true", "create Value true");
	free(c);
	freeVal(v);

	TEST_DONE()
	;
}

// ************************************************************ 
void testOperators(void) {
	Value *result;
	testName = "test value comparison and boolean operators";
	MAKE_VALUE(result, DT_INT, 0);

	// equality
	RC (*fptrVE)(Value *, Value *, Value *);
	fptrVE = &valueEquals;

	testOPTrue("i10", "i10", fptrVE, "10 = 10");
	testOPFalse("i9", "i10", fptrVE, "9 != 10");
	testOPTrue("sHello World", "sHello World", fptrVE,
			"Hello World = Hello World");
	testOPFalse("sHello Worl", "sHello World", fptrVE,
			"Hello Worl != Hello World");
	testOPFalse("sHello Worl", "sHello Wor", fptrVE, "Hello Worl != Hello Wor");

	// smaller
	RC (*fptrVS)(Value *, Value *, Value *);
	fptrVS = &valueSmaller;

	testOPTrue("i3", "i10", fptrVS, "3 < 10");
	testOPTrue("f5.0", "f6.5", fptrVS, "5.0 < 6.5");

	// boolean
	RC (*fptrBA)(Value *, Value *, Value *);
	fptrBA = &boolAnd;

	testOPTrue("bt", "bt", fptrBA, "t AND t = t");
	testOPFalse("bt", "bf", fptrBA, "t AND f = f");

	RC (*fptrBO)(Value *, Value *, Value *);
	fptrBO = &boolOr;

	testOPTrue("bt", "bf", fptrBO, "t OR f = t");
	testOPFalse("bf", "bf", fptrBO, "f OR f = f");

	Value *lv = stringToValue("bf");

	TEST_CHECK(boolNot(lv, result));
	ASSERT_TRUE(result->v.boolV, "!f = t");

	freeVal(lv);
	freeVal(result);

	TEST_DONE()
	;
}

void testOPTrue(char* l, char* r, RC (*fptr)(Value *, Value *, Value *),
		char* m) {
	Value *lv = stringToValue(l);
	Value *rv = stringToValue(r);
	OP_TRUE(lv, rv, fptr, m);
	freeVal(lv);
	freeVal(rv);
}

void testOPFalse(char* l, char* r, RC (*fptr)(Value *, Value *, Value *),
		char* m) {
	Value *lv = stringToValue(l);
	Value *rv = stringToValue(r);
	OP_FALSE(lv, rv, fptr, m);
	freeVal(lv);
	freeVal(rv);
}

// ************************************************************
void testExpressions(void) {
	Expr *op, *l, *r;
	Value *res;
	testName = "test complex expressions";

	MAKE_CONS(l, stringToValue("i10"));
	evalExpr(NULL, NULL, l, &res);
	testOPTrueV(stringToValue("i10"), res, "Const 10");

	MAKE_CONS(r, stringToValue("i20"));
	evalExpr(NULL, NULL, r, &res);
	testOPTrueV(stringToValue("i20"), res, "Const 20");

	MAKE_BINOP_EXPR(op, l, r, OP_COMP_SMALLER);
	evalExpr(NULL, NULL, op, &res);
	testOPTrueV(stringToValue("bt"), res, "Const 10 < Const 20");

	MAKE_CONS(l, stringToValue("bt"));
	evalExpr(NULL, NULL, l, &res);
	testOPTrueV(stringToValue("bt"), res, "Const true");

	r = op;
	MAKE_BINOP_EXPR(op, r, l, OP_BOOL_AND);
	evalExpr(NULL, NULL, op, &res);
	testOPTrueV(stringToValue("bt"), res, "(Const 10 < Const 20) AND true");
	freeExpr(op);

	TEST_DONE()
	;
}

void testOPTrueV(Value* lv, Value* rv, char* m) {
	OP_TRUE(lv, rv, valueEquals, m);
	freeVal(lv);
	freeVal(rv);
}
