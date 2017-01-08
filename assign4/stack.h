/*
 * stack.h
 *
 *  Created on: Dec 1, 2015
 *      Author: tushar
 */

#ifndef STACK_H_
#define STACK_H_

#include "dberror.h"
#include "btree_mgr.h"

#define MAX 100

int top;
int stack[MAX];

void initializeStack() {
	top = -1;
}

void push(int data) {
	stack[++top] = data;
}

int pop() {
	int data = stack[top];
	top--;

	return data;
}

bool IsStackEmpty() {
	if(top==-1)
		return true;

	return false;
}

#endif /* STACK_H_ */
