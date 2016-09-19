/*

* Copyright (c) 1998 Todd C. Miller <Todd.Miller@courtesan.com>

*

* Permission to use, copy, modify, and distribute this software for any

* purpose with or without fee is hereby granted, provided that the above

* copyright notice and this permission notice appear in all copies.

*

* THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES

* WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF

* MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR

* ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES

* WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN

* ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF

* OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

*/
 

#include <sys/types.h>

#include <string.h>

#ifndef UTILITY_H
#define UTILITY_H

#define MAX_NUMBER_OF_ELEMENTS 40
#define MAX_NUMBER_OF_NODES 20

#define VARCHAR "VARCHAR"
#define VARCHAR_SIZE 255
#define VARCHAR_VARIED "VARCHAR("
#define CHAR "CHAR"
#define CHAR_SIZE sizeof(char)	
#define CHAR_VARIED "CHAR("
#define INT "INT"
#define INT_SIZE sizeof(int)
#define DOUBLE "DOUBLE"
#define DOUBLE_SIZE sizeof(double)

typedef int CASE;
#define UPPER 0 
#define LOWER 1

#define GREATER_THAN_SYMBOL '$'
#define LESS_THAN_SYMBOL '#'

char GREATER_THAN_SYMBOL_STRING[2];
char LESS_THAN_SYMBOL_STRING[2];

typedef struct Node Node;

typedef int bool;
#define true 1
#define false 0
 
typedef struct Stack {
        int top;
        char *array[MAX_NUMBER_OF_ELEMENTS];
}Stack;

Stack * createStack();

typedef struct Node {
	char *value;
	Node *left;
	Node *right;
} Node;

Node *createNode();

typedef struct NodeStack {
        int top;
        Node *array[MAX_NUMBER_OF_NODES];
}NodeStack;

NodeStack *createNodeStack();

Node * buildExpressionTree(char *expression);

void pushAll(Stack *destination, Stack *src);

void pushToOperators(Stack *stack, char *value);

void pushToOperands(Stack *stack, char *value, int size);

void push(Stack *stack, char *value);

int flipStack(Stack *stack);

extern char *toString(Stack *stack);

void printOperatorStack(Stack *stack);

char *pop(Stack *stack);

char *seek(Stack *stack);

void printStack(Stack *stack);

bool isOperator(char *src, char *operator);

bool isMathOperator(char *token);

bool isLogicalOperator(char *token);

bool isBinaryOperator(char *token);

int convertToCase(char *string, CASE char_case);


/*

* Appends src to string dst of size siz (unlike strncat, siz is the

* full size of dst, not space left).  At most siz-1 characters

* will be copied.  Always NUL terminates (unless siz <= strlen(dst)).

* Returns strlen(src) + MIN(siz, strlen(initial dst)).

* If retval >= siz, truncation occurred.

*/

size_t strlcat(char *dst, const char *src, size_t siz);

size_t strlcpy(char *dst, const char *src, size_t siz);

#endif
