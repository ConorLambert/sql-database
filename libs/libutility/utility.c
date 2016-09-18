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
 
#include "utility.h" 


Node *createNode(char *value) {
	Node *node = malloc(sizeof(Node));
	node->value = value;
	node->left = NULL;
	node->right = NULL;
	return node;
}

void pushNode(NodeStack *nodeStack, Node *node) {
	nodeStack->array[nodeStack->top++] = node;	
}

Node *popNode(NodeStack *nodeStack) {
	if(nodeStack->top > 0)
                return nodeStack->array[--nodeStack->top];
        return NULL;
}

Node *rootNode(NodeStack *nodeStack) {
	return nodeStack->array[nodeStack->top - 1];
}

NodeStack *createNodeStack() {
	NodeStack *nodeStack = malloc(sizeof(NodeStack));
	nodeStack->top = 0;
	return nodeStack;	
}


Node *buildExpressionTree(char *expression) {

	NodeStack *operands = createNodeStack();
	
	char *start = expression;
	char *end; // = expression;	
	char *beginning = expression;
	
	while((start - beginning) < strlen(expression)) {
		end = strstr(start, " ");	
		if(!end) {
			end = start;
			while(end[0] != '\0')
				++end;
		}
		
		char *token = malloc((end - start) + 1);
		strlcpy(token, start, (end - start) + 1);
		
		Node *node = createNode(token);

		if(!isLogicalOperator(token) && !isMathOperator(token)) {
			pushNode(operands, node);		
		} else {
			Node *operand1 = popNode(operands);
			Node *operand2 = popNode(operands);
			
			node->right = operand1;
			node->left = operand2;

			// add this to stack
			pushNode(operands, node);
		}		
	
		start = end + 1;
	}
	
	// return the first node in the list (not the top)
    	return rootNode(operands);
}



Stack * createStack() {
        Stack *stack = malloc(sizeof(Stack));
        stack->top = 0;
        return stack;
}


void pushAll(Stack *destination, Stack *src) {
        for(; src->top > 0;)
                pushToOperands(destination, src->array[--src->top], 1);
}


void push(Stack *stack, char *value) {
	stack->array[stack->top++] = value;

}


char *pop(Stack *stack){
        if(stack->top > 0)
                return stack->array[--stack->top];
        return NULL;
}


char *seek(Stack *stack) {
	return stack->array[stack->top - 1];	
}


int flipStack(Stack *stack) {
	int i, j;
	for(i = 0, j = stack->top - 1; i < j; ++i, --j) {
		printf("\ni %d, j %d stack->array[] = %s, %s\n", i, j, stack->array[i], stack->array[j]);
		char *temp = stack->array[i];
		stack->array[i] = stack->array[j];
		stack->array[j] = temp;
	}
		
	return 0;
}


void pushToOperators(Stack *stack, char *value) {
	stack->array[stack->top++] = value;
}

char *toString(Stack *stack) {
	char *conversion = calloc(500, sizeof(char));

	int i;
	for(i = 0; i < stack->top - 1; ++i){ 
		strcat(conversion, stack->array[i]);
		strcat(conversion, " ");
	}

	strcat(conversion, stack->array[i]);

	return conversion;
}


void pushToOperands(Stack *stack, char *value, int size) {
	stack->array[stack->top] = malloc(size + 1); 
	printf("\nafter malloc\n");
	
	if(size == 1) {	// if its an operator or single character operand
		printf("\nafter malloc %c\n", *value);
		stack->array[stack->top][0] = *value;
		stack->array[stack->top++][1] = '\0';		
		printf("\nafter malloc\n");
		
	} else {
		strlcpy(stack->array[stack->top++], value, size + 1);
	}
		
	printf("\nafter strlcpy %s\n", stack->array[stack->top - 1]);
}


void printOperatorStack(Stack *stack) {
	printf("\n");
	int i;
	for(i = 0; i < stack->top; ++i) {
		printf("%c ", *stack->array[i]);
	}
	printf("\n");
}


void printStack(Stack *stack){
        printf("\n");
        int i;
        for(i = 0; i < stack->top; ++i)
                printf("%s ", stack->array[i]);
        printf("\n");
}


bool isOperator(char *src, char *operator) {
        printf("\nchecking %s\n", operator);
        if(strncmp(src, operator, strlen(operator)) == 0) {
                printf("\nreturning true\n");
                return true;
        }

        printf("\nreturning false\n");
        return false;
}


bool isMathOperator(char *token) {
        if(isOperator(token, "=")) {
                return true;
        } else if(isOperator(token, "!=")) {
                return true;
        } else if(isOperator(token, "+")){
                return true;
        } else if(isOperator(token, "-")) {
                return true;
        } else if(isOperator(token, "*")) {
                return true;
        } else if(isOperator(token, "/")) {
                return true;
        } else {
                return false;
        }

}


bool isLogicalOperator(char *token) {
        GREATER_THAN_SYMBOL_STRING[0] = GREATER_THAN_SYMBOL;
        GREATER_THAN_SYMBOL_STRING[1] = '\0';

        LESS_THAN_SYMBOL_STRING[0] = LESS_THAN_SYMBOL;
        LESS_THAN_SYMBOL_STRING[1] = '\0';

        if(isOperator(token, "&")) {
                return true;
        } else if(isOperator(token, "|")) {
                return true;
        } else if(isOperator(token, GREATER_THAN_SYMBOL_STRING)){
                return true;
        } else if(isOperator(token, LESS_THAN_SYMBOL_STRING)) {
                return true;
        } else if(isOperator(token, "!=")) {
                return true;
        } else {
                return false;
        }
}




/*

* Appends src to string dst of size siz (unlike strncat, siz is the

* full size of dst, not space left).  At most siz-1 characters

* will be copied.  Always NUL terminates (unless siz <= strlen(dst)).

* Returns strlen(src) + MIN(siz, strlen(initial dst)).

* If retval >= siz, truncation occurred.

*/

size_t

strlcat(char *dst, const char *src, size_t siz)

{

	char *d = dst;

	const char *s = src;

	size_t n = siz;

	size_t dlen;
 

	/* Find the end of dst and adjust bytes left but don't go past end */

	while (n-- != 0 && *d != '\0')

		d++;

	dlen = d - dst;

	n = siz - dlen;
 

	if (n == 0)

		return(dlen + strlen(s));

	while (*s != '\0') {

		if (n != 1) {

			*d++ = *s;

			n--;

		}

		s++;

	}

	*d = '\0';
 

	return(dlen + (s - src));	/* count does not include NUL */

}


/*
 * Copy src to string dst of size siz.  At most siz-1 characters
 * will be copied.  Always NUL terminates (unless siz == 0).
 * Returns strlen(src); if retval >= siz, truncation occurred.
 */
size_t strlcpy(char *dst, const char *src, size_t siz)
{
	char *d = dst;
	const char *s = src;
	size_t n = siz;

	/* Copy as many bytes as will fit */
	if (n != 0) {
		while (--n != 0) {
			if ((*d++ = *s++) == '\0')
				break;
		}
	}

	/* Not enough room in dst, add NUL and traverse rest of src */
	if (n == 0) {
		if (siz != 0)
			*d = '\0';		/* NUL-terminate dst */
		while (*s++)
			;
	}

	return(s - src - 1);	/* count does not include NUL */
}
