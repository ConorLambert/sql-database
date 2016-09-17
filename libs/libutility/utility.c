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


Stack * createStack() {
        Stack *stack = malloc(sizeof(Stack));
        stack->top = 0;
        return stack;
}


void pushAll(Stack *destination, Stack *src) {
        for(; src->top > 0;)
                pushToOperands(destination, src->array[--src->top], 1);
}


char *pop(Stack *stack){
        if(stack->top > 0)
                return stack->array[--stack->top];

        return NULL;
}


void pushToOperators(Stack *stack, char *value) {
	stack->array[stack->top++] = value;
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
		strlcpy(stack->array[stack->top++], value, size);
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
