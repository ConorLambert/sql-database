#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "sqlaccess.h"
#include "../../../libs/libutility/utility.h"

#define NUMBER_OF_KEYWORDS 6
#define NUMBER_OF_SYMBOLS 6

char **keywords;

char symbols[] = " ,=!()><+-*/%;";

#define SELECT 0
#define INSERT 1 
#define DELETE 2
#define DROP 3 
#define ALTER 4 
#define UPDATE 5

/*
#define MAX_TARGET_COLUMNS 20
#define MAX_CONDITIONS 5
#define MAX_CONDITION_COLUMNS 20
#define MAX_CONDITION_VALUES MAX_CONDITION_COLUMNS
#define MAX_TABLES 5

#define GREATER_THAN_SYMBOL '$'
#define LESS_THAN_SYMBOL '#'
*/

enum {ASSOC_NONE=0, ASSOC_LEFT, ASSOC_RIGHT};
 


/******************************************************************* - UTILITY FUNCTIONS - ***********************************************************************************/

char * extractPart(char *start_keyword, char *end_keyword, char *query);



/******************************************************************* - SELECT - ***********************************************************************************/

typedef struct OperatorPrecedence {
	int priority;
}OperatorPrecedence;

struct operator_type {
  char *op;
  int prec;
  int assoc;
  int unary;
} operators[]={				// NOTE: change the precedence values to suit program requirements
  {'|', 11, ASSOC_LEFT, 1},
  {'&', 10, ASSOC_LEFT, 1},
  {'=', 9, ASSOC_RIGHT, 1},
  {'!', 9, ASSOC_RIGHT, 1},
  {'^', 1,  ASSOC_RIGHT, 0},
  {'*', 3,  ASSOC_LEFT,  0},
  {'<', 5,  ASSOC_LEFT,  0},
  {'>', 5,  ASSOC_LEFT,  0},
  {LESS_THAN_SYMBOL, 5,  ASSOC_LEFT,  0},
  {GREATER_THAN_SYMBOL, 5,  ASSOC_LEFT,  0},
  {'/', 3,  ASSOC_LEFT,  0},
  {'%', 3,  ASSOC_LEFT,  0},
  {'+', 4,  ASSOC_LEFT,  0},
  {'-', 4,  ASSOC_LEFT,  0},
  {'(', 0,  ASSOC_NONE,  0},
  {')', 0,  ASSOC_NONE,  0}
};


struct operator_type *getop(char *ch);

int findEndOfOperand(char *operand);

void replaceWith(char *src, char *operator, int len);

Stack *buildStack(char *conditions);

Stack * tokenizeConditions(char *conditions);

int tokenizeIdentifiers(char *tokens, char **destination);

int tokenizeKeywordSelect(char *query);




/******************************************************************* - INSERT - ***********************************************************************************/

char *extractTableName(char *query);

int extractColumns(char *query, char **columns);

int extractData(char *query, char **data);

void tokenizeInsertKeyword(char *query);





/******************************************************************* - ALTER - ***********************************************************************************/


char *extractType(char *query, char **type);

char *extractIdentifier(char *query, char **identifier);

int tokenizeAlterAdd(char *table_name, char *query);

int tokenizeAlterDrop(char *table_name, char *query);

int tokenizeAlterRename(char *table_name, char *query);

int tokenizeAlterRenameTable(char *table_name, char *query);

int tokenizeAlterKeyword(char *query);

// TO DO
// MODIFY COLUMNS: Just do basic adding constraints, changing column types can be left for future versions




/****************************************************************** - DELETE - ***********************************************************************************/

char *getTableName(char *query);

int tokenizeDeleteKeyword(char *query);




/****************************************************************** - UPDATE - **********************************************************************************/

int tokenizeUpdateKeyword(char *query);





/******************************************************************** - JOIN - ***********************************************************************************/

int tokenizeJoin(char *query);




/****************************************************************** - CREATE - ***********************************************************************************/

int tokenizeCreateDatabase(char *query);


int tokenizeCreateTable(char *query);




/****************************************************************** - START - ***********************************************************************************/
int tokenizeKeywords(char *query);

void initialize();


