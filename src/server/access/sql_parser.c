#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "sql_parser.h"

/*
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
*/



struct operator_type *getop(char *ch) {
	printf("\ngetop %c\n", ch);
  	int i;
  	for(i=0; i<sizeof operators/sizeof operators[0]; ++i)
		if(operators[i].op==ch) return operators+i;	  	
  
	printf("\nreturning NULL\n");
	return NULL;
}

int findEndOfOperand(char *operand) {
	int i, j;
	for(i = 1; i < strlen(operand); ++i) {	// start at 1 because 0 is the token
		//printf("\nchecking is AND operator %s or this %s\n", operand, operand + i);
		if(isOperator(operand + i, "AND") || isOperator(operand + i, "OR")) 
			return i;

		for(j = 0; j < strlen(symbols); ++j) {
			//printf("\nComparing symbols i:%d. j:%d\n", i, j);
			if(operand[i] == symbols[j])
				return i;
		}			
	}

	// an error has occured - statement not entered properly
	return i;
}


void replaceWith(char *src, char *operator, int len) {
	printf("\nin replace with\n");
	src[0] = *operator;
	int i;
	for(i = 1; i < len; ++i) 
		src[i] = ' ';
}


Stack *buildStack(char *conditions) {

	printf("\nConditions %s %d\n", conditions, strlen(conditions));
	
	Stack *operators = createStack();
	Stack *operands = createStack();

	char *token;
	struct operator_type *op1 = NULL;
	struct operator_type *op2 = NULL;

	bool inLeft = false;

	int i; //strlen(conditions);
	for(i = 0, token = conditions; i < strlen(conditions); ++i, token = conditions + i) {
		printf("\n\n\n%d", i);
		printf("\nOperands: \n");
		printStack(operands);
		printf("\nOperators: \n");
		printOperatorStack(operators);

		// TO DO: Convert to lowercase

		if(isspace(*token)) 
			continue;

		printf("\n\n");				
		if(isOperator(token, "AND ")) { 
			replaceWith(token, "&", strlen("AND"));
			printf("\n\t\tITS AN &\n");
		} else if(isOperator(token, "OR ")) {
			replaceWith(token, "|", strlen("OR"));
		} else if(isOperator(token, ">=")){
			printf("\n\t\ttoken = %s", token);
			token[0] = GREATER_THAN_SYMBOL;
			token[1] = ' ';	
		} else if(isOperator(token, "<=")) {
			token[0] = LESS_THAN_SYMBOL;
			token[1] = ' ';
		} else if(isOperator(token, "!=")) {
			replaceWith(token, "!", strlen("!="));
		}		 
		
		if(op1 = getop(*token)){			
			// if there are no operators
			printf("\nIts an operator");

			if(*token == '(') {
				inLeft = true;
				pushToOperators(operators, token);
				continue;
			} else if(*token == ')')
				inLeft = false;
		

			if(inLeft) {
				// get the operator currently on top of the stack
				struct operator_type *op2 = getop(*(operators->array[operators->top - 1]));	

				// see if the most recent operator on top of the stack is not a left paren and has a greater precedence	
				if(op2->op != '(' && op1->prec > op2->prec) {
					char *temp = pop(operators);
					pushToOperators(operators, token);
					pushToOperators(operators, temp);
				} else {
					pushToOperators(operators, token);
				}

				continue;
			}
		
			if(operators->top == 0 || *token == '(') {					
				printf("\nINLEFTpushing to operators\n");
				
				pushToOperators(operators, token);
			
				continue;
			}
			
			// get the operator currently on top of the stack
			struct operator_type *op2 = getop(*(operators->array[operators->top - 1]));	

			printf("\nchecking ...\n");	
			// if the operator is a right parantheses

			if(op1->op == ')') {
				printf("\npushing to operands\n");
				// pop the operators off the stack until the left parantheses is reached (remove the left parantheses symbol)
				char *symbol = NULL;
	
				while(*(symbol = pop(operators)) != '(') {
					printf("\npushing symbol %c to operands\n", *symbol);
					pushToOperands(operands, symbol, 1);
				}
			// else if the token operator has a higher precedence than the top value on the operators
			} else if(op1->assoc == ASSOC_RIGHT) {    
				while(op1->prec > op2->prec) {	// while op1 is a lower precedence
					printf("\npushing operator %c to operands\n", op2->op);
					pushToOperands(operands, (pop(operators)), 1);
					if(operators->top == 0)
						break;
					printf("\nHERE\n");
					op2 = getop(*(operators->array[operators->top - 1]));
				}		

				printf("\nASSOC_RIGHT: pushing to operators\n");
				pushToOperators(operators, token);
			} else if(op1->assoc == ASSOC_LEFT){
				printf("\nASSOC_LEFT\n");			
				while(op1->prec >= op2->prec) {	// while op2 is a lower or equal precedence
					printf("\npushing operator %c to operands\n", op2->op);
					pushToOperands(operands, pop(operators), 1);
					if(operators->top == 0)
						break;
					printf("\nHERE\n");
					op2 = getop(*(operators->array[operators->top - 1]));
				}
						
				printf("\nASSOC_LEFTpushing to operators\n");
				pushToOperators(operators, token);
			}
		} else { // else its an operand
			printf("\nIts an operand\n");
			printf("\npushing to operands %s\n", token);
			int k = findEndOfOperand(token); 
			printf("\nk is %d\n", k);
			pushToOperands(operands, token, k);
			i += k - 1;
			printf("\ni is %d\n", i);	

			// each iteration increments by 1 but i is currently at the next token so we push by 1 before the iteration begins
		}
	
		printf("\ni is %d\n", i);				
       	}

	printf("\nPushAll Here %s\n", token);
	// push the rest of the operators onto the stack
	//printStack(operands);
	pushAll(operands, operators);
		
	//free(operators);

	return operands;
}



Stack * tokenizeConditions(char *conditions) {
	printf("\nWHERE CLAUSE %s\n", conditions);

	// create two stacks for first phase
	Stack *result = buildStack(conditions);
	printStack(result);
		
	// build the expression tree from this stack
	//return buildExpressionTree(result);
	return result;
}


int tokenizeIdentifiers(char *tokens, char **destination) {
	
	int i = 0;
	destination[i] = strtok(tokens, symbols);
	while(destination[i] != NULL) {
      		printf("\n%s", destination[i]);
       		destination[++i] = strtok(NULL, symbols);
   	}

	return i;
}


char * extractPart(char *start_keyword, char *end_keyword, char *query) {	
	char *start = strstr(query, start_keyword) + strlen(start_keyword);			
	char *end = strstr(start, end_keyword);
	char *result =  malloc((end - start) + 1);
	strlcpy(result, start, (end - start) + 1);
	printf("\nresult = %s\n", result);
	return result;
}


int tokenizeKeywordSelect(char *query) {

	char *target_columns[MAX_TARGET_COLUMNS];
	char *target_column_tokens;
	char *tables[MAX_TABLES];
	char *table_tokens;
	char *condition_tokens;
	Stack *conditions;

	printf("\nSQL Query: %s\n", query);	

	target_column_tokens = extractPart("SELECT", "FROM", query);
	if(strstr(query, "WHERE")) {
		table_tokens = extractPart("FROM", "WHERE", query);
		condition_tokens =  extractPart("WHERE", ";", query);
		conditions = tokenizeConditions(condition_tokens);
	} else {
		table_tokens = extractPart("FROM", ";", query);
	}

	// further tokenize based on symbols	
	// we store each idenitifer into its own array position and record the number of identifiers per array
	// we can then forward this information onto the select function in order to execute
	int number_of_target_columns = tokenizeIdentifiers(target_column_tokens, target_columns);
	printf("\nnumber_of_target_columns %d\n", number_of_target_columns);
	
	int number_of_tables = tokenizeIdentifiers(table_tokens, tables);
	printf("\nnumber_of_tables %d\n", number_of_tables);

	/*
	flipStack(result);
	char *where_clause = convertToString(result);
	*/

	// execute query
	// char **result_set = selectRecord(tables[0], number_of_tables, target_columns, number_of_target_columns, where_clause);

	return 0;
}




/******************************************************************* - INSERT - ***********************************************************************************/

/*
	INSERT INTO table_name
	VALUES (value1,value2,value3,...);

	INSERT INTO table_name (column1,column2,column3,...)
	VALUES (value1,value2,value3,...);
*/

char *extractTableName(char *query) {
	// find table name
	char *marker = strstr(query, "INTO") + strlen("INTO");
	while(marker[0] == ' ') 
		++marker;
	
	char *first_bracket = strstr(marker, "(");
	
	char *end;
	if (strstr(marker, " ") < first_bracket)
		end = strstr(marker, " ");
	else
		end = first_bracket;

	char *table_name = malloc((end - marker) + 1);
	strlcpy(table_name, marker, (end - marker) + 1);

	return table_name;
}


int extractColumns(char *query, char **columns) {
	int i = 0;
	char *marker = query;
	char *end = query;

	printf("\nin extract columns\n");
	while(*end != ')') {
		while(marker[0] == ' ' || marker[0] == ',') 
			++marker;
	
		if(*marker == ')')
                        break;

		end = marker;
		while(end[0] != ' ' && end[0] != ',' && end[0] != ')')
			++end;	
		columns[i] = malloc((end - marker) + 1);
		strlcpy(columns[i++], marker, (end - marker) + 1);	
		marker = end + 1;
	}		

	
	return i;
}


int extractData(char *query, char **data){
	int i = 0;
	char *marker = query;
	char *end = query;

	printf("\nin extract data\n");
	while(*end != ')') {
		while(marker[0] == ' ' || marker[0] == ',') 
			++marker;
	
		if(*marker == ')')
                        break;

		end = marker;
		while(end[0] != ' ' && end[0] != ',' && end[0] != ')')
			++end;	
			
		data[i] = malloc((end - marker) + 1);
		strlcpy(data[i++], marker, (end - marker) + 1);	
		marker = end + 1;
	}		

	printf("\nretuning data\n");	
	return i;
}


void tokenizeInsertKeyword(char *query) {

	printf("start");
	char *table_name = extractTableName(query);	
	printf("\nTable Name: %s\n", table_name);

	char *columns[20];
	char *first_bracket = strstr(query, "(");		
	char *values = strstr(query, "VALUES");
	int number_of_columns = 0;
	printf("\nchecking columns\n");
	if(first_bracket < values) {
		number_of_columns = extractColumns(first_bracket + 1, columns);
		int j;
		for(j = 0; j < number_of_columns; ++j)
			printf("\ncolumn %d: %s\n", j, columns[j]);
		first_bracket = strstr(values, "(");		
	}
	
	char *data[20];	
	int number_of_data = extractData(first_bracket + 1, data);

	int j;
	for(j = 0; j < number_of_data; ++j)
		printf("\ndata %d: %s\n", j, data[j]);

	return insert(table_name, columns, number_of_columns, data, number_of_data);
}






/******************************************************************* - ALTER - ***********************************************************************************/


char *extractType(char *query, char **type) {
	char *start = query;
	while(start[0] == ' ')
		++start;

	char *end = start;	
	if(strncmp(start, "char(", strlen("char(")) == 0 || strncmp(start, "varchar(", strlen("varchar(")) == 0) {
		end = strstr(start, ")") + 1;
	} else {
		printf("\n\tstart %s\n", start);
		while(end[0] != '(' && end[0] != ' ' && end[0] != ')' && end[0] != ',' && end[0] != ';')
			end++;
	}

	*type = malloc((end - start) + 1) ;
	strlcpy(*type, start, (end - start) + 1);
	printf("\nType: %s\n", *type);

	return end;
}


char *extractIdentifier(char *query, char **identifier){
	char *start = query;	
	while(start[0] == ' ' || start[0] == '(' || start[0] == ',')
		++start;
		
	char *end = strstr(start, " ");
	*identifier = malloc((end - start) + 1);
	strlcpy(*identifier, start, (end - start) + 1);
	printf("\nIdentifier: %s\n", *identifier);

	return end;
}


/*
	ALTER TABLE table_name
		ADD column_name column-definition;

	ALTER TABLE supplier
		ADD supplier_name char(50);

	ALTER TABLE table_name
	ADD   (column_1 column-definition,
       	       column_2 column-definition,
       	       column_n column_definition);
*/

int tokenizeAlterAdd(char *table_name, char *query) {

	char *identifiers[20];
	char *types[20];

	char *marker = query;

	printf("\nmarker %s\n", marker);
	
	int i = 0;
	while(*marker != ')' && *marker != ';') {
		marker = extractIdentifier(marker + 1, &identifiers[i]);	
		printf("\n\tidenitifiers = %s, marker = %s\n", identifiers[i], marker);
		marker = extractType(marker + 1, &types[i]);
		printf("\n\ttypes = %s, marker = %s\n", types[i], marker);
		++i;

		// remove any whitespace
		while(marker[0] == ' ' )
			++marker;
	}
	
	printf("\noutside loop\n");

	int j;
	for(j = 0; j < i; ++j) 
		printf("\nidentifiers[%d] = %s, types[%d] = %s\n", j, identifiers[j], j, types[j]);


	return alterTableAddColumns(table_name, identifiers, types, i);
}


/*
	-ALTER TABLE table_name
	  DROP COLUMN column_name;
*/
int tokenizeAlterDropColumn(char *table_name, char *query){
	
	char *columns[20];	
	char *start;

	printf("\nIn drop column\n");
	// is there multiple columns
	char *start_bracket = strstr(query, "(");
	char *end_statement;
	if(start_bracket) {
		start = start_bracket;
		end_statement = strstr(start, ")");
	} else {
		start = query;
		end_statement = strstr(start, ";");
	}

	char *end = start;
	int i = 0;
	while(end[0] != ';' && end[0] != ')') {
	
		while(start[0] == ' ' || start[0] == '(')
			++start;

		end = start;
		while(end[0] != ' ' && end[0] != ';' && end[0] != ')' && end[0] != ',')
			++end;

		columns[i] = malloc((end - start) + 1);
		strlcpy(columns[i], start, (end - start) + 1);
	
		while(end[0] == ' ')
			++end;

		start = end + 1;
		++i;
	}

	int j;
	for(j = 0; j < i; ++j)
		printf("\ncolumns[%d] = %s\n", j, columns[j]);

	// execute query
	return alterTableDropColumns(table_name, columns, i);
}


int tokenizeAlterRenameColumn(char *table_name, char *query) {
	char *start = query;

	while(start[0] == ' ')
		++start;

	char *end = strstr(start, " ");

	char *current_name = malloc((end - start) + 1);
	strlcpy(current_name, start, (end - start) + 1);

	start = strstr(end, "TO") + strlen("TO");
	while(start[0] == ' ')
               ++start;

	end = start + 1;
	while(end[0] != ' ' && end[0] != ';')
		++end;

	char *new_name = malloc((end - start) + 1);
	strlcpy(new_name, start, (end - start) + 1);

	printf("\nold_name = %s, new_name = %s\n", current_name, new_name);

	// execute query
	return alterTableRenameColumn(table_name, current_name, new_name);
	
}


int tokenizeAlterRenameTable(char *table_name, char *query) {
	char *start = query;

	while(start[0] == ' ')
		++start;

	char *end = start + 1;
	while(end[0] != ' ' && end[0] != ';')
		++end;

	char *new_name = malloc((end - start) + 1);
	strlcpy(new_name, start, (end - start) + 1);

	printf("\ncurrent_name = %s, new_name = %s\n", table_name, new_name);

	// execute query
	//return alterTableRenameTable(table_name, new_name);

}


int tokenizeAlterKeyword(char *query) {

	// get table_name
	char *start = strstr(query, "TABLE") + strlen("TABLE");
	
	while(start[0] == ' ')	
		++start;
	
	char *end = start + 1;
	while(end[0] != ' ')	
		++end;

	char *table_name = malloc(end - start);
	strlcpy(table_name, start, (end - start) + 1);

	printf("\nTable_Name %s\n", table_name);

	char *marker;
	// find which type of alter the query is
	if(marker = strstr(end, "ADD"))	{	
		printf("\nIts Add\n");	
		tokenizeAlterAdd(table_name, marker + strlen("ADD"));
	} else if(marker = strstr(end, "DROP COLUMN")) {
		tokenizeAlterDropColumn(table_name, marker + strlen("DROP COLUMN"));
	} else if(marker = strstr(end, "MODIFY")) {
		//tokenizeAlterModify(table_name, marker + strlen("MODIFY"));
	} else if (marker = strstr(end, "CHANGE COLUMN")){
		tokenizeAlterRenameColumn(table_name, marker + strlen("CHANGE COLUMN"));
	} else if (marker = strstr(end, "RENAME TO")){
		tokenizeAlterRenameTable(table_name, marker + strlen("RENAME TO"));
	} 
		else
		return -1;


}

// TO DO
// MODIFY COLUMNS: Just do basic adding constraints, changing column types can be left for future versions




/****************************************************************** - DELETE - ***********************************************************************************/

char *getTableName(char *query) {
	char *end = query;
	while(end[0] != ' ' && end[0] != ';')
		++end;

	char *table_name = malloc((end - query) + 1);
	strlcpy(table_name, query, (end - query) + 1);

	return table_name;
}


int tokenizeDeleteKeyword(char *query){
	

	Stack *result = NULL;
	char *start = strstr(query, "DELETE") + strlen("DELETE");

	while(start[0] == ' ')
		++start;

	char *table_name = NULL;
	if(start[0] == '*') {
		start = strstr(start, "FROM") + strlen("FROM");
		while(start[0] == ' ')
	                 ++start;

		table_name = getTableName(start);
		start += strlen(table_name);
	} else {
		start = strstr(start, " ");
		while(start[0] == ' ')
	                 ++start;
		table_name = getTableName(start); //strstr(query, "FROM") + strlen("FROM");
		start += strlen(table_name);
	
		start = strstr(query, "WHERE");
		if(start) {
			char *condition_tokens =  extractPart("WHERE", ";", start);
			result = buildStack(condition_tokens);
			printStack(result);		
		}
	}
	
	printf("\nTable Name: %s\n", table_name);	

	char *where_clause = toString(result);

	// execute query
	deleteRecord1(table_name, where_clause);

	return 0;
}






/****************************************************************** - UPDATE - **********************************************************************************/

int tokenizeUpdateKeyword(char *query){
	char *start = strstr(query, "UPDATE") + strlen("UPDATE");

	while(start[0] == ' ')
		++start;

	char *end = strstr(start, " ");

	char *table_name = malloc((end - start) + 1);
	strlcpy(table_name, start, (end - start) + 1);

	
	start = strstr(end, "SET") + strlen("SET");

	while(start[0] == ' ')
		++start;

	char *set_columns[20];
	char *set_values[20];
	int i = 0;

	// UPDATE update_table_name SET column1=value1,column2=value2 WHERE some_column=some_value;	
	while(strncmp(start, "WHERE ", strlen("WHERE ")) != 0) { // space after WHERE becuase there could be an identifier with WHERE as its start
		end = start + 1;
	
		while(end[0] != ' ' && end[0] != '=')
			++end;

		set_columns[i] = malloc((end - start) + 1);
		strlcpy(set_columns[i], start, (end - start) + 1);
		
		start = end + 1;
		while(start[0] == ' ' || start[0] == '=')
		       ++start;

		end = start + 1;
		while(end[0] != ' ' && end[0] != ',')
			++end;

		// get column data
		set_values[i] = malloc((end - start) + 1);
		strlcpy(set_values[i], start, (end - start) + 1);

		start = end + 1;
		while(start[0] == ' ' || start[0] == ',')
			++start;

		++i;
	}
	
	char *condition_tokens =  extractPart("WHERE", ";", start);
	Stack *result = buildStack(condition_tokens);
	printStack(result);		

	printf("\ntable_name: %s\n", table_name);

	int j;
	for(j = 0; j < i; ++j)
		printf("\nset_columns[%d] = %s - set_values[%d] = %s\n", j, set_columns[j], j, set_values[j]);

	/*
	flipStack(result);
	char *where_clause = convertToString(result);
	*/

	// execute query
	//update(table_name, set_columns, set_values, i, where_clause);
}






/******************************************************************** - JOIN - ***********************************************************************************/

// starts from strstr("INNER JOIN") + strlen("INNER JOIN")
/* Example:
		SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
		FROM Orders
		INNER JOIN Customers
		ON Orders.CustomerID=Customers.CustomerID;
*/

int tokenizeJoin(char *query) {
	char *start = strstr(query, "SELECT") + strlen("SELECT");	
	while(start[0] == ' ')
		++start;

	char *tables[10];
	char *display_fields[20];
	int i = 0;
	
	char *end;

	while(strncmp(start, "FROM ", strlen("FROM ")) != 0) {

		end = start + 1;
		while(end[0] != ' ' && end[0] != ',')
			++end;
					
		display_fields[i] = malloc((end - start) + 1);
		strlcpy(display_fields[i++], start, (end - start) + 1);		
		
		printf("\n\t%s\n", display_fields[i - 1]);

		start = end + 1;
		while(start[0] == ' ')
			++start;		
	}

	// get main table name
	start = strstr(start, " ");
	while(start[0] == ' ')
		++start;

	end = strstr(start, " ");
	char *table_name = malloc((end - start) + 1);
	strlcpy(table_name, start, (end - start) + 1);
	printf("\ntable_name%s\n", table_name);

	start = end + 1;
	while(start[0] == ' ')
		++start;

	char *join_type = NULL;
	if(strncmp(start, "INNER JOIN", strlen("INNER JOIN")) == 0 || strncmp(start, "JOIN", strlen("JOIN")) == 0)
		join_type = "INNER JOIN";
	else if(strncmp(start, "OUTER JOIN", strlen("OUTER JOIN")) == 0)
		join_type = "OUTER JOIN";
	else if(strncmp(start, "FULL JOIN", strlen("FULL JOIN")) == 0)
		join_type = "FULL JOIN";
	else if(strncmp(start, "LEFT JOIN", strlen("LEFT JOIN")) == 0)
		join_type = "LEFT JOIN";
	else if(strncmp(start, "RIGHT JOIN", strlen("RIGHT JOIN")) == 0)
                join_type = "RIGHT JOIN";
	else
		return -1;

	start = strstr(start, "JOIN") + strlen("JOIN");
	
	// get join table name
	//start = strstr(end, join_type) + strlen(join_type);

	while(start[0] == ' ')
		++start;

	end = strstr(start, " ");
	
	char *join_table = malloc((end - start) + 1);
	strlcpy(join_table, start, (end - start) + 1);
	printf("\n%s %s ON..\n", join_type, join_table);

	// get main table columns and join table columns
	start = strstr(end, "ON");
	
	char *on_conditions = NULL;
	Stack *on_result = NULL;
	char *where_conditions = NULL;
	Stack *where_result = NULL;

	bool order_by = false;
	
	char *order_bys[10];

	if(end = strstr(start, "WHERE")){
		on_conditions = extractPart("ON", "WHERE", start);
		if(end = strstr(start, "ORDER BY")) {
			where_conditions = extractPart("WHERE", "ORDER BY", start + strlen(on_conditions));
			order_by = true;
			// getOrderBys(end);
		} else {
			where_conditions = extractPart("WHERE", ";", start + strlen(on_conditions));
		}
	
		where_result = buildStack(where_conditions);		
	} else if (end = strstr(start, "ORDER BY")) {
		on_conditions = extractPart("ON", "ORDER BY", start);
		order_by = true;
		// getOrderBys(end);
	} else {
		on_conditions = extractPart("ON", ";", start);
	}

	on_result = buildStack(on_conditions);
	printStack(on_result);
	if(where_result)
		printStack(where_result);
	
	int j;
	for(j = 0; j < i; ++j)
		printf("\n-%s \n", display_fields[j]);

	printf("\njoin_type = %s -- table_name = %s -- join_table = %s\n", join_type, table_name, join_table);	


	// TO DO
	/*
	if(order_by) {
		order_bys = getOrderBys(end);
	}
	*/

	// selectInnerJoin(table_name, join_table, join_type, display_fields, i, on_conditions, where_conditions, order_bys);
}






/****************************************************************** - CREATE - ***********************************************************************************/

int tokenizeCreateDatabase(char *query) {
	char *start = strstr(query, "CREATE DATABASE");
	if(start) {
		start += strlen("CREATE DATABASE");
		while(start[0] == ' ')
			++start;
		char *end = start + 1;
		while(end[0] != ' ' && end[0] != ';') 
			++end;		

		char *database_name = malloc((end - start) + 1);
		strlcpy(database_name, start, (end - start) + 1);

		printf("\ndatabase_name = %s\n", database_name);

		return createDatabase(database_name);
	} else { 
		return -1;
	}
}

bool hasConstraint(char *query, char *constraint) {

        while(query[0] == ' ')
                ++query;

        if(strncmp(query, constraint, strlen(constraint)) == 0)
                return true;
        else
                return false;
}


int setConstraint(char *constraint, char **destination) {

        *destination = malloc(strlen(constraint) + 1);
        strlcpy(*destination, constraint, strlen(constraint));

        return 0;

}

char *getKey(char *marker, char *end) {

        char *key = NULL;
        key = malloc((end - marker) + 1);
        strlcpy(key, marker, end - marker);

        return key;
}

char *extractKey(char *start) {
        while(start[0] == ' ' || start[0] == '(')
                ++start;
        char *end = start + 1;
        while(end[0] != ' ' && end[0] != ')' && end[0] != ',')
                ++end;
        return getKey(start, end);
}

char *setMarker(char *end) {
        while(end[0] != ' ' && end[0] != ')')
                ++end;
        return end;
}


int tokenizeCreateTable(char *query) {
	char *start = strstr(query, "CREATE TABLE");
	if(!start)
		return -1;
	
	start += strlen("CREATE TABLE");
	while(start[0] == ' ')
		++start;
	
	char *end = strstr(start, " ");	 
	char *table_name = malloc((end - start) + 1);
	strlcpy(table_name, start, (end - start) + 1);
	//table_name[len] = '\0';
	printf("\ntable_name %s\n", table_name);

	start = strstr(end, "(");
	if(!start)
		return -1;

	char *column_names[20];
	char *data_types[20];

        //#define MAX_FOREIGN_KEYS 10
        //#define MAX_PRIMARY_KEYS 10

        // constraints
        char *primary_keys[MAX_PRIMARY_KEYS];
        int number_of_primary_keys = 0;
        char *foreign_keys[MAX_FOREIGN_KEYS];
        int number_of_foreign_keys = 0;
        char *foreign_key_tables[MAX_FOREIGN_KEYS];
        char *foreign_key_names[MAX_FOREIGN_KEYS];
        char *not_nulls[20];
        char *checks[20];

        char *marker = start;
        int i = 0;
        while(*marker != ')' && *marker != ';') {       // stop when we encounter the end of the create table statement
                printf("\nmarker = %s\n", marker);
                if(hasConstraint(marker, "PRIMARY KEY")) {
                        printf("\nhas primary key\n");
                        marker = strstr(marker, "(") + 1;
                        while(marker[0] != ')') {
                                primary_keys[number_of_primary_keys] = extractKey(marker);
                                marker = strstr(marker, primary_keys[number_of_primary_keys]) + strlen(primary_keys[number_of_primary_keys]);
                                while(marker[0] == ' ' || marker[0] == ',')
                                        ++marker;
                                ++number_of_primary_keys;
                        }
                } else if(hasConstraint(marker, "FOREIGN KEY")) {
                        printf("\nhas foreign key\n");
                        marker = strstr(marker, "(") + 1;
                        foreign_keys[number_of_foreign_keys] = extractKey(marker);
                        // get the foreign key table
                        marker = strstr(marker, "REFERENCES") + strlen("REFERENCES");
                        while(marker[0] == ' ')
                                ++marker;
                        end = marker + 1;
                        while(end[0] != ' ' && end[0] != '(')
                                ++end;
                        foreign_key_tables[number_of_foreign_keys] = malloc((end - marker) + 1);
                        strlcpy(foreign_key_tables[number_of_foreign_keys], marker, end - marker);

                        // get the foreign key name
                        marker = strstr(end, "(") + 1;
                        while(marker[0] == ' ')
                                ++marker;

                        end = marker + 1;
                        while(end[0] != ' ' && end[0] != ')')
                                ++end;
                        foreign_key_names[number_of_foreign_keys] = malloc((end - marker) + 1);
                        strlcpy(foreign_key_names[number_of_foreign_keys], marker, end - marker);
                        marker = strstr(marker, ")") + 1;
                        ++number_of_foreign_keys;
                } else {
                        marker = extractIdentifier(marker, &column_names[i]);
		        marker = extractType(marker, &data_types[i]);

                        if(hasConstraint(marker, "NOT NULL")) {
                                printf("\nhas NOT NULL\n");
                                setConstraint("NOT NULL", &not_nulls[i]);
                                marker = strstr(marker, "NOT NULL") + strlen("NOT NULL");
                        } else {
                                printf("\nNULL\n");
                                setConstraint("NULL", &not_nulls[i]);
                        }

                        ++i;
                }

                // go from the end of the current field to the start of the next one
                while(marker[0] == ' ' || marker[0] == ',')
                        ++marker;
        }

       	
        int j;
        for(j = 0; j < i; ++j)
                printf("\ncolumn_names[%d] = %s -- data_types[%d] = %s, not_nulls[%d] = %s\n", j, column_names[j], j, data_types[j], j, not_nulls[j]);

        for(j = 0; j < number_of_primary_keys; ++j)
                printf("\nprimary_key[%d] = %s\n", j, primary_keys[j]);

        for(j = 0; j < number_of_foreign_keys; ++j)
                printf("\nforeign_keys[%d] = %s, foreign_key_table[%d] = %s, foreign_key_names[%d] = %s\n", j, foreign_keys[j], j, foreign_key_tables[j], j, foreign_key_names[j]);
       


	create(table_name, column_names, data_types, i);
	printf("\ncreated table %d\n", number_of_primary_keys);
	if(number_of_primary_keys == 0) {	
		// create primary key manually
	} else {
		addConstraintPrimaryKeys(table_name, primary_keys, number_of_primary_keys);
	}

	if(number_of_foreign_keys > 0)
	        addConstraintForeignKeys(table_name, number_of_foreign_keys, foreign_keys, foreign_key_names, foreign_key_tables);

	printf("\nreturning \n");
	return 0;	
}






/****************************************************************** - START - ***********************************************************************************/

int tokenizeKeywords(char *query){	
	// check which statmenet is being executed i.e. is it SELECT or DELETE or DROP etc
	int i;
	for(i = 0; i < NUMBER_OF_KEYWORDS; ++i)	{
		printf("\ni %d\n", i);
		if(strstr(query, keywords[i]) != NULL) {
			printf("\nretuning %d\n", i);
			return i;
		}
	}		

	return -1;
}



void initialize() {
	keywords = malloc(NUMBER_OF_KEYWORDS * sizeof(char *));
	keywords[0] = "SELECT";
	keywords[1] = "INSERT";
	keywords[2] = "DELETE";
	keywords[3] = "DROP";
	keywords[4] = "ALTER TABLE";
	keywords[5] = "UPDATE";
}



/*
void tokenizeJoinClause(char *query) {


	// display fields parser
	while(strncmp(start, "FROM ", strlen("FROM ")) != 0) {

	end = start + 1;
	while(end[0] != ' ' && end[0] != ',' && end[0] != '.')
		++end;
			
	if(end[0] == '.') {	
		start = end + 1;
		while(end[0] != ' ' && end[0] != ',')
			++end;
	}

	display_fields[i] = malloc((end - start) + 1);
	strlcpy(display_fields[i++], start, (end - start) + 1);		
	
	start = end + 1;
	while(start[0] == ' ')
		++start;		
	}




	// clause parser
	char *start = query;

	while(start[0] == ' ')
		++start;

	char *end = start + 1;
	
	while(end[0] != '.')
		++end;
	
	// markers set and remain at the name of the column
	char *start_marker = start;
	char *end_marker = end;
	
	start = ++end;
	while(end[0] != ' ' && end[0] != '=')
		++end;

	// compare to table 
	// else is it the join table
	// else return false
	char *table_columns[10];
	int t;
	char *join_columns[10];
	int j;

	char *table_name = "Orders";

	printf("\nstart_marker = %s\n", start_marker);
	// Customers.CustomerID=Orders.CustomerID
	if(strncmp(table_name, start_marker, end_marker - start_marker) == 0) {
		table_columns[t] = malloc((end - start) + 1);
		strlcpy(table_column, start, (end - start) + 1);
		
		// other end of equals is the join column
		start = strstr(end, ".") + strlen(".");
		end = start + 1;
		while(end[0] != ' ' && end[0] != ';')
			end++;
		join_columns[j] = malloc((end - start) + 1);
		strlcpy(join_columns[j], start, (end - start) + 1);		
	} else if (strncmp(join_table, start_marker, end_marker - start_marker) == 0) {
		join_column = malloc((end - start) + 1);
		strlcpy(join_column, start, (end - start) + 1);

		// other end of equals is the table_column
		start = strstr(end, ".") + strlen(".");
		end = start + 1;
		while(end[0] != ' ' && end[0] != ';')
			end++;
		table_column = malloc((end - start) + 1);
		strlcpy(table_column, start, (end - start) + 1);	
	} else {
		return -1;
	}
}
*/
