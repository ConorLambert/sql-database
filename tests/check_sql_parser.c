#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include "../src/server/access/sql_parser.h"
#include "../libs/libcfu/src/cfuhash.h"


char *database_name1;
char *table_name1;
char *join_table1;
Table *table1;
DataBuffer *dataBuffer;

// basic test data
char *columns1[4];
char *types1[4];

char *data1[4];
char *data2[4];
char *data3[4];
char *data4[4];
char *data5[4];
char *data6[4];
char *data7[4];



// UTILITY FUNCTIONS
void util_createDatabase(){	
	char query[100] = "CREATE DATABASE ";
	strcpy(query, database_name1);
	strcpy(query, ";");
	tokenizeCreateDatabase(query);
}

void util_deleteDatabase(char *database_name) {
	deleteDatabase(database_name)	;
}

void util_createTable() {
	char query[100] = "CREATE TABLE ";
	strcat(query, table_name1);
	strcat(query, " (PersonID int, LastName varchar(255), FirstName varchar(255), Gender char);");
	tokenizeCreateTable(query);
}

void util_freeDataBuffer() {
        cfuhash_destroy (dataBuffer->tables);
        free(dataBuffer);
}


void util_initializeInserts(){
	columns1[0] = "PersonID";
	columns1[1] = "LastName";
	columns1[2] = "FirstName";
	columns1[3] = "Gender";

	types1[0] = "int";
	types1[1] = "varchar(255)";
	types1[2] = "varchar(255)";
	types1[3] = "char";

	data1[0] = "1";
        data1[1] = "value2";
        data1[2] = "value3";
        data1[3] = "value4";

        data2[0] = "2";
        data2[1] = "value2i";
        data2[2] = "value3i";
        data2[3] = "value4i";

	data3[0] = "3";
        data3[1] = "value2ii";
        data3[2] = "value3ii";
        data3[3] = "value4ii";
/*
	data4[0] = "4";
        data4[1] = "value2iii";
        data4[2] = "value3iii";
        data4[3] = "value4iii";

        data5[0] = "5";
        data5[1] = "value2iv";
        data5[2] = "value3iv";
        data5[3] = "value4iv";

	data6[0] = "6";
        data6[1] = "value2v";
        data6[2] = "value3v";
        data6[3] = "value4v";

	data7[0] = "7";
        data7[1] = "value2vi";
        data7[2] = "value3vi";
        data7[3] = "value4vi";
*/


}


char * util_formulateInserts(char *table_name, char **columns, int number_of_columns, char **data, int number_of_data) {
	char *query = malloc(100);
	strcat(query,  "INSERT INTO ");
	strcat(query, table_name);
	
	int i;
	if(number_of_columns > 0) {
		strcat(query, " (");
		for(i = 0; i < number_of_columns - 1; ++i) {	
			strcat(query, columns[i]);
			strcat(query, ",");
		}
		strcat(query, columns[i]);
		strcat(query, ")");
	}	

	printf("\n\n\n\nquery %s, %s\n", query, data[0]);
	strcat(query, " VALUES ");

	strcat(query, "(");
	for(i = 0; i < number_of_data - 1; ++i) {	
		strcat(query, data[i]);
		strcat(query, ",");
	}
	strcat(query, data[i]);
	strcat(query, ")");

	strcat(query, ";");	
	
	return query;
}


util_insert() {
	Record *record = NULL;

	char *test1 = util_formulateInserts(table_name1, columns1, 0, data1, 4);
 
	tokenizeInsertKeyword(test1);
	record = table1->pages[0]->records[0];
        ck_assert(strcmp(record->data[0], data1[0]) == 0);
        ck_assert(strcmp(record->data[1], data1[1]) == 0);
        ck_assert(strcmp(record->data[2], data1[2]) == 0);
        ck_assert(strcmp(record->data[3], data1[3]) == 0);

	char *test2 = util_formulateInserts(table_name1, columns1, 4, data2, 4);
        tokenizeInsertKeyword(test2);
	record = table1->pages[0]->records[1];
        ck_assert(strcmp(record->data[0], data2[0]) == 0);
        ck_assert(strcmp(record->data[1], data2[1]) == 0);
        ck_assert(strcmp(record->data[2], data2[2]) == 0);
        ck_assert(strcmp(record->data[3], data2[3]) == 0);


	char *test3 = util_formulateInserts(table_name1, columns1, 0, data3, 4); 
        tokenizeInsertKeyword(test3);
	record = table1->pages[0]->records[2];
        ck_assert(strcmp(record->data[0], data3[0]) == 0);
        ck_assert(strcmp(record->data[1], data3[1]) == 0);
        ck_assert(strcmp(record->data[2], data3[2]) == 0); // <-----------
        ck_assert(strcmp(record->data[3], data3[3]) == 0);

/*	
	char *test4 = util_formulateInserts(table_name1, columns1, 0, data4, 4); 
        tokenizeInsertKeyword(test4);
	record = table1->pages[0]->records[3];
        ck_assert(strcmp(record->data[0], data4[0]) == 0);
        ck_assert(strcmp(record->data[1], data4[1]) == 0);
        ck_assert(strcmp(record->data[2], data4[2]) == 0); // <-----------
        ck_assert(strcmp(record->data[3], data4[3]) == 0);
*/
	

}


void setup() {

	database_name1 = "test_database";
	table_name1 = "Persons";
        
	initialize();   
	dataBuffer = initializeDataBuffer();      
	util_createDatabase(); 	
	util_createTable();
	table1 = (Table *) cfuhash_get(dataBuffer->tables, table_name1);
	util_initializeInserts();
	util_insert();
}


void teardown() {
	printf("\nTEARDOWN\n");
        drop(table_name1);
	util_freeDataBuffer();	
	deleteDatabase("test_database");
}





bool util_testWhereClause(Stack *result1, Stack *expected1) {
	
	char *result1_pop;
	char *expected1_pop;

	while((result1_pop = pop(result1)) && (expected1_pop = pop(expected1))) {	
		//printf("\nresult1_pop %s, expected1_pop %s\n", result1_pop, expected1_pop);
		if(strcmp(result1_pop, expected1_pop) != 0)		
			return false;
	}	

	return true;
}


Stack *createExpectedStack(char *expected) {
	Stack *result = createStack();
	
	char *start = expected;
	char *beginning = expected;
	
	int i = 0;
	char *end;	
	while(i < strlen(expected)) {		
		while(start[0] == ' ')
			++start;
	
		end = strstr(start, " ");
		if(!end) {
			end = start + 1;
			while(end[0] != '\0') 
				++end;
		}
		char *value = malloc((end - start) + 1);
		strlcpy(value, start, (end - start) + 1); 

		int len = strlen(value);
		pushToOperands(result, value, len);

		start = end + 1;
		i = end - beginning;
	}	

	return result;
}





START_TEST(test_update) {

	printf("\nTESTING UPDATE\n");

        char test1[] = "UPDATE update_table_name SET column1=value1,column2=value2 WHERE some_column=some_value;";
        tokenizeUpdateKeyword(test1);
        char test2[] = "UPDATE update_table_name SET first_name = 'Conor' WHERE rid = 20;";
        tokenizeUpdateKeyword(test2);
} END_TEST


void testInnerJoin() {
	printf("\nTESTING INNER JOIN\n");
        char test1[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Customers INNER JOIN Orders ON Customers.CustomerID=Orders.CustomerID;";
        tokenizeJoin(test1);
        char test2[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Customers INNER JOIN Orders ON Customers.CustomerID=Orders.CustomerID ORDER BY Customers.CustomerName;";
        tokenizeJoin(test2);
        char test3[] = "SELECT  ID, NAME, AMOUNT, DATE FROM CUSTOMERS INNER JOIN ORDERS ON CUSTOMERS.ID=ORDERS.CUSTOMER_ID;";
        tokenizeJoin(test3);
        char test4[] = "SELECT ProductID, Name, ListPrice, UnitPrice FROM SalesOrderDetail JOIN Product ON sd.ProductID = p.ProductID AND sd.UnitPrice < p.ListPrice WHERE p.ProductID = 718;";
        tokenizeJoin(test4);
}


START_TEST (test_join) {
	printf("\nTESTING JOIN\n");

        testInnerJoin();
        //testOuterJoin();
        //testLeftJoin();
        //testRightJoin();
} END_TEST


START_TEST (test_delete_index_intermediate) {

	printf("\nTESTING DELETE INDEX INTERMEDIATE\n");

	/*
	data1[0] = "1";
	data1[1] = "value2";
	data1[2] = "value3";
	data1[3] = "value4";

	data2[0] = "2";
	data2[1] = "value2i";
	data2[2] = "value3i";
	data2[3] = "value4i";

	data3[0] = "3";
	data3[1] = "value2ii";
	data3[2] = "value3ii";
	data3[3] = "value4ii";
	*/

	char insert[] = "INSERT INTO Persons VALUES (4, value2iii, value3iii, value4iii);";
	tokenizeInsertKeyword(insert);	
	ck_assert(table1->pages[0]->number_of_records == 4);
	char insert1[] = "INSERT INTO Persons VALUES (5, value2iv, value3iv, value4iv);";
	tokenizeInsertKeyword(insert1);	
	ck_assert(table1->pages[0]->number_of_records == 5);		
	char insert2[] = "INSERT INTO Persons VALUES (6, value2v, value3v, value4v);";
	tokenizeInsertKeyword(insert2);	
	ck_assert(table1->pages[0]->number_of_records == 6);	
	char insert3[] = "INSERT INTO Persons VALUES (7, value2vi, value3vi, value4vi);";
	tokenizeInsertKeyword(insert3);	
	ck_assert(table1->pages[0]->number_of_records == 7);

	// CREATE INDEX
	printf("\nCreating Index\n");
	createIndex("LastName", table1);
	ck_assert(hasIndex("LastName", table1));	

	// DELETES
	ck_assert(table1->pages[0]->records[0] != NULL);
        char test1[] = "DELETE FROM Persons WHERE LastName=value2 AND FirstName = value3;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test1);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[0] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 6);

		
	ck_assert(table1->pages[0]->records[3] != NULL);
	ck_assert(table1->pages[0]->records[5] != NULL);
        char test2[] = "DELETE FROM Persons WHERE LastName=value2iii OR FirstName = value3v;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test2);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[3] == NULL);
	ck_assert(table1->pages[0]->records[5] == NULL);
	
	
} END_TEST


START_TEST (test_delete_index) {

	
	printf("\nTESTING DELETE INDEX BASIC\n");

	printf("\nCreating Index\n");
	createIndex("PersonID", table1);
	ck_assert(hasIndex("PersonID", table1));

		
	char test2[] = "DELETE FROM Persons WHERE PersonID=value1;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test2);
	printf("\nAsserting \n");
	// NO RECORDS FOUND
	ck_assert(table1->pages[0]->number_of_records == 3);
	

	ck_assert(table1->pages[0]->records[0] != NULL);
	ck_assert(table1->pages[0]->records[0]->data[0]);
        char test1[] = "DELETE FROM Persons WHERE PersonID=1;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test1);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[0] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 2);

	
	
	ck_assert(table1->pages[0]->records[2] != NULL);
	ck_assert(table1->pages[0]->records[2]->data[0]);
        char test3[] = "DELETE FROM Persons WHERE PersonID=3;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test3);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[2] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 1);
	
	
	/*
        char test2[] = "DELETE FROM Customers WHERE CustomerName  =   'Alfreds Futterkiste'  AND    ContactName  ='Maria Anders'   ;";
        tokenizeDeleteKeyword(test2);
        char test3[] = "DELETE * FROM Users;";
        tokenizeDeleteKeyword(test3);
	*/
	
} END_TEST


START_TEST(test_delete_sequential_advanced) {
	// add more records to the table
	
	/*
	data1[0] = "1";
	data1[1] = "value2";
	data1[2] = "value3";
	data1[3] = "value4";

	data2[0] = "2";
	data2[1] = "value2i";
	data2[2] = "value3i";
	data2[3] = "value4i";

	data3[0] = "3";
	data3[1] = "value2ii";
	data3[2] = "value3ii";
	data3[3] = "value4ii";
	*/

	char insert[] = "INSERT INTO Persons VALUES (4, value2iii, value3iii, value4iii);";
	tokenizeInsertKeyword(insert);	
	ck_assert(table1->pages[0]->number_of_records == 4);

	
	char insert1[] = "INSERT INTO Persons VALUES (5, value2iv, value3iv, value4iv);";
	tokenizeInsertKeyword(insert1);	
	ck_assert(table1->pages[0]->number_of_records == 5);

		
	char insert2[] = "INSERT INTO Persons VALUES (6, value2v, value3v, value4v);";
	tokenizeInsertKeyword(insert2);	
	ck_assert(table1->pages[0]->number_of_records == 6);

	
	char insert3[] = "INSERT INTO Persons VALUES (7, value2vi, value3vi, value4vi);";
	tokenizeInsertKeyword(insert3);	
	ck_assert(table1->pages[0]->number_of_records == 7);
	
	
	// DELETES
	ck_assert(table1->pages[0]->records[3] != NULL);
 	char test1[] = "DELETE FROM Persons WHERE LastName=value2iii AND FirstName=value3iii;";
	printf("\n\tTokenizing\n");
        tokenizeDeleteKeyword(test1);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[3] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 6);

	
	ck_assert(table1->pages[0]->records[5] != NULL);
	ck_assert(table1->pages[0]->records[6] != NULL);
 	char test2[] = "DELETE FROM Persons WHERE Gender=value4v OR LastName=value2vi;";
	printf("\n\tTokenizing\n");
        tokenizeDeleteKeyword(test2);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[5] == NULL);
	ck_assert(table1->pages[0]->records[6] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 4);


		
	ck_assert(table1->pages[0]->records[0] != NULL);
	ck_assert(table1->pages[0]->records[2] != NULL);
 	char test3[] = "DELETE FROM Persons WHERE (PersonID=1 AND LastName = value2) OR FirstName=value3ii;";
	printf("\n\tTokenizing\n");
        tokenizeDeleteKeyword(test3);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[0] == NULL);
	ck_assert(table1->pages[0]->records[2] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 2);

	
	// 1 and 4
	ck_assert(table1->pages[0]->records[1] != NULL);
	ck_assert(table1->pages[0]->records[4] != NULL);
 	char test4[] = "DELETE FROM Persons WHERE (LastName=value2i AND FirstName=value3i) OR (LastName=value2iv AND FirstName=value3iv);";
	printf("\n\tTokenizing\n");
        tokenizeDeleteKeyword(test4);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[1] == NULL);
	ck_assert(table1->pages[0]->records[4] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 0);

	
 	char test5[] = "DELETE FROM Persons WHERE (LastName=value2i AND FirstName=value3i) OR (LastName=value2iv AND FirstName=value3iv);";
	printf("\n\tTokenizing\n");
        tokenizeDeleteKeyword(test5);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->number_of_records == 0);
	



} END_TEST



START_TEST(test_delete_sequential_intermediate) {
	
	// multiple records deletes
	// same as the first record
	char insert[] = "INSERT INTO Persons VALUES (4, value2, value3, value4);";
	tokenizeInsertKeyword(insert);	
	ck_assert(table1->pages[0]->number_of_records == 4);

	// delete record
	ck_assert(table1->pages[0]->records[3] != NULL);
        ck_assert(table1->pages[0]->records[0] != NULL);
	char test1[] = "DELETE FROM Persons WHERE LastName=value2;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test1);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[3] == NULL);
        ck_assert(table1->pages[0]->records[0] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 2);
	
	// delete record
        ck_assert(table1->pages[0]->records[1] != NULL);
	char test2[] = "DELETE FROM Persons WHERE LastName=value2i;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test2);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[1] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 1);

	// delete non-existent record
	char test3[] = "DELETE FROM Persons WHERE LastName=value2i;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test3);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->number_of_records == 1);
	

} END_TEST


START_TEST (test_delete_sequential_basic) {
	
	// delete non-existent record
	
	char test1[] = "DELETE FROM Persons WHERE PersonID=value1;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test1);
	printf("\nAsserting \n");
	// NO RECORDS FOUND
	ck_assert(table1->pages[0]->number_of_records == 3);
	

	// delete record
	ck_assert(table1->pages[0]->records[0] != NULL);
	ck_assert(table1->pages[0]->records[0]->data[0]);
        char test2[] = "DELETE FROM Persons WHERE LastName=value2;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test2);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[0] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 2);

	
	// delete record
	ck_assert(table1->pages[0]->records[1] != NULL);
	ck_assert(table1->pages[0]->records[1]->data[0]);
        char test3[] = "DELETE FROM Persons WHERE FirstName=value3i;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test3);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[1] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 1);	

		
	// delete record
	ck_assert(table1->pages[0]->records[2] != NULL);
	ck_assert(table1->pages[0]->records[2]->data[0]);
        char test4[] = "DELETE FROM Persons WHERE Gender=value4ii;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test4);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[2] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 0);	
	

		
	// attempted delete record (empty table)
        char test5[] = "DELETE FROM Persons WHERE Gender=value4ii;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test5);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->number_of_records == 0);	
	
	

} END_TEST


START_TEST(test_build_expression_tree) {
	char expression[] = "first_name = 'Conor' AND age = 40";
	Stack *result = buildStack(expression);
	char *conversion = toString(result);	
	Node *root = buildExpressionTree(conversion);

	printf("\nck_assert\n");
	ck_assert_str_eq(root->value, "&");

	Node *right, *left;
	right = root->right;
	ck_assert_str_eq(right->value, "=");
	left = right->left;
	ck_assert_str_eq(left->value, "age");
	right = right->right;
	ck_assert_str_eq(right->value, "40");

	left = root->left;
	ck_assert_str_eq(left->value, "=");
	right = left->right;
	ck_assert_str_eq(right->value, "'Conor'");
	left = left->left;
	ck_assert_str_eq(left->value, "first_name");


	
	// last_name 'PHILIP' = first_name 'Conor' age 40 = = & |
	char expression2[] = "last_name = 'PHILIP' OR (first_name = 'Conor' AND age = 40)";
	result = buildStack(expression2);
	conversion = toString(result);	
	root = buildExpressionTree(conversion);

	ck_assert_str_eq(root->value, "|");

	right = root->right;
	ck_assert_str_eq(right->value, "&");
	left = root->left;
	ck_assert_str_eq(left->value, "=");
	right = left->right;
	ck_assert_str_eq(right->value, "'PHILIP'");
	left = left->left;
	ck_assert_str_eq(left->value, "last_name");
	left = root->right->left;
	ck_assert_str_eq(left->value, "=");
	right = left->right;
	ck_assert_str_eq(right->value, "40");
	left = left->left;
	ck_assert_str_eq(left->value, "age");
	right = root->right->right;
	ck_assert_str_eq(right->value, "=");
	left = right->left;
	ck_assert_str_eq(left->value, "first_name");
	right = right->right;
	ck_assert_str_eq(right->value, "'Conor'");

} END_TEST



START_TEST(test_build_stack) {
	printf("\nTESTING Build Stack\n");

	Stack *expectedResult;

        char expression1[] = "A * (B + C * D) + E";
	expectedResult = createExpectedStack("A B C D * + * E +");
	Stack *result1 = buildStack(expression1);
	printStack(result1);
        ck_assert(util_testWhereClause(result1, expectedResult));
             
	
        char expression2[] = "A * B ^ C + D";
     	expectedResult = createExpectedStack("A B C ^ * D +");
	Stack *result2 = buildStack(expression2);
	printStack(result2);
        ck_assert(util_testWhereClause(result2, expectedResult));

	
	char expression3[] = "A - B + C";
        expectedResult = createExpectedStack("A B - C +");
	Stack *result3 = buildStack(expression3);
	printStack(result3);
        ck_assert(util_testWhereClause(result3, expectedResult));

	
	char expression4[] = "3 + 4 * 2 / ( 1 - 5 ) ^ 2 ^ 3";
        expectedResult = createExpectedStack("3 4 2 * 1 5 - 2 3 ^ ^ / +");
	Stack *result4 = buildStack(expression4);
	printStack(result4);
        ck_assert(util_testWhereClause(result4, expectedResult));

	
	char expression5[] = "3 + 4 AND 4 + 3";
 	expectedResult = createExpectedStack("3 4 + 4 3 + &");
	Stack *result5 = buildStack(expression5);
	printStack(result5);
        ck_assert(util_testWhereClause(result5, expectedResult));
       	
	
	char expression6[] = "first_name = 'Conor' AND age = 40";
        expectedResult = createExpectedStack("first_name 'Conor' = age 40 = &");
	Stack *result6 = buildStack(expression6);
	printStack(result6);
        ck_assert(util_testWhereClause(result6, expectedResult));
	

	char expression7[] = "last_name = 'PHILIP' OR (first_name = 'Conor' AND age = 40)";
	expectedResult = createExpectedStack("last_name 'PHILIP' = first_name 'Conor' age 40 = = & |");
	Stack *result7 = buildStack(expression7);
	printStack(result7);
        ck_assert(util_testWhereClause(result7, expectedResult));
	

        char expression8[] = "(last_name = 'PHILIP' OR first_name = 'Conor') AND age = 40";
        expectedResult = createExpectedStack("last_name 'PHILIP' first_name 'Conor' = = | age 40 = &");
	Stack *result8 = buildStack(expression8);
	printStack(result8);
        ck_assert(util_testWhereClause(result8, expectedResult));

	char expression9[] = "last_name = 'PHILIP' OR (first_name = 'Conor' AND age >= 40)";
	expectedResult = createExpectedStack("last_name 'PHILIP' = first_name 'Conor' age 40 $ = & |");
	Stack *result9 = buildStack(expression9);
	ck_assert(util_testWhereClause(result9, expectedResult));; 	

} END_TEST



START_TEST (test_alter_rename){
	printf("\nTESTING Alter Rename\n");

        // rename column
        char test1[] = "ALTER TABLE Persons CHANGE COLUMN FirstName TO SomeName;";
        tokenizeAlterKeyword(test1);
	printf("\nname : %s\n", table1->format->fields[2]->name);
        ck_assert(strcmp(table1->format->fields[2]->name, "SomeName") == 0);
        ck_assert(strcmp(table1->format->fields[2]->type, "varchar(255)") == 0);
	
	/*
        // rename table
        char test2[] = "ALTER TABLE supplier RENAME TO vendor;";
        tokenizeAlterKeyword(test2);
	*/
} END_TEST


START_TEST (test_alter_drop) {
	printf("\nTESTING Alter Drop\n");

        Format *format = getTableFormat(table1);	
	int original_number_of_fields = format->number_of_fields;

	char test1[] = "ALTER TABLE Persons DROP COLUMN LastName;";
        tokenizeAlterKeyword(test1);
	printf("\nnumber_of_fields = %d\n", table1->format->number_of_fields);
        ck_assert(table1->format->number_of_fields == original_number_of_fields - 1);
	ck_assert(strcmp(table1->format->fields[0]->name, "PersonID") == 0);
        ck_assert(strcmp(table1->format->fields[1]->name, "FirstName") == 0);
        ck_assert(strcmp(table1->format->fields[2]->name, "Gender") == 0);
        ck_assert(table1->format->fields[3] == NULL);

	ck_assert(strcmp(table1->format->fields[0]->type, "int") == 0);
        ck_assert(strcmp(table1->format->fields[1]->type, "varchar(255)") == 0);
        ck_assert(strcmp(table1->format->fields[2]->type, "char") == 0);

	
	Record *record;
        printf("\n%s, %s, %s\n", data1[1], data1[2], data1[3]);
        record = table1->pages[0]->records[0];
        ck_assert(strcmp(record->data[0], data1[0]) == 0);
        ck_assert(strcmp(record->data[1], data1[2]) == 0);
        ck_assert(strcmp(record->data[2], data1[3]) == 0);
        ck_assert(record->data[3] == NULL);

	printf("\n%s, %s, %s\n", data2[1], data2[2], data2[3]);
        record = table1->pages[0]->records[1];
        ck_assert(strcmp(record->data[0], data2[0]) == 0);
        ck_assert(strcmp(record->data[1], data2[2]) == 0);
        ck_assert(strcmp(record->data[2], data2[3]) == 0);
        ck_assert(record->data[3] == NULL);


	// TO DO
	// if the user inserts data into only specific columns then a drop column may have a different affect on it (or maybe not)	
	/*
        printf("\n%s, %s, %s\n", data3[1], data3[2], data3[3]);
        record = table1->pages[0]->records[2];
        ck_assert(strcmp(record->data[0], data3[0]) == 0);
        ck_assert(strcmp(record->data[1], data3[2]) == 0);
        ck_assert(strcmp(record->data[2], data3[3]) == 0);
        ck_assert(record->data[3] == NULL);
	*/


	/*
        char test2[] = "ALTER TABLE table_name DROP COLUMN  (column_name1,column_name2);";
        tokenizeAlterKeyword(test2);
	*/
	
} END_TEST


START_TEST (test_alter_add) {
	printf("\nTESTING Alter Add\n");

	Format *format = getTableFormat(table1);	
	int original_number_of_fields = format->number_of_fields;

        char test1[] = "ALTER TABLE Persons ADD supplier_name char(50);";
        tokenizeAlterKeyword(test1);
	ck_assert(format->number_of_fields == original_number_of_fields + 1);
	ck_assert(strcmp(format->fields[format->number_of_fields - 1]->name, "supplier_name") == 0);
	ck_assert(strcmp(format->fields[format->number_of_fields - 1]->type, "char(50)") == 0);

	
	original_number_of_fields = format->number_of_fields;
        char test2[] = "ALTER TABLE Persons ADD (column_1 char(257)  , column_2 INT , column_3 CHAR )  ;";
        tokenizeAlterKeyword(test2);
	ck_assert(format->number_of_fields == original_number_of_fields + 3);
	ck_assert(strcmp(format->fields[format->number_of_fields - 3]->name, "column_1") == 0);
	ck_assert(strcmp(format->fields[format->number_of_fields - 3]->type, "char(257)") == 0);

	ck_assert(strcmp(format->fields[format->number_of_fields - 2]->name, "column_2") == 0);
	ck_assert(strcmp(format->fields[format->number_of_fields - 2]->type, "INT") == 0);

	ck_assert(strcmp(format->fields[format->number_of_fields - 1]->name, "column_3") == 0);
	ck_assert(strcmp(format->fields[format->number_of_fields - 1]->type, "CHAR") == 0);

} END_TEST


START_TEST (test_alter) {
        printf("\nTESTING ALTER\n");

	 //testAlterAdd();

         //testAlterDrop();

         //testAlterRename();
} END_TEST



START_TEST (test_insert) {
	printf("\nTESTING INSERT\n");

	char test3[] = "INSERT INTO Persons (PersonID, LastName, Gender) VALUES ( value1ii , value2ii , value4ii);";
        tokenizeInsertKeyword(test3);
	Record *record = table1->pages[0]->records[3];
        ck_assert(strcmp(record->data[0], "value1ii") == 0);
        ck_assert(strcmp(record->data[1], "value2ii") == 0);
        ck_assert(!record->data[2]); // <-----------
        ck_assert(strcmp(record->data[3], "value4ii") == 0);

} END_TEST



void testCreateDatabase() {
	printf("\nTESTING CREATE DATABASE\n");

        char test1[] = "CREATE DATABASE dbname;";
        tokenizeCreateDatabase(test1);

	char *folder = "data/dbname";
        struct stat sb;
	printf("\nim here\n");
        stat(folder, &sb);
        ck_assert(stat(folder, &sb) == 0 && S_ISDIR(sb.st_mode));
	printf("\nim here\n");
	util_deleteDatabase("dbname");
        ck_assert(stat(folder, &sb) == -1);

		
 	char test2[] = "CREATE DATABASE my_db;";
        tokenizeCreateDatabase(test2);

	char *folder2 = "data/my_db";
        struct stat sb2;
        stat(folder2, &sb2);
        ck_assert(stat(folder2, &sb2) == 0 && S_ISDIR(sb2.st_mode));

	util_deleteDatabase("my_db");
        ck_assert(stat(folder2, &sb2) == -1);
	
}


void testCreateTable() {
	printf("\nTESTING CREATE TABLE\n");

        char test2[] = "CREATE TABLE Persons (PersonID int, LastName varchar(255), FirstName varchar(255), Height double);";

	dataBuffer = initializeDataBuffer();

	printf("\nTESTING\n%s\n", test2);	
        tokenizeCreateTable(test2);

	table1 = (Table *) cfuhash_get(dataBuffer->tables, "Persons");

	char *data_types[4];  
	data_types[0] = "int";
	data_types[1] = "varchar(255)";
	data_types[2] = "varchar(255)";
	data_types[3] = "double";

	char *column_names[4];  
	column_names[0] = "PersonID";
	column_names[1] = "LastName";
	column_names[2] = "FirstName";
	column_names[3] = "Height";	


        int i;
        for(i = 0; i < table1->format->number_of_fields; ++i){
               	printf("\n\t\tcolumn_name[%d] = %s\n", i, table1->format->fields[i]->name);
		printf("\n\t\tdata_type[%d] = %s\n", i, table1->format->fields[i]->type);
		ck_assert(table1->format->number_of_fields == 4);
                ck_assert(strcmp(table1->format->fields[i]->type, data_types[i]) == 0);
                ck_assert(strcmp(table1->format->fields[i]->name, column_names[i]) == 0);
        }


	// TO DO
	// try with primary key and/or foreign key

}



START_TEST(test_create) {
	printf("\nTESTING CREATE\n");
        testCreateDatabase();
        testCreateTable();
} END_TEST



START_TEST(full_test){
	printf("\nTesting All Query Types\n");

        char test[] = "SELECT first_name, last_name FROM Customers, Users WHERE age = 20;";
        char test1[] = "SELECT first_name, last_name FROM Customers;";
        char test2[] = "SELECT * FROM Customers WHERE City = 'Berlin' OR City = 'München';";
        char test3[] = "SELECT * FROM Customers WHERE Country = 'Germany' AND (City = 'Berlin' OR City = 'München');";
        char test4[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Customers INNER JOIN Orders ON Customers.CustomerID=Orders.CustomerID";

        int result = tokenizeKeywords(test4);
        if(result == SELECT)
                tokenizeKeywordSelect(test4);

} END_TEST



START_TEST (test_tokenize_keyword) {
	printf("\nTESTING Tokenize\n");

	char test0[] = "SELECT first_name, last_name FROM Customers, Users WHERE age = 20;";
        
	initialize();
	int result = tokenizeKeywords(test0);
        ck_assert(result == SELECT);	                
} END_TEST


/*
int main() {
        initialize();

        //tests();

        //testBuildStack();

        //testInsert();

        //testAlter();

        //testDelete();

        //testUpdate();

        //testJoin();

        testCreate();
}
*/



Suite * storage_suite(void)
{
	Suite *s;
	TCase *tc_create;
	TCase *tc_tokenize;
	TCase *tc_insert;
	TCase *tc_alter;
	TCase *tc_build_stack;
	TCase *tc_build_expression_tree;
	TCase *tc_delete;

	s = suite_create("SQL Parser");

	
	tc_tokenize = tcase_create("Tokenize");
	tcase_add_test(tc_tokenize, test_tokenize_keyword);

	// Create test case
	tc_create = tcase_create("Create Table/Database");
	tcase_add_test(tc_create, test_create);

	tc_insert = tcase_create("Inserting");
	tcase_add_test(tc_insert, test_insert);
	tcase_add_checked_fixture(tc_insert, setup, teardown);
	
	tc_alter = tcase_create("Altering");
	tcase_add_test(tc_alter, test_alter);
	tcase_add_test(tc_alter, test_alter_add);
	tcase_add_test(tc_alter, test_alter_drop);
	tcase_add_test(tc_alter, test_alter_rename);
	tcase_add_checked_fixture(tc_alter, setup, teardown);

	tc_build_stack = tcase_create("Building Stack");
	tcase_add_test(tc_build_stack, test_build_stack);
	tcase_add_checked_fixture(tc_build_stack, setup, teardown);

	tc_build_expression_tree = tcase_create("Building Expression Tree");
	tcase_add_test(tc_build_expression_tree, test_build_expression_tree);
	tcase_add_checked_fixture(tc_build_expression_tree, setup, teardown);

	tc_delete = tcase_create("Delete");
	tcase_add_test(tc_delete, test_delete_sequential_basic);
	tcase_add_test(tc_delete, test_delete_sequential_intermediate);
	tcase_add_test(tc_delete, test_delete_sequential_advanced);
	tcase_add_test(tc_delete, test_delete_index);
	tcase_add_test(tc_delete, test_delete_index_intermediate);
	tcase_add_checked_fixture(tc_delete, setup, teardown);

	// Add test cases to suite 
	suite_add_tcase(s, tc_tokenize);
	suite_add_tcase(s, tc_create);
	suite_add_tcase(s, tc_insert);
	suite_add_tcase(s, tc_alter);
	suite_add_tcase(s, tc_build_stack);
	suite_add_tcase(s, tc_build_expression_tree);
	suite_add_tcase(s, tc_delete);
		

	return s;
}


int main(void)
{
   int number_failed;
   Suite *s;
   SRunner *sr;

   s = storage_suite();
   sr = srunner_create(s);
	
   srunner_run_all(sr, CK_NORMAL);
   number_failed = srunner_ntests_failed(sr);
   srunner_free(sr);
   return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
