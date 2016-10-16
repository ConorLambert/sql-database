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
	destroyDataBuffer();
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

}


char * util_formulateInserts(char *table_name, char **columns, int number_of_columns, char **data, int number_of_data) {
	char *query = calloc(100, sizeof(char));
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
}


void setup() {
	printf("\n\t\tSETUP\n");
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

void setup2(){
	setup();
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
	char insert4[] = "INSERT INTO Persons VALUES (8, value2vii, value3vii, value4vii);";
	tokenizeInsertKeyword(insert4);	
	ck_assert(table1->pages[0]->number_of_records == 8);
	char insert5[] = "INSERT INTO Persons VALUES (9, value2viii, value3viii, value4viii);";
	tokenizeInsertKeyword(insert5);	
	ck_assert(table1->pages[0]->number_of_records == 9);
	char insert6[] = "INSERT INTO Persons VALUES (10, value2x, value3x, value4x;";
	tokenizeInsertKeyword(insert6);	
	ck_assert(table1->pages[0]->number_of_records == 10);
	char insert7[] = "INSERT INTO Persons VALUES (11, value2xi, value3xi, value4xi);";
	tokenizeInsertKeyword(insert7);	
	ck_assert(table1->pages[0]->number_of_records == 11);
}


void teardown() {
	printf("\nTEARDOWN\n");
        drop(table_name1);
	util_freeDataBuffer();	
	deleteDatabase("test_database");
	printf("\n\t\tfinishing teardown\n");
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






START_TEST(test_delete_index_advanced) {
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
	char insert4[] = "INSERT INTO Persons VALUES (8, value2vii, value3vii, value4vii);";
	tokenizeInsertKeyword(insert4);	
	ck_assert(table1->pages[0]->number_of_records == 8);
	char insert5[] = "INSERT INTO Persons VALUES (9, value2viii, value3viii, value4viii);";
	tokenizeInsertKeyword(insert5);	
	ck_assert(table1->pages[0]->number_of_records == 9);
	char insert6[] = "INSERT INTO Persons VALUES (10, value2x, value3x, value4x;";
	tokenizeInsertKeyword(insert6);	
	ck_assert(table1->pages[0]->number_of_records == 10);
	char insert7[] = "INSERT INTO Persons VALUES (11, value2xi, value3xi, value4xi);";
	tokenizeInsertKeyword(insert7);	
	ck_assert(table1->pages[0]->number_of_records == 11);



	// CREATE INDEX
	printf("\nCreating Index\n");
	createIndex("LastName", table1);
	ck_assert(hasIndex("LastName", table1));	
	createIndex("FirstName", table1);
	ck_assert(hasIndex("FirstName", table1));	


	ck_assert(table1->pages[0]->records[0] != NULL);
	ck_assert(table1->pages[0]->records[5] != NULL);
        char test1[] = "DELETE FROM Persons WHERE (LastName = value2v AND FirstName = value3v) OR (LastName=value2 AND FirstName = value3);";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test1);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[0] == NULL);
	ck_assert(table1->pages[0]->records[5] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 9);
	
		
	ck_assert(table1->pages[0]->records[3] != NULL);
	ck_assert(table1->pages[0]->records[6] != NULL);
        char test2[] = "DELETE FROM Persons WHERE LastName=value2iii OR FirstName = value3vi;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test2);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[3] == NULL);
	ck_assert(table1->pages[0]->records[6] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 7);

	
	ck_assert(table1->pages[0]->records[1] != NULL);
	ck_assert(table1->pages[0]->records[2] != NULL);
	ck_assert(table1->pages[0]->records[7] != NULL);
        char test3[] = "DELETE FROM Persons WHERE LastName=value2i OR FirstName = value3ii OR Gender = value4vii;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test3);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[1] == NULL);
	ck_assert(table1->pages[0]->records[2] == NULL);
	ck_assert(table1->pages[0]->records[7] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 4);

		
	ck_assert(table1->pages[0]->records[4] != NULL);
	ck_assert(table1->pages[0]->records[8] != NULL);
	ck_assert(table1->pages[0]->records[9] != NULL);
	ck_assert(table1->pages[0]->records[10] != NULL);
        char test4[] = "DELETE FROM Persons WHERE (LastName=value2iv OR PersonID = 9) OR (FirstName = value3xi OR LastName = value2x);";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test4);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[4] == NULL);
	ck_assert(table1->pages[0]->records[8] == NULL);
	ck_assert(table1->pages[0]->records[9] == NULL);
	ck_assert(table1->pages[0]->records[10] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 0);

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
	
	//printf("\nck_assert(table1->pages[0]->records[2]->data[1] = %s\n", table1->pages[0]->records[2]->data[1]);
	RecordKey *recordKey = findRecordKey(table1, 2);
	printf("\nrecordKey->key %d, recordKey->page_number %d, recordKey->slot_number %d\n", recordKey->rid, recordKey->value->page_number, recordKey->value->slot_number);
	print_subtree(table1->header_page->b_tree, table1->header_page->b_tree->root);
	
	ck_assert(table1->pages[0]->records[3] != NULL);
	ck_assert(table1->pages[0]->records[5] != NULL);
        char test2[] = "DELETE FROM Persons WHERE LastName=value2iii OR FirstName = value3v;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test2);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[3] == NULL);
	ck_assert(table1->pages[0]->records[5] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 4);

	//printf("\nck_assert(table1->pages[0]->records[2]->data[1] = %s\n", table1->pages[0]->records[2]->data[1]);
	recordKey = findRecordKey(table1, 2);
	printf("\nrecordKey->key %d, recordKey->page_number %d, recordKey->slot_number %d\n", recordKey->rid, recordKey->value->page_number, recordKey->value->slot_number);
	print_subtree(table1->header_page->b_tree, table1->header_page->b_tree->root);


	ck_assert(table1->pages[0]->records[1] != NULL);
	ck_assert(table1->pages[0]->records[2] != NULL);
        char test3[] = "DELETE FROM Persons WHERE LastName=value2i OR LastName = value2ii;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test3);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->records[1] == NULL);
	ck_assert(table1->pages[0]->records[2] == NULL);
	ck_assert(table1->pages[0]->number_of_records == 2);

	
	
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

	/*	
 	char test5[] = "DELETE FROM Persons WHERE (LastName=value2i AND FirstName=value3i) OR (LastName=value2iv AND FirstName=value3iv);";
	printf("\n\tTokenizing\n");
        tokenizeDeleteKeyword(test5);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->number_of_records == 0);
	*/



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
	/*
        char test5[] = "DELETE FROM Persons WHERE Gender=value4ii;";
	printf("\nTokenizing\n");
        tokenizeDeleteKeyword(test5);
	printf("\nAsserting \n");
	ck_assert(table1->pages[0]->number_of_records == 0);	
	*/
	

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




START_TEST(test_select_basic) {
	printf("\n\n\nTESTING Select SQL Parser\n");
	
	char ***data;

        char test1[] = "SELECT Gender FROM Persons WHERE LastName = value2v OR FirstName = value3iv;";
	printf("\nTokenizing\n");
        data = tokenizeKeywordSelect(test1);
	printf("\nAsserting \n");
	ck_assert_str_eq(data[0][0], "value4iv");
	printf("\n\n");
	ck_assert_str_eq(data[1][0], "value4v");
	
	char test2[] = "SELECT Gender, FirstName, LastName FROM Persons WHERE LastName = value2vii AND Gender = value4vii;";
	printf("\nTokenizing\n");
        data = tokenizeKeywordSelect(test2);
	printf("\nAsserting \n");
	ck_assert_str_eq(data[0][0], "value4vii");
	printf("\n\n");
	ck_assert_str_eq(data[0][1], "value3vii");
	printf("\n\n");
	ck_assert_str_eq(data[0][2], "value2vii");

	char test3[] = "SELECT PersonID, FirstName FROM Persons WHERE LastName=value2i OR FirstName = value3ii OR Gender = value4vii;";
	printf("\nTokenizing\n");
        data = tokenizeKeywordSelect(test3);
	printf("\nAsserting \n");
	ck_assert_str_eq(data[0][0], "2");	
	ck_assert_str_eq(data[0][1], "value3i");
	printf("\n%s\t%s\n", data[0][0], data[0][1]);
	ck_assert_str_eq(data[1][0], "3");
	ck_assert_str_eq(data[1][1], "value3ii");
	printf("\n%s\t%s\n", data[1][0], data[1][1]);
	ck_assert_str_eq(data[2][0], "8");
	ck_assert_str_eq(data[2][1], "value3vii");
	printf("\n%s\t%s\n", data[2][0], data[2][1]);
	printf("\n");

	// select non-existent record
	char test4[] = "SELECT PersonID, FirstName FROM Persons WHERE LastName=value2vv;";
	printf("\nTokenizing\n");
        data = tokenizeKeywordSelect(test4);
	printf("\nAsserting \n");
	ck_assert(data == NULL);
	
	
} END_TEST


START_TEST(test_update_record){

        printf("\nTESTING Alter Record\n");

	char test1[] = "UPDATE Persons SET Gender=new_gender WHERE PersonID=2;";
	printf("\nTokenizing\n");
        tokenizeUpdateKeyword(test1);
	printf("\nAsserting\n");
        ck_assert_str_eq(table1->pages[0]->records[1]->data[3], "new_gender");

	char test2[] = "UPDATE Persons SET PersonID=12 WHERE LastName=value2iv AND FirstName = value3iv;";
	printf("\nTokenizing\n");
        tokenizeUpdateKeyword(test2);
	printf("\nAsserting\n");
        ck_assert_str_eq(table1->pages[0]->records[4]->data[0], "12");

	char test3[] = "UPDATE Persons SET FirstName=NewName WHERE PersonID > 9;";
	printf("\nTokenizing\n");
        tokenizeUpdateKeyword(test3);
	printf("\nAsserting\n");
        ck_assert_str_eq(table1->pages[0]->records[9]->data[2], "NewName");
	ck_assert_str_eq(table1->pages[0]->records[10]->data[2], "NewName");

	char test4[] = "UPDATE Persons SET FirstName=OtherName WHERE PersonID > 1 AND PersonID < 5;";
	printf("\nTokenizing\n");
        tokenizeUpdateKeyword(test4);
	printf("\nAsserting\n");
        ck_assert_str_eq(table1->pages[0]->records[1]->data[2], "OtherName");
        ck_assert_str_eq(table1->pages[0]->records[2]->data[2], "OtherName");
        ck_assert_str_eq(table1->pages[0]->records[3]->data[2], "OtherName");
} END_TEST








/********************************************************************* ALTER ************************************************************************************/

START_TEST (test_alter_rename_column){
	printf("\nTESTING Alter Rename\n");

        // rename column
        char test1[] = "ALTER TABLE Persons CHANGE COLUMN FirstName TO SomeName;";
        tokenizeAlterKeyword(test1);
	printf("\nname : %s\n", table1->format->fields[2]->name);
        ck_assert(strcmp(table1->format->fields[2]->name, "SomeName") == 0);
        ck_assert(strcmp(table1->format->fields[2]->type, "varchar(255)") == 0);
	
} END_TEST


START_TEST (test_alter_drop_column) {
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


	
	original_number_of_fields = format->number_of_fields;	
        char test2[] = "ALTER TABLE Persons DROP COLUMN (FirstName,PersonID);";
        tokenizeAlterKeyword(test2);
	printf("\nnumber_of_fields = %d\n", table1->format->number_of_fields);
        ck_assert(table1->format->number_of_fields == original_number_of_fields - 2);
        ck_assert_str_eq(table1->format->fields[0]->name, "Gender");
        ck_assert_str_eq(table1->format->fields[0]->type, "char");
	ck_assert(table1->format->fields[1] == NULL);
	ck_assert(table1->format->fields[2] == NULL);
	ck_assert(table1->format->fields[3] == NULL);
        ck_assert_str_eq(table1->pages[0]->records[0]->data[0], data1[3]);		
	
	// TO DO
	// if the user inserts data into only specific columns then a drop column may have a different affect on it (or maybe not)
	
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







/********************************************************************* DELETE **********************************************************************************/



START_TEST(test_drop_table) {
        printf("\nTESTING Delete Table\n");
	/*
        // DELETE
	char test1[] = "DROP TABLE table_name;";
	tokenizeDropKeyword(test1);		

        ck_assert(!cfuhash_exists(dataBuffer->tables, table_name1));
	*/
} END_TEST







/************************************************************************* JOIN *********************************************************************************/

// LEFT JOIN

START_TEST(test_right_join_index_key) {
	printf("\n\n\n\nTESTING RIGHT JOIN INDEX KEY\n");
       
	tokenizeCreateTable("CREATE TABLE Orders (OrderID int, CustomerID int, EmployeeID int, PRIMARY KEY(OrderID));");
	tokenizeCreateTable("CREATE TABLE Customers (CustomerID int, CustomerName varchar(50), ContactName varchar(50), Address varchar(70), PRIMARY KEY(CustomerID));");

	Table *orders = (Table *) cfuhash_get(dataBuffer->tables, "Orders");
	Table *customers = (Table *) cfuhash_get(dataBuffer->tables, "Customers");
	createIndex("CustomerID", orders);
	ck_assert(hasIndex("CustomerID", orders));
	ck_assert(orders);
	ck_assert(customers);
	ck_assert(isPrimaryKey(customers, "CustomerID"));

	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10307, 1, 10);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10308, 1, 7);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10309, 37, 3);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10310, 1, 13);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10311, 77, 8);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10312, 1, 5);");

	tokenizeInsertKeyword("INSERT INTO Customers VALUES(0, 'AlfredsFutterkiste', 'MariaAnder', 'ObereStr.571');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(1, 'AnaTrujilloEmparedadosyhelados', 'AnaTrujillo', 'Avda.delaConstitución2222');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(2, 'AntonioMorenoTaquería', 'MariaAnder', 'ObereStr.571');");			

	char ***result;	
	char test2[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Orders RIGHT JOIN Customers ON Customers.CustomerID=Orders.CustomerID;";
	result = tokenizeJoin(test2);
	printf("\nresult[0] = %s\n", result[0][0]);	
	/*
	ck_assert_str_eq(result[0][0], "'AlfredsFutterkiste'");
	ck_assert(result[0][1] == NULL);
	ck_assert_str_eq(result[1][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][1], "10308");
	ck_assert_str_eq(result[2][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[2][1], "10307");	
	ck_assert_str_eq(result[3][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[3][1], "10310"); 
	ck_assert_str_eq(result[4][0], "'AnaTrujilloEmparedadosyhelados'");
 	ck_assert_str_eq(result[4][1], "10312");
	ck_assert_str_eq(result[5][0], "'AntonioMorenoTaquería'");
	ck_assert(result[5][1] == NULL); 	
	*/
} END_TEST




START_TEST(test_right_join_primary_key) {
	printf("\n\n\n\nTESTING LEFT JOIN\n");
       
	tokenizeCreateTable("CREATE TABLE Orders (OrderID int, CustomerID int, EmployeeID int, PRIMARY KEY(OrderID));");
	tokenizeCreateTable("CREATE TABLE Customers (CustomerID int, CustomerName varchar(50), ContactName varchar(50), Address varchar(70), PRIMARY KEY(CustomerID));");
	Table *orders = (Table *) cfuhash_get(dataBuffer->tables, "Orders");
	Table *customers = (Table *) cfuhash_get(dataBuffer->tables, "Customers");
	ck_assert(orders);
	ck_assert(customers);
	ck_assert(isPrimaryKey(customers, "CustomerID"));

	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10307, 1, 10);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10308, 1, 7);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10309, 37, 3);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10310, 1, 13);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10311, 77, 8);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10312, 1, 5);");

	tokenizeInsertKeyword("INSERT INTO Customers VALUES(0, 'AlfredsFutterkiste', 'MariaAnder', 'ObereStr.571');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(1, 'AnaTrujilloEmparedadosyhelados', 'AnaTrujillo', 'Avda.delaConstitución2222');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(2, 'AntonioMorenoTaquería', 'MariaAnder', 'ObereStr.571');");			
	
	char ***result;
	char test1[] = "SELECT Orders.OrderID, Customers.CustomerName FROM Customers RIGHT JOIN Orders ON Customers.CustomerID=Orders.CustomerID;";
        result = tokenizeJoin(test1);
	printf("\nresult[0][0] = %s\n", result[0][0]);
	//printf("\nresult[0][1] = %s\n", result[0][1]);

	/*
	ck_assert_str_eq(result[0][0], "10307");
	ck_assert_str_eq(result[0][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][0], "10308");
	ck_assert_str_eq(result[1][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[2][0], "10309");
	ck_assert(result[2][1] == NULL);
	ck_assert_str_eq(result[3][0], "10310");
	ck_assert_str_eq(result[3][1], "'AnaTrujilloEmparedadosyhelados'"); 
 	ck_assert_str_eq(result[4][0], "10311");
	ck_assert(result[4][1] == NULL); 
	ck_assert_str_eq(result[5][0], "10312");
	ck_assert_str_eq(result[5][1], "'AnaTrujilloEmparedadosyhelados'"); 
	
	// INDEX
	createIndex("CustomerID", orders);
	char test2[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Customers RIGHT JOIN Orders ON Customers.CustomerID=Orders.CustomerID;";
	result = tokenizeJoin(test2);
	printf("\nresult[0] = %s\n", result[0][0]);
	

	ck_assert_str_eq(result[0][0], "'AlfredsFutterkiste'");
	ck_assert(result[0][1] == NULL);
	ck_assert_str_eq(result[1][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][1], "10308");
	ck_assert_str_eq(result[2][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[2][1], "10307");
	ck_assert_str_eq(result[3][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[3][1], "10310");
	ck_assert_str_eq(result[4][0], "'AnaTrujilloEmparedadosyhelados'"); 
 	ck_assert_str_eq(result[4][1], "10312");
	ck_assert_str_eq(result[5][0], "'AntonioMorenoTaquería'");
	ck_assert(result[5][1] == NULL);
	*/

} END_TEST


START_TEST(test_right_join_sequential) {
	printf("\n\n\n\nTESTING LEFT JOIN\n");
       
	tokenizeCreateTable("CREATE TABLE Orders (OrderID int, CustomerID int, EmployeeID int, PRIMARY KEY(OrderID));");
	tokenizeCreateTable("CREATE TABLE Customers (CustomerID int, CustomerName varchar(50), ContactName varchar(50), Address varchar(70));");

	Table *orders = (Table *) cfuhash_get(dataBuffer->tables, "Orders");
	Table *customers = (Table *) cfuhash_get(dataBuffer->tables, "Customers");
	ck_assert(orders);
	ck_assert(customers);

	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10307, 1, 10);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10308, 1, 7);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10309, 37, 3);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10310, 1, 13);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10311, 77, 8);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10312, 1, 5);");

	tokenizeInsertKeyword("INSERT INTO Customers VALUES(0, 'AlfredsFutterkiste', 'MariaAnder', 'ObereStr.571');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(1, 'AnaTrujilloEmparedadosyhelados', 'AnaTrujillo', 'Avda.delaConstitución2222');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(2, 'AntonioMorenoTaquería', 'MariaAnder', 'ObereStr.571');");			
	
	char ***result;
	char test1[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Orders RIGHT JOIN Customers ON Customers.CustomerID=Orders.CustomerID;";
        result = tokenizeJoin(test1);
	printf("\nresult[0][0] = %s\n", result[0][0]);
	printf("\nresult[0][1] = %s\n", result[0][1]);

	/*
	ck_assert_str_eq(result[0][0], "10307");
	ck_assert_str_eq(result[0][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][0], "10308");
	ck_assert_str_eq(result[1][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[2][0], "10309");
	ck_assert(result[2][1] == NULL);
	ck_assert_str_eq(result[3][0], "10310");
	ck_assert_str_eq(result[3][1], "'AnaTrujilloEmparedadosyhelados'"); 
 	ck_assert_str_eq(result[4][0], "10311");
	ck_assert(result[4][1] == NULL); 
	ck_assert_str_eq(result[5][0], "10312");
	ck_assert_str_eq(result[5][1], "'AnaTrujilloEmparedadosyhelados'"); 
	*/
	/*
	cfuhash_delete(dataBuffer->tables, "Orders");
	cfuhash_delete(dataBuffer->tables, "Customers");
	*/
	// TO DO
	// test opposide way (i.e. customers is the left join table)
} END_TEST


// LEFT JOIN INDEX KEY
START_TEST(test_left_join_index_key) {
	printf("\n\n\n\nTESTING LEFT JOIN INDEX KEY\n");
       
	tokenizeCreateTable("CREATE TABLE Orders (OrderID int, CustomerID int, EmployeeID int, PRIMARY KEY(OrderID));");
	tokenizeCreateTable("CREATE TABLE Customers (CustomerID int, CustomerName varchar(50), ContactName varchar(50), Address varchar(70), PRIMARY KEY(CustomerID));");

	Table *orders = (Table *) cfuhash_get(dataBuffer->tables, "Orders");
	Table *customers = (Table *) cfuhash_get(dataBuffer->tables, "Customers");
	createIndex("CustomerID", orders);
	ck_assert(hasIndex("CustomerID", orders));
	ck_assert(orders);
	ck_assert(customers);
	ck_assert(isPrimaryKey(customers, "CustomerID"));

	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10307, 1, 10);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10308, 1, 7);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10309, 37, 3);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10310, 1, 13);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10311, 77, 8);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10312, 1, 5);");

	tokenizeInsertKeyword("INSERT INTO Customers VALUES(0, 'AlfredsFutterkiste', 'MariaAnder', 'ObereStr.571');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(1, 'AnaTrujilloEmparedadosyhelados', 'AnaTrujillo', 'Avda.delaConstitución2222');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(2, 'AntonioMorenoTaquería', 'MariaAnder', 'ObereStr.571');");			

	char ***result;	
	char test2[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Customers LEFT JOIN Orders ON Customers.CustomerID=Orders.CustomerID;";
	result = tokenizeJoin(test2);
	printf("\nresult[0] = %s\n", result[0][0]);	
	ck_assert_str_eq(result[0][0], "'AlfredsFutterkiste'");
	ck_assert(result[0][1] == NULL);
	ck_assert_str_eq(result[1][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][1], "10308");
	ck_assert_str_eq(result[2][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[2][1], "10307");	
	ck_assert_str_eq(result[3][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[3][1], "10310"); 
	ck_assert_str_eq(result[4][0], "'AnaTrujilloEmparedadosyhelados'");
 	ck_assert_str_eq(result[4][1], "10312");
	ck_assert_str_eq(result[5][0], "'AntonioMorenoTaquería'");
	ck_assert(result[5][1] == NULL); 	
} END_TEST


START_TEST(test_left_join_sequential) {
	printf("\n\n\n\nTESTING LEFT JOIN\n");
       
	tokenizeCreateTable("CREATE TABLE Orders (OrderID int, CustomerID int, EmployeeID int, PRIMARY KEY(OrderID));");
	tokenizeCreateTable("CREATE TABLE Customers (CustomerID int, CustomerName varchar(50), ContactName varchar(50), Address varchar(70));");

	Table *orders = (Table *) cfuhash_get(dataBuffer->tables, "Orders");
	Table *customers = (Table *) cfuhash_get(dataBuffer->tables, "Customers");
	ck_assert(orders);
	ck_assert(customers);

	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10307, 1, 10);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10308, 1, 7);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10309, 37, 3);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10310, 1, 13);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10311, 77, 8);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10312, 1, 5);");

	tokenizeInsertKeyword("INSERT INTO Customers VALUES(0, 'AlfredsFutterkiste', 'MariaAnder', 'ObereStr.571');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(1, 'AnaTrujilloEmparedadosyhelados', 'AnaTrujillo', 'Avda.delaConstitución2222');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(2, 'AntonioMorenoTaquería', 'MariaAnder', 'ObereStr.571');");			
	
	char ***result;
	char test1[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Orders LEFT JOIN Customers ON Customers.CustomerID=Orders.CustomerID;";
        result = tokenizeJoin(test1);
	ck_assert_str_eq(result[0][0], "10307");
	ck_assert_str_eq(result[0][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][0], "10308");
	ck_assert_str_eq(result[1][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[2][0], "10309");
	ck_assert(result[2][1] == NULL);
	ck_assert_str_eq(result[3][0], "10310");
	ck_assert_str_eq(result[3][1], "'AnaTrujilloEmparedadosyhelados'"); 
 	ck_assert_str_eq(result[4][0], "10311");
	ck_assert(result[4][1] == NULL); 
	ck_assert_str_eq(result[5][0], "10312");
	ck_assert_str_eq(result[5][1], "'AnaTrujilloEmparedadosyhelados'"); 



	// TO DO
	// test opposide way (i.e. customers is the left join table)
} END_TEST


START_TEST(test_left_join_primary_key) {
	printf("\n\n\n\nTESTING LEFT JOIN\n");
       
	tokenizeCreateTable("CREATE TABLE Orders (OrderID int, CustomerID int, EmployeeID int, PRIMARY KEY(OrderID));");
	tokenizeCreateTable("CREATE TABLE Customers (CustomerID int, CustomerName varchar(50), ContactName varchar(50), Address varchar(70), PRIMARY KEY(CustomerID));");


	Table *orders = (Table *) cfuhash_get(dataBuffer->tables, "Orders");
	Table *customers = (Table *) cfuhash_get(dataBuffer->tables, "Customers");
	ck_assert(orders);
	ck_assert(customers);
	ck_assert(isPrimaryKey(customers, "CustomerID"));

	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10307, 1, 10);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10308, 1, 7);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10309, 37, 3);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10310, 1, 13);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10311, 77, 8);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10312, 1, 5);");

	tokenizeInsertKeyword("INSERT INTO Customers VALUES(0, 'AlfredsFutterkiste', 'MariaAnder', 'ObereStr.571');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(1, 'AnaTrujilloEmparedadosyhelados', 'AnaTrujillo', 'Avda.delaConstitución2222');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(2, 'AntonioMorenoTaquería', 'MariaAnder', 'ObereStr.571');");			
	
	char ***result;
	char test1[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Orders LEFT JOIN Customers ON Customers.CustomerID=Orders.CustomerID;";
        result = tokenizeJoin(test1);
	ck_assert_str_eq(result[0][0], "10307");
	ck_assert_str_eq(result[0][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][0], "10308");
	ck_assert_str_eq(result[1][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[2][0], "10309");
	ck_assert(result[2][1] == NULL);
	ck_assert_str_eq(result[3][0], "10310");
	ck_assert_str_eq(result[3][1], "'AnaTrujilloEmparedadosyhelados'"); 
 	ck_assert_str_eq(result[4][0], "10311");
	ck_assert(result[4][1] == NULL); 
	ck_assert_str_eq(result[5][0], "10312");
	ck_assert_str_eq(result[5][1], "'AnaTrujilloEmparedadosyhelados'"); 
	
	// INDEX
	createIndex("CustomerID", orders);
	char test2[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Customers LEFT JOIN Orders ON Customers.CustomerID=Orders.CustomerID;";
	result = tokenizeJoin(test2);
	printf("\nresult[0] = %s\n", result[0][0]);
	

	ck_assert_str_eq(result[0][0], "'AlfredsFutterkiste'");
	ck_assert(result[0][1] == NULL);
	ck_assert_str_eq(result[1][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][1], "10308");
	ck_assert_str_eq(result[2][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[2][1], "10307");
	ck_assert_str_eq(result[3][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[3][1], "10310");
	ck_assert_str_eq(result[4][0], "'AnaTrujilloEmparedadosyhelados'"); 
 	ck_assert_str_eq(result[4][1], "10312");
	ck_assert_str_eq(result[5][0], "'AntonioMorenoTaquería'");
	ck_assert(result[5][1] == NULL);

} END_TEST


// OUTER JOIN
START_TEST(test_outer_join_fastest) {
	printf("\n\n\n\nTESTING OUTER JOIN FASTEST\n");
       
	tokenizeCreateTable("CREATE TABLE Orders (OrderID int, CustomerID int, EmployeeID int, PRIMARY KEY(OrderID));");
	tokenizeCreateTable("CREATE TABLE Customers (CustomerID int, CustomerName varchar(50), ContactName varchar(50), Address varchar(70), PRIMARY KEY(CustomerID));");

	Table *orders = (Table *) cfuhash_get(dataBuffer->tables, "Orders");
	Table *customers = (Table *) cfuhash_get(dataBuffer->tables, "Customers");
	ck_assert(orders);
	ck_assert(customers);
	createIndex("CustomerID", orders);
	ck_assert(hasIndex("CustomerID", orders));
	ck_assert(isPrimaryKey(customers, "CustomerID"));

	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10307, 1, 10);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10308, 1, 7);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10309, 37, 3);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10310, 1, 13);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10311, 77, 8);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10312, 1, 5);");

	tokenizeInsertKeyword("INSERT INTO Customers VALUES(0, 'AlfredsFutterkiste', 'MariaAnder', 'ObereStr.571');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(1, 'AnaTrujilloEmparedadosyhelados', 'AnaTrujillo', 'Avda.delaConstitución2222');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(2, 'AntonioMorenoTaquería', 'MariaAnder', 'ObereStr.571');");			
	
	char ***result;
	char test1[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Orders OUTER JOIN Customers ON Customers.CustomerID=Orders.CustomerID;";
        result = tokenizeJoin(test1);
	ck_assert_str_eq(result[0][0], "10309");
	ck_assert_str_eq(result[0][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][0], "10307");
	ck_assert_str_eq(result[1][1], "'AnaTrujilloEmparedadosyhelados'");
} END_TEST


// INNER JOIN

START_TEST(test_inner_join_fastest) {
	printf("\nTESTING INNER JOIN FASTEST\n");
       
	tokenizeCreateTable("CREATE TABLE Orders (OrderID int, CustomerID int, EmployeeID int, PRIMARY KEY(OrderID));");
	tokenizeCreateTable("CREATE TABLE Customers (CustomerID int, CustomerName varchar(50), ContactName varchar(50), Address varchar(70), PRIMARY KEY(CustomerID));");

	Table *orders = (Table *) cfuhash_get(dataBuffer->tables, "Orders");
	Table *customers = (Table *) cfuhash_get(dataBuffer->tables, "Customers");
	ck_assert(orders);
	ck_assert(customers);
	createIndex("CustomerID", orders);
	ck_assert(hasIndex("CustomerID", orders));
	ck_assert(isPrimaryKey(customers, "CustomerID"));

	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10307, 1, 10);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10308, 1, 7);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10309, 37, 3);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10310, 1, 13);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10311, 77, 8);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10312, 1, 5);");

	tokenizeInsertKeyword("INSERT INTO Customers VALUES(0, 'AlfredsFutterkiste', 'MariaAnder', 'ObereStr.571');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(1, 'AnaTrujilloEmparedadosyhelados', 'AnaTrujillo', 'Avda.delaConstitución2222');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(2, 'AntonioMorenoTaquería', 'MariaAnder', 'ObereStr.571');");			
	
	char ***result;
	char test1[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Orders INNER JOIN Customers ON Customers.CustomerID=Orders.CustomerID;";
        result = tokenizeJoin(test1);
	ck_assert_str_eq(result[0][0], "10308");
	ck_assert_str_eq(result[0][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][0], "10307");
	ck_assert_str_eq(result[1][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[2][0], "10310");
	ck_assert_str_eq(result[2][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[3][0], "10312");
	ck_assert_str_eq(result[3][1], "'AnaTrujilloEmparedadosyhelados'");

	freeJoinResult(result);

	result = tokenizeJoin(test1);

} END_TEST


START_TEST(test_inner_join_slow) {
	printf("\nTESTING INNER JOIN SLOW\n");
       
	tokenizeCreateTable("CREATE TABLE Orders (OrderID int, CustomerID int, EmployeeID int, PRIMARY KEY(OrderID));");
	tokenizeCreateTable("CREATE TABLE Customers (CustomerID int, CustomerName varchar(50), ContactName varchar(50), Address varchar(70), PRIMARY KEY(CustomerID));");

	Table *orders = (Table *) cfuhash_get(dataBuffer->tables, "Orders");
	Table *customers = (Table *) cfuhash_get(dataBuffer->tables, "Customers");
	ck_assert(orders);
	ck_assert(customers);
	ck_assert(isPrimaryKey(customers, "CustomerID"));

	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10307, 1, 10);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10308, 1, 7);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10309, 37, 3);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10310, 1, 13);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10311, 77, 8);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10312, 1, 5);");

	tokenizeInsertKeyword("INSERT INTO Customers VALUES(0, 'AlfredsFutterkiste', 'MariaAnder', 'ObereStr.571');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(1, 'AnaTrujilloEmparedadosyhelados', 'AnaTrujillo', 'Avda.delaConstitución2222');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(2, 'AntonioMorenoTaquería', 'MariaAnder', 'ObereStr.571');");			
	
	char ***result;
	char test1[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Customers INNER JOIN Orders ON Customers.CustomerID=Orders.CustomerID;";
        result = tokenizeJoin(test1);
	ck_assert_str_eq(result[0][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[0][1], "10307");
	ck_assert_str_eq(result[1][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][1], "10308");
	ck_assert_str_eq(result[2][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[2][1], "10310");
	ck_assert_str_eq(result[3][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[3][1], "10312");

} END_TEST


START_TEST(test_inner_join_sequential_intermediate) {
	tokenizeCreateTable("CREATE TABLE Orders (OrderID int, CustomerID int, EmployeeID int);");
	tokenizeCreateTable("CREATE TABLE Customers (CustomerID int, CustomerName varchar(50), ContactName varchar(50), Address varchar(70));");

	Table *orders = (Table *) cfuhash_get(dataBuffer->tables, "Orders");
	Table *customers = (Table *) cfuhash_get(dataBuffer->tables, "Customers");
	ck_assert(orders);
	ck_assert(customers);
	
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10307, 1, 10);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10308, 1, 7);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10309, 37, 3);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10310, 1, 13);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10311, 77, 8);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10312, 1, 5);");

	tokenizeInsertKeyword("INSERT INTO Customers VALUES(0, 'AlfredsFutterkiste', 'MariaAnder', 'ObereStr.571');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(1, 'AnaTrujilloEmparedadosyhelados', 'AnaTrujillo', 'Avda.delaConstitución2222');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(2, 'AntonioMorenoTaquería', 'MariaAnder', 'ObereStr.571');");			
	
	char ***result;
	char test1[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Orders INNER JOIN Customers ON Customers.CustomerID=Orders.CustomerID;";
        result = tokenizeJoin(test1);
	ck_assert_str_eq(result[0][0], "10307");
	ck_assert_str_eq(result[0][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[1][0], "10308");
	ck_assert_str_eq(result[1][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[2][0], "10310");
	ck_assert_str_eq(result[2][1], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[3][0], "10312");
	ck_assert_str_eq(result[3][1], "'AnaTrujilloEmparedadosyhelados'");
} END_TEST


START_TEST(test_inner_join_sequential) {
	printf("\nTESTING INNER JOIN SEQUENTIAL\n");
       
	tokenizeCreateTable("CREATE TABLE Orders (OrderID int, CustomerID int, EmployeeID int);");
	tokenizeCreateTable("CREATE TABLE Customers (CustomerID int, CustomerName varchar(50), ContactName varchar(50), Address varchar(70));");

	Table *orders = (Table *) cfuhash_get(dataBuffer->tables, "Orders");
	Table *customers = (Table *) cfuhash_get(dataBuffer->tables, "Customers");
	ck_assert(orders);
	ck_assert(customers);

	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10308, 2, 7);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10309, 37, 3);");
	tokenizeInsertKeyword("INSERT INTO Orders VALUES(10310, 77, 8);");

	tokenizeInsertKeyword("INSERT INTO Customers VALUES(1, 'AlfredsFutterkiste', 'MariaAnder', 'ObereStr.571');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(2, 'AnaTrujilloEmparedadosyhelados', 'AnaTrujillo', 'Avda.delaConstitución2222');");
	tokenizeInsertKeyword("INSERT INTO Customers VALUES(3, 'AntonioMorenoTaquería', 'MariaAnder', 'ObereStr.571');");
	
	char ***result;

	char test1[] = "SELECT Customers.CustomerName, Orders.OrderID FROM Customers INNER JOIN Orders ON Customers.CustomerID=Orders.CustomerID;";
        result = tokenizeJoin(test1);
	ck_assert_str_eq(result[0][0], "'AnaTrujilloEmparedadosyhelados'");
	ck_assert_str_eq(result[0][1], "10308");
} END_TEST




/*

START_TEST(test_add_constraint_foreign_key) {
        printf("\nTESTING Constraint Foreign Key\n");

        char *table_name2 = "test_table2";

        char *column_names2[4];
        column_names2[0] = "LAST_NAME";
        column_names2[1] = "HEIGHT";
        column_names2[2] = "MOTHERS_MAIDEN_NAME";
        column_names2[3] = "TELEPHONE_NO";

        char *data_types2[4];
        data_types2[0] = "VARCHAR";
        data_types2[1] = "INT";
        data_types2[2] = "VARCHAR";
        data_types2[3] = "VARCHAR";

        int number_of_fields2 = 4;

        //createTable(table_name2, column_names2, data_types2, number_of_fields2);
        create(table_name2, column_names2, data_types2, number_of_fields2);

        Table *table2 = (Table *) cfuhash_get(dataBuffer->tables, table_name2);
        //createFormat(table2, column_names2, data_types2, number_of_fields2);

        // ADD FOREIGN KEY
        char *foreign_keys[5];
        foreign_keys[0] = column_names1[1]; // name of the column within the original table

        char *foreign_key_names[5];
        foreign_key_names[0] = column_names2[1];
        char *foreign_key_tables[5];
        foreign_key_tables[0] = table_name2;
        int number_of_foreign_keys = 1;


        printf("\nhere5\n");
        addConstraintForeignKeys(table_name1, number_of_foreign_keys, foreign_keys, foreign_key_names, foreign_key_tables);
        //addConstraintForeignKey(table_name1, table_name2, "HEIGHT");
printf("\nhere5\n");

        ck_assert(table1->format->number_of_foreign_keys == 1);
        ck_assert(table1->format->foreign_keys[0]->field == table1->format->fields[locateField(table1->format, column_names1[1])]);
        ck_assert(table1->format->foreign_keys[0]->table == table2);
        ck_assert(table1->format->foreign_keys[0]->origin_field == table2->format->fields[locateField(table2->format, column_names2[1])]);

} END_TEST



START_TEST(test_add_constraint_primary_key) {
        printf("\nTESTING Constraint Primary Key\n");

        int number_of_primary_keys = 0;

        // ADD FOREIGN KEY
        char *primary_keys[5];
        primary_keys[0] = column_names1[1];
        number_of_primary_keys++;
        primary_keys[1] = column_names1[2];
        number_of_primary_keys++;

        addConstraintPrimaryKeys(table_name1, number_of_primary_keys, primary_keys);

        ck_assert(table1->format->number_of_primary_keys == number_of_primary_keys);
        ck_assert(table1->format->primary_keys[0] == table1->format->fields[locateField(table1->format, column_names1[1])]);
        ck_assert(table1->format->primary_keys[1] == table1->format->fields[locateField(table1->format, column_names1[2])]);

} END_TEST
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
	TCase *tc_select;
	TCase *tc_update;
	TCase *tc_drop;
	TCase *tc_join;
	
	s = suite_create("SQL Parser");

	
	tc_tokenize = tcase_create("Tokenize");
	tcase_add_test(tc_tokenize, test_tokenize_keyword);

	// Create test case
	tc_create = tcase_create("Create Table/Database");
	tcase_add_test(tc_create, test_create);

	tc_insert = tcase_create("Inserting");
	tcase_add_test(tc_insert, test_insert);
	tcase_add_checked_fixture(tc_insert, setup, teardown);

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
	tcase_add_test(tc_delete, test_delete_index_advanced);	
	tcase_add_checked_fixture(tc_delete, setup, teardown);

	tc_select = tcase_create("Select");
	tcase_add_test(tc_select, test_select_basic);
	tcase_add_checked_fixture(tc_select, setup2, teardown);

	tc_update = tcase_create("Update");
	tcase_add_test(tc_update, test_update_record);
	tcase_add_checked_fixture(tc_update, setup2, teardown);

	tc_alter = tcase_create("Altering");
	tcase_add_test(tc_alter, test_alter_add);
	tcase_add_test(tc_alter, test_alter_drop_column);
	tcase_add_test(tc_alter, test_alter_rename_column);
	tcase_add_checked_fixture(tc_alter, setup2, teardown);

	tc_drop = tcase_create("Dropping");
	tcase_add_test(tc_drop, test_drop_table);

	tc_join = tcase_create("Joins");
	tcase_add_test(tc_join, test_inner_join_sequential);
	tcase_add_test(tc_join, test_inner_join_sequential_intermediate);
	tcase_add_test(tc_join, test_inner_join_fastest);
	tcase_add_test(tc_join, test_inner_join_slow);
	tcase_add_test(tc_join, test_left_join_sequential);
	tcase_add_test(tc_join, test_left_join_primary_key);
	tcase_add_test(tc_join, test_left_join_index_key);
	tcase_add_test(tc_join, test_right_join_sequential);
	tcase_add_test(tc_join, test_right_join_primary_key);
	tcase_add_test(tc_join, test_right_join_index_key);
	tcase_add_checked_fixture(tc_join, setup2, teardown);

	// Add test cases to suite 
	suite_add_tcase(s, tc_tokenize);
	suite_add_tcase(s, tc_create);
	suite_add_tcase(s, tc_insert);
	suite_add_tcase(s, tc_build_stack);
	suite_add_tcase(s, tc_build_expression_tree);
	suite_add_tcase(s, tc_select);
	suite_add_tcase(s, tc_update);
	suite_add_tcase(s, tc_alter);
	suite_add_tcase(s, tc_drop);
	suite_add_tcase(s, tc_delete);
	suite_add_tcase(s, tc_join);

	return s;
}


int main(void)
{
   int number_failed;
   Suite *s;
   SRunner *sr;

   s = storage_suite();
   sr = srunner_create(s);
   srunner_set_fork_status(sr, CK_NOFORK);	
   srunner_run_all(sr, CK_NORMAL);
   number_failed = srunner_ntests_failed(sr);
   srunner_free(sr);
   return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
