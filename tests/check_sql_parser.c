#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include "../src/server/access/sql_parser.h"
//#include "../src/server/access/sqlaccess.h"
#include "../libs/libcfu/src/cfuhash.h"


char *database_name1;
char *table_name1;
char *join_table1;
Table *table1;
DataBuffer *dataBuffer;


void util_createDatabase(){	
	char query[100] = "CREATE DATABASE ";
	strcpy(query, database_name1);
	strcpy(query, ";");
	tokenizeCreateDatabase(query);
}


void util_createTable() {
	char query[100] = "CREATE TABLE ";
	strcpy(query, table_name1);
	strcpy(query, " (PersonID int, LastName varchar(255), FirstName varchar(255), Gender char;");
	tokenizeCreateTable(query);
}

void util_freeDataBuffer() {
        cfuhash_destroy (dataBuffer->tables);
        free(dataBuffer);
}



void setup() {

	database_name1 = "test_database";
	table_name1 = "Persons";
        
	initialize();
   
	util_createDatabase(); 	
	util_createTable();

	dataBuffer = initializeDataBuffer();      
        table1 = (Table *) cfuhash_get(dataBuffer->tables, table_name1);
}


void teardown() {
        drop(table_name1);
	util_freeDataBuffer();	
	deleteDatabase("test_database");
}



START_TEST(test_build_stack) {

        char expression1[] = "A * (B + C * D) + E";
        char expected1[] = "A B C D * + * E +";
        char expression2[] = "A * B ^ C + D";
        char expression3[] = "A - B + C";
        char expression4[] = "3 + 4 * 2 / ( 1 - 5 ) ^ 2 ^ 3";
        char expression5[] = "3 + 4 AND 4 + 3";
        char expression6[] = "first_name = 'Conor' AND age = 40";
        char expression7[] = "last_name = 'PHILIP' OR (first_name = 'Conor' AND age = 40)";
        char expression8[] = "(last_name = 'PHILIP' OR first_name = 'Conor') AND age = 40";
        char expression9[] = "last_name = 'PHILIP' OR (first_name = 'Conor' AND age >= 40)";


        printf("\n\n\n\nTESTING");
        Stack *result1 = buildStack(expression1);
        // ck_assert(strcmp(result1, expected1) == 0)
        printf("\n%s\n", expression1);
        printStack(result1);



        printf("\n\n\n\nTESTING");
        Stack *result2 = buildStack(expression2);
        printf("\n%s\n", expression2);
        printStack(result2);



        printf("\n\n\n\nTESTING");
        Stack *result4 = buildStack(expression4);
        printf("\n%s\n", expression4);
        printStack(result4);

        /*
        printf("\n\n\n\nTESTING");
        Stack *result5 = buildStack(expression5);
        printf("\n%s\n", expression5);
        printStack(result5);


        printf("\n\n\n\nTESTING");
        Stack *result6 = buildStack(expression6);
        printf("\n%s\n", expression6);
        printStack(result6);


        printf("\n\n\n\nTESTING");
        Stack *result7 = buildStack(expression7);
        printf("\n%s\n", expression7);
        printStack(result7);


        printf("\n\n\n\nTESTING");
        Stack *result8 = buildStack(expression8);
        printf("\n%s\n", expression8);
        printStack(result8);
	
	printf("\n\n\n\nTESTING");
        Stack *result9 = buildStack(expression9);
        printf("\n%s\n", expression9);
        printStack(result9);
        */
} END_TEST



START_TEST (test_insert) {
	printf("\nTESTING INSERT\n");

        char test1[] = "INSERT INTO table_name VALUES(value1,value2,value3);";
        char test2[] = "INSERT INTO table_name (column1, column2, column3) VALUES ( value1 , value2 , value3 );";

        tokenizeInsertKeyword(test2);

	// ck_assert data was inserted

	// ck_assert indexes created
} END_TEST




void testAlterRename(){

        // rename column
        char test1[] = "ALTER TABLE table_name CHANGE COLUMN old_name TO new_name;";
        tokenizeAlterKeyword(test1);

        // rename table
        char test2[] = "ALTER TABLE supplier RENAME TO vendor;";
        tokenizeAlterKeyword(test2);
}


void testAlterDrop() {
        char test1[] = "ALTER TABLE table_name DROP COLUMN column_name;";
        tokenizeAlterKeyword(test1);
        char test2[] = "ALTER TABLE table_name DROP COLUMN  (column_name1,column_name2);";
        tokenizeAlterKeyword(test2);
}


void testAlterAdd() {
        char test1[] = "ALTER TABLE supplier ADD supplier_name char(50);";
        tokenizeAlterKeyword(test1);
        char test2[] = "ALTER TABLE table_name ADD (column_1 char(257)  , column_2 INT , column_3 CHAR )  ;";
        tokenizeAlterKeyword(test2);
}


START_TEST (test_alter) {
        printf("\nTESTING ALTER\n");

	testAlterAdd();

        testAlterDrop();

        testAlterRename();
} END_TEST




START_TEST (test_delete) {

	printf("\nTESTING DELETE\n");

        char test1[] = "DELETE FROM table_name WHERE some_column=some_value;";
        tokenizeDeleteKeyword(test1);
        char test2[] = "DELETE FROM Customers WHERE CustomerName  =   'Alfreds Futterkiste'  AND    ContactName  ='Maria Anders'   ;";
        tokenizeDeleteKeyword(test2);
        char test3[] = "DELETE * FROM Users;";
        tokenizeDeleteKeyword(test3);
} END_TEST




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





void testCreateDatabase() {
	printf("\nTESTING CREATE DATABASE\n");

        char test1[] = "CREATE DATABASE dbname;";
        tokenizeCreateDatabase(test1);

        char test2[] = "CREATE DATABASE my_db;";
        tokenizeCreateDatabase(test2);
}

void testCreateTable() {
	printf("\nTESTING CREATE TABLE\n");

        char test2[] = "CREATE TABLE Persons (PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(256));";
        tokenizeCreateTable(test2);

	// ck_assert
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
	char test0[] = "SELECT first_name, last_name FROM Customers, Users WHERE age = 20;";
        
        int result;
 
	result = tokenizeKeywords(test0);
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

	s = suite_create("SQL Parser");

	tc_tokenize = tcase_create("Tokenize");
	tcase_add_test(tc_tokenize, test_tokenize_keyword);

	/* Create test case */
	tc_create = tcase_create("Create Table");
	tcase_add_test(tc_create, test_create);
	
	/*
	tc_select = tcase_create("Select Record");
        tcase_add_test(tc_select, test_select_record);
	tcase_add_checked_fixture(tc_select, setup, teardown);
	*/


	/* Add test cases to suite */
	suite_add_tcase(s, tc_create);
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
