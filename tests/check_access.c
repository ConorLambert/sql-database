#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include "../src/server/access/sqlaccess.h"
#include "../libs/libcfu/src/cfuhash.h"
#include "../libs/libbtree/btree.h"


char *test_database;

DataBuffer *dataBuffer;

Table *table1;

Index *index1;

char *table_name1;
char *index_name1;

char *field_first_name1;
char *field_age1;
char *field_date_of_birth1;
char *field_telephone_no1;
char **fields1;

char *first_name1;
char *age1;
char *date_of_birth1;
char *telephone_no1;
char **data1;

char *first_name2;
char *age2;
char *date_of_birth2;
char *telephone_no2;
char **data2;

char *first_name3;
char *age3;
char *date_of_birth3;
char *telephone_no3;
char **data3;

int number_of_fields1;


void util_createTable1(){
	table_name1 = malloc(strlen("test_table1") + 1);
	strcpy(table_name1, "test_table1");
}


void util_createFields1() {
	field_first_name1 = malloc(strlen("VARCHAR FIRST_NAME") + 1);
        strcpy(field_first_name1, "VARCHAR FIRST_NAME");
        field_age1 = malloc(strlen("INT AGE") + 1);
        strcpy(field_age1, "INT AGE");
        field_date_of_birth1 = malloc(strlen("VARCHAR DATE_OF_BIRTH") + 1);
        strcpy(field_date_of_birth1, "VARCHAR DATE_OF_BIRTH");
        field_telephone_no1 = malloc(strlen("VARCHAR TELEPHONE_NO") + 1);
        strcpy(field_telephone_no1, "VARCHAR TELEPHONE_NO");
        number_of_fields1 = 4;

        fields1 = malloc(number_of_fields1 * sizeof(char *));
        fields1[0] = field_first_name1;
        fields1[1] = field_age1;
        fields1[2] = field_date_of_birth1;
        fields1[3] = field_telephone_no1;
//printf("\nfields1[0] = %s, fields1[1] = %s, fields1[2] = %s, fields1[3] = %s\n", fields1[0], fields1[1], fields1[2], fields1[3]);
        create(table_name1, fields1, number_of_fields1);


}


void util_createAndInsertRecord1() {
	// create and insert a record 1
        first_name1 = malloc(strlen("Conor") + 1);
        strcpy(first_name1, "Conor");
        age1 = malloc(strlen("33") + 1);
        strcpy(age1, "33");
        date_of_birth1 = malloc(strlen("12-05-1990") + 1);
        strcpy(date_of_birth1, "12-05-1990");
        telephone_no1 = malloc(strlen("086123456") + 1);
        strcpy(telephone_no1, "086123456");
        data1 = malloc(4 * sizeof(char *));
        data1[0] = first_name1;
        data1[1] = age1;
        data1[2] = date_of_birth1;
        data1[3] = telephone_no1;

printf("\ninserting\n");
	insert(data1, number_of_fields1, table_name1, test_database); // INSERT
}

void util_createAndInsertRecord2() {
	first_name2 = malloc(strlen("Damian") + 1);
        strcpy(first_name2, "Damian");
        age2 = malloc(strlen("44") + 1);
        strcpy(age2, "44");
        date_of_birth2 = malloc(strlen("05-09-1995") + 1);
        strcpy(date_of_birth2, "05-09-1995");
        telephone_no2 = malloc(strlen("086654321") + 1);
        strcpy(telephone_no2, "086654321");
        data2 = malloc(4 * sizeof(char *));
        data2[0] = first_name2;
        data2[1] = age2;
        data2[2] = date_of_birth2;
        data2[3] = telephone_no2;
	
	insert(data2, number_of_fields1, table_name1, test_database); 
}

void util_createAndInsertRecord3() {
	first_name3 = malloc(strlen("Freddie") + 1);
        strcpy(first_name3, "Freddie");
        age3 = malloc(strlen("55") + 1);
        strcpy(age3, "55");
        date_of_birth3 = malloc(strlen("24-02-1965") + 1);
        strcpy(date_of_birth3, "24-02-1965");
        telephone_no3 = malloc(strlen("08624681") + 1);
        strcpy(telephone_no3, "08624681");
        data3 = malloc(4 * sizeof(char *));
        data3[0] = first_name3;
        data3[1] = age3;
        data3[2] = date_of_birth3;
        data3[3] = telephone_no3;

	insert(data3, number_of_fields1, table_name1, test_database);
}


// returns rid of error insertion
int util_testCorrectness(Table *table, Index *index, char *index_name, char *data[], char *fields[], int number_of_fields) {
	
	int errorid = 0;
	
	// counter
	int i, j, k;
	
printf("\nin util test correctness\n");
	// test record

	 printf("\n\t\t\tnumber_of_records\n");
	// get the last record inserted
	int number_of_pages = table->number_of_pages;
	int number_of_records = table->pages[number_of_pages - 1]->number_of_records;
	printf("\n\t\t\tnumber_of_records = %d\n", table->pages[number_of_pages - 1]->number_of_records);
	Record * record = table->pages[number_of_pages - 1]->records[number_of_records - 1];

	for(i = 0; i < number_of_fields; ++i) {
		printf("\n\t\trecord = %s\n", record->data[i]);
		ck_assert(strcmp(record->data[i], data[i]) == 0);
		printf("\n\t\trecord->data[%d] = %s, data[%d] = %s\n", i, record->data[i], i, data[i]);
	}
		
	int rid = number_of_records - 1;
	printf("\n\t\t\trid = %d\n", rid);
	int slot_number = (number_of_records - 1);
	printf("\n\t\t\tSlot number = %d\n", slot_number);
	int page_number = number_of_pages - 1;
	printf("\n\t\t\tPage number = %d\n", page_number);

	// test record key
	bt_key_val *key_val = btree_search(table->header_page->b_tree, &rid);
        ck_assert(*(int *)key_val->key == rid);
        printf("\n\t\t\tRecordKey->key_val->val slot_number = %d\n", ((RecordKeyValue *) (key_val->val))->slot_number);
	printf("\n\t\t\tRecordKey->key_val->val page_number = %d\n", ((RecordKeyValue *) (key_val->val))->page_number);
	ck_assert(((RecordKeyValue *) (key_val->val))->slot_number == slot_number);
	ck_assert(((RecordKeyValue *) (key_val->val))->page_number == page_number);
	
	// test index
	ck_assert(index->b_tree != NULL);
        ck_assert(strcmp(index->index_name, "FIRST_NAME") == 0);
	// test for index insertion
        ck_assert(table->indexes->number_of_indexes == 1);
        ck_assert(table->indexes->indexes[table->indexes->number_of_indexes - 1] == index);

	// test index key
	// test for insertion
	printf("\n\t\t%s\n", data[0]);
	bt_key_val * b_tree_key_val1 = btree_search(index->b_tree, data[0]);
	printf("\n\t\t\tkey_val->key = %s, val = %d\n", (char *) b_tree_key_val1->key, * (int *)b_tree_key_val1->val);
        ck_assert(strcmp(b_tree_key_val1->key, data[0]) == 0);
        ck_assert(* (int *)b_tree_key_val1->val == rid);

	return errorid;
}


void setup(void) {
        test_database = malloc(strlen("test_database") + 1);
        strcpy(test_database, "test_database");
        util_createDatabase();
        dataBuffer = initializeDataBuffer();
        // create a table

	util_createTable1();
   	/*
	table_name1 = malloc(strlen("test_table1") + 1);
        strcpy(table_name1, "test_table1");
	*/

	util_createFields1();
        
	/*
	field_first_name1 = malloc(strlen("VARCHAR FIRST_NAME") + 1);
        strcpy(field_first_name1, "VARCHAR FIRST_NAME");
        field_age1 = malloc(strlen("INT AGE") + 1);
        strcpy(field_age1, "INT AGE");
        field_date_of_birth1 = malloc(strlen("VARCHAR DATE_OF_BIRTH") + 1);
        strcpy(field_date_of_birth1, "VARCHAR DATE_OF_BIRTH");
        field_telephone_no1 = malloc(strlen("VARCHAR TELEPHONE_NO") + 1);
        strcpy(field_telephone_no1, "VARCHAR TELEPHONE_NO");
        number_of_fields1 = 4;

        fields1 = malloc(number_of_fields1 * sizeof(char *));
        fields1[0] = field_first_name1;
        fields1[1] = field_age1;
        fields1[2] = field_date_of_birth1;
        fields1[3] = field_telephone_no1;
printf("\nfields1[0] = %s, fields1[1] = %s, fields1[2] = %s, fields1[3] = %s\n", fields1[0], fields1[1], fields1[2], fields1[3]);
        create(table_name1, fields1, number_of_fields1);
	*/

printf("\nafter create\n");
        table1 = (Table *) cfuhash_get(dataBuffer->tables, table_name1);

        createFormat(table1, fields1, number_of_fields1);
printf("\nafter format\n");

        index_name1 = "FIRST_NAME";
        index1 = createIndex(index_name1, table1);
printf("\nafter create index\n");


	util_createAndInsertRecord1(); 
             
        // create and insert a record 2
        util_createAndInsertRecord2();
      
	// create and insert a record 3
        util_createAndInsertRecord3();
}


void teardown(void) {
	printf("\nbefore drop table\n");
        drop(table_name1);
	printf("\ndropped table\n");
	util_freeDataBuffer(dataBuffer);
	printf("\nfreeing buffer\n");
        util_deleteDatabase();
	printf("\n\delewted database\n");
	
}



// UTLITY FUNCTIONS
Table * util_createTable(char *table_name) {
	return createTable(table_name);
}


void util_testFormat(char **fields, int number_of_fields, Table *table) {
	int i;
        for(i = 0; i < number_of_fields; ++i){
                char *type = strtok(fields[i], " ");
                char *name = strtok(NULL, " ");
                ck_assert(strcmp(table->format->fields[i]->type, type) == 0);
                ck_assert(strcmp(table->format->fields[i]->name, name) == 0);
        }
}

void util_createDatabase(){
	createDatabase("test_database");		
}


void util_deleteDatabase(){	
	deleteDatabase("test_database");
}


void util_deleteTestFile() {
	system("rm test_serialize.csd");
}

void util_freeDataBuffer() {
	cfuhash_destroy (dataBuffer->tables);
	free(dataBuffer);
}


// TESTS

START_TEST(test_create_and_delete_database) {
	printf("\nTESTING Creating and Deleting Databases \n");

	util_createDatabase();

	// check if the directory has been created
	char *folder = "data/test_database";
    	struct stat sb;
	stat(folder, &sb);		
    	ck_assert(stat(folder, &sb) == 0 && S_ISDIR(sb.st_mode));

	// create tables to store in that database
	dataBuffer = initializeDataBuffer();
	util_createTable("test_table1.csd");
	util_createTable("test_table1.csi");
	util_createTable("test_table1.csf");

     	util_createTable("test_table2.csd");
	util_createTable("test_table2.csi");
	util_createTable("test_table2.csf");

	util_createTable("test_table3.csd");
	util_createTable("test_table3.csi");
	util_createTable("test_table3.csf");

	// delete directory before finishing tests
	util_deleteDatabase();
	ck_assert(stat(folder, &sb) == -1);

	util_freeDataBuffer();
} END_TEST


START_TEST(test_create) {
	printf("\nTESTING Create Table\n");
	
	dataBuffer = initializeDataBuffer();

	util_createTable1;

	util_createFields1();
	ck_assert(cfuhash_exists(dataBuffer->tables, table_name1));

	util_freeDataBuffer(dataBuffer);
} END_TEST




START_TEST(test_insert) {

	printf("\nTESTING insert\n");
	
	// create the database
	util_createDatabase();
	dataBuffer = initializeDataBuffer();

	util_createTable1();

	util_createFields1();

	table1 = (Table *) cfuhash_get(dataBuffer->tables, table_name1);

        createFormat(table1, fields1, number_of_fields1);
printf("\nafter format\n");

        index_name1 = "FIRST_NAME";
        index1 = createIndex(index_name1, table1);
printf("\nafter create index\n");

	util_createAndInsertRecord1();
printf("\nafter record1\n");
	util_testCorrectness(table1, index1, index_name1, data1, fields1, number_of_fields1);		

	util_createAndInsertRecord2();
printf("\nafter record2\n");
	util_testCorrectness(table1, index1, index_name1, data2, fields1, number_of_fields1);

	util_createAndInsertRecord3();	
printf("\nafter record3\n");
        util_testCorrectness(table1, index1, index_name1, data3, fields1, number_of_fields1);	
	
	teardown();
} END_TEST



START_TEST(test_select_record) {
	
	printf("\nTESTING Select Record\n");
	
	// SEARCH
	// index search
	char *result1 = selectRecord("test_database", table_name1, "AGE", "FIRST_NAME", first_name2);
	printf("\n\t\tActual Result = %s Expected Result = %s, SELECT AGE FROM %s WHERE FIRST_NAME = %s\n", result1, age2, table_name1, first_name2);
	ck_assert(strcmp(result1, age2) == 0);
	

	// sequential search
	char *result2 = selectRecord("test_database", table_name1, "TELEPHONE_NO", "AGE", age3);
	printf("\n\t\tActual Result = %s Expected Result = %s, SELECT TELEPHONE_NO FROM %s WHERE AGE = %s\n", result2, telephone_no3, table_name1, age3);
	ck_assert(strcmp(result2, telephone_no3) == 0);


	// sequential search
	char *result3 = selectRecord("test_database", table_name1, "FIRST_NAME", "TELEPHONE_NO", telephone_no3);
	printf("\n\t\tActual Result = %s Expected Result = %s, SELECT FIRST_NAME FROM %s WHERE TELEPHONE_NO = %s\n", result3, first_name3, table_name1, telephone_no3);
	ck_assert(strcmp(result3, first_name3) == 0);


	// search for non-existent record
	ck_assert(selectRecord("test_database", table_name1, "FIRST_NAME", "TELEPHONE_NO", "123456") == NULL);

} END_TEST



START_TEST(test_delete_record){
	printf("\nTESTING Delete Record\n");

      	// before
	ck_assert(findRecordKey(table1, 1) != NULL);
	ck_assert(findIndexKey(index1, first_name2) != NULL);
	// after delete from index column - Damian
	printf("\nbefore delete record\n");
	deleteRecord("test_database", table_name1, "FIRST_NAME", first_name2);
	//ck_assert(table->pages[0]->records[1] == NULL);
	ck_assert(findRecordKey(table1, 1) == NULL);
	ck_assert(findIndexKey(index1, first_name2) == NULL);


	// delete from non-index column - Age = 33
	ck_assert(findRecordKey(table1, 0) != NULL);
        ck_assert(findIndexKey(index1, first_name1) != NULL);
	// after
	printf("\nbefore delete record\n");
	deleteRecord("test_database", table_name1, "AGE", age1);
	//ck_assert(table->pages[0] == NULL);
	ck_assert(findRecordKey(table1, 0) == NULL);
        ck_assert(findIndexKey(index1, first_name1) == NULL);


	printf("\nbefore delete record\n");

	
	// delete from non-index column - Age = 33
	printf("\n\t\t\tTesting pages\n");
	printf("\n\t\t\tTesting number of records\n");
	ck_assert(findRecordKey(table1, 2) != NULL);

	printf("\n\t\t\tTesting find record key\n");
        ck_assert(findIndexKey(index1, first_name3) != NULL);
	printf("\n\t\t\tTesting find Index key\n");

	
	// after
	deleteRecord("test_database", table_name1, "TELEPHONE_NO", telephone_no3);
	printf("\n\t\t\tAfter deletion\n");
	//ck_assert(table->pages[1] == NULL);
	ck_assert(findRecordKey(table1, 2) == NULL);
        ck_assert(findIndexKey(index1, telephone_no3) == NULL);

	

	// delete non-existent record
	printf("\n\t\t\tJust before non-existent delete\n");
	ck_assert(deleteRecord("test_database", table_name1, "TELEPHONE_NO", "1234567") == -1);

}END_TEST




START_TEST(test_delete_table) {
        printf("\nTESTING Delete Table\n");

        // DELETE
        drop(table_name1);

        ck_assert(!cfuhash_exists(dataBuffer->tables, table_name1));
       
} END_TEST


START_TEST(test_alter_record){
	
	printf("\nTESTING Alter Record\n");
	
	char *new_value = "66";
	alterRecord("test_database", table_name1, "AGE", new_value, "rid", "2");
	Record *record = table1->pages[0]->records[2];
	printf("\nrecord->age = %s\n", record->data[1]);
	ck_assert(strcmp(record->data[1], new_value) == 0);
	ck_assert(strcmp(data3[1], new_value) == 0);

} END_TEST


START_TEST (test_alter_column_change_name) {
	printf("\nTESTING Alter Column Change Name\n");
        
	alterTableChangeColumn("test_database", table_name1, "AGE", "HEIGHT");
	ck_assert(strcmp(table1->format->fields[1]->name, "HEIGHT") == 0);
	ck_assert(strcmp(table1->format->fields[1]->type, "INT") == 0);

} END_TEST


START_TEST (test_alter_column_add_column) {
	printf("\nTESTING Alter Add Column\n");
       
	alterTableAddColumn("test_database", table_name1, "NEW_COLUMN", "INT");	
	
	ck_assert(table1->format->number_of_fields == 5);
	ck_assert(strcmp(table1->format->fields[4]->name, "NEW_COLUMN") == 0);
	ck_assert(strcmp(table1->format->fields[4]->type, "INT") == 0);
} END_TEST


START_TEST (test_alter_delete_column) {
	
	printf("\nTESTING Alter Delete Column\n"); 
        
	alterTableDeleteColumn("test_database", table_name1, "AGE");
	ck_assert(table1->format->number_of_fields == 3);
	ck_assert(strcmp(table1->format->fields[1]->name, "DATE_OF_BIRTH") == 0);
	ck_assert(strcmp(table1->format->fields[2]->name, "TELEPHONE_NO") == 0);
	ck_assert(table1->format->fields[3] == NULL);
 
	
	printf("\n%s, %s, %s\n", data1[1], data1[2], data1[3]);
	ck_assert(strcmp(data1[1], "12-05-1990") == 0);
	ck_assert(strcmp(data1[2], "086123456") == 0);
	ck_assert(data1[3] == NULL);		
	
	printf("\n%s, %s, %s\n", data2[1], data2[2], data2[3]);
	ck_assert(strcmp(data2[1], "05-09-1995") == 0);
	ck_assert(strcmp(data2[2], "086654321") == 0);
	ck_assert(data2[3] == NULL);		

	printf("\n%s, %s, %s\n", data3[1], data3[2], data3[3]);
	ck_assert(strcmp(data3[1], "24-02-1965") == 0);
	ck_assert(strcmp(data3[2], "08624681") == 0);
	ck_assert(data3[3] == NULL);		
} END_TEST


START_TEST(test_add_constraint_foreign_key) {
	printf("\nTESTING Constraint Foreign Key\n"); 

        char *table_name2 = "test_table2";

	char *field_first_name2 = malloc(strlen("VARCHAR LAST_NAME") + 1);
        strcpy(field_first_name2, "VARCHAR LAST_NAME");
        char *field_age2 = malloc(strlen("INT HEIGHT") + 1);
        strcpy(field_age2, "INT HEIGHT");
        char *field_date_of_birth2 = malloc(strlen("VARCHAR MOTHERS_MAIDEN_NAME") + 1);
        strcpy(field_date_of_birth2, "VARCHAR MOTHERS_MAIDEN_NAME");
        char *field_telephone_no2 = malloc(strlen("VARCHAR TELEPHONE_NO") + 1);
        strcpy(field_telephone_no2, "VARCHAR TELEPHONE_NO");
        int number_of_fields2 = 4;

        char **fields2 = malloc(number_of_fields2 * sizeof(char *));
        fields2[0] = field_first_name2;
        fields2[1] = field_age2;
        fields2[2] = field_date_of_birth2;
        fields2[3] = field_telephone_no2;
//printf("\nfields1[0] = %s, fields1[1] = %s, fields1[2] = %s, fields1[3] = %s\n", fields1[0], fields1[1], fields1[2], fields1[3]);
        create(table_name2, fields2, number_of_fields2);

printf("\nhere2\n");

        Table *table2 = (Table *) cfuhash_get(dataBuffer->tables, table_name2);
printf("\nhere3\n");

	createFormat(table2, fields2, number_of_fields2);

printf("\nhere4\n");

	// ADD FOREIGN KEY
	addConstraintForeignKey(table_name1, table_name2, "HEIGHT");
printf("\nhere5\n");

	ck_assert(table1->format->number_of_foreign_keys == 1);
	ck_assert(table1->format->foreign_keys[0]->field == table2->format->fields[locateField(table2->format, "HEIGHT")]);		
	ck_assert(table1->format->foreign_keys[0]->table == table2);

} END_TEST


Suite * storage_suite(void)
{
	Suite *s;
	TCase *tc_create;
	TCase *tc_create_and_delete_database;
	TCase *tc_insert;
	TCase *tc_select;
	TCase *tc_delete;
	TCase *tc_alter;
	TCase *tc_foreign_key;
	

	s = suite_create("SQL Access");

	/* Create test case */
	tc_create = tcase_create("Create Table");
	tcase_add_test(tc_create, test_create);
	
	tc_create_and_delete_database = tcase_create("Create and Delete Database");
	tcase_add_test(tc_create_and_delete_database, test_create_and_delete_database);

	tc_insert = tcase_create("Insert Data");
	tcase_add_test(tc_insert, test_insert);
	
	/* Select test case */
	tc_select = tcase_create("Select Record");
        tcase_add_test(tc_select, test_select_record);
	tcase_add_checked_fixture(tc_select, setup, teardown);

	/* Alter test case*/
	tc_alter = tcase_create("Alter");
        tcase_add_test(tc_alter, test_alter_record);
	tcase_add_test(tc_alter, test_alter_delete_column);
	tcase_add_test(tc_alter, test_alter_column_add_column);
	tcase_add_test(tc_alter, test_alter_column_change_name);
	tcase_add_checked_fixture(tc_alter, setup, teardown);

	/* Delete test case */
	tc_delete = tcase_create("Delete");
        tcase_add_test(tc_delete, test_delete_record);
	tcase_add_test(tc_delete, test_delete_table);
	tcase_add_checked_fixture(tc_delete, setup, teardown);

	/* Constraint Foreign Key */
	tc_foreign_key = tcase_create("Constraint Foreign Key");
        tcase_add_test(tc_foreign_key, test_add_constraint_foreign_key);
	tcase_add_checked_fixture(tc_foreign_key, setup, teardown);


	/* Add test cases to suite */
	suite_add_tcase(s, tc_create);
	suite_add_tcase(s, tc_create_and_delete_database);
	suite_add_tcase(s, tc_insert);	
	suite_add_tcase(s, tc_select);
	suite_add_tcase(s, tc_delete);
	suite_add_tcase(s, tc_alter);
	suite_add_tcase(s, tc_foreign_key);
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
