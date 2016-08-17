#include <stdio.h>
#include <check.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include "../src/server/access/sqlaccess.h"
#include "../libs/libcfu/src/cfuhash.h"
#include "../libs/libbtree/btree.h"

void util_createTable(char *table_name) {
	char field_first_name[] = "VARCHAR FIRST_NAME";
        char field_age[] = "INT AGE";
        char field_date_of_birth[] = "VARCHAR DATE_OF_BIRTH";
        char field_telephone_no[] = "VARCHAR TELEPHONE_NO";
        char *fields[] = {field_first_name, field_age, field_date_of_birth, field_telephone_no};
        
	int number_of_fields = 4;

	create(table_name, fields, number_of_fields);
}


void util_testFormat(char *fields[], int number_of_fields, Table *table) {
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


START_TEST(test_commit) {
	printf("\nTESTING Commit\n");

	DataBuffer *dataBuffer = initializeDataBuffer();

	util_createDatabase();
	char *table_name = "test_table.csd";
	util_createTable(table_name);	

	free(dataBuffer);
} END_TEST


START_TEST(test_create_and_delete_database) {
	printf("\nTESTING Creating and Deleting Databases \n");

	util_createDatabase();

	// check if the directory has been created
	char *folder = "data/test_database";
    	struct stat sb;
	stat(folder, &sb);		
    	ck_assert(stat(folder, &sb) == 0 && S_ISDIR(sb.st_mode));

	// create tables to store in that database
	DataBuffer *dataBuffer = initializeDataBuffer();
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

	free(dataBuffer);
} END_TEST


START_TEST(test_create) {
	printf("\nTESTING Create Table\n");

	DataBuffer *dataBuffer = initializeDataBuffer();

	// test the table was in fact created
	char *table_name = "test_table";
	util_createTable(table_name);	
	ck_assert(cfuhash_exists(dataBuffer->tables, table_name));

	free(dataBuffer);	
} END_TEST


// returns rid of error insertion
int util_testCorrectness(Table *table, Index *index, char *index_name, char *data[], char *fields[], int number_of_fields) {
	
	int errorid = 0;
	
	// counter
	int i, j, k;
	

	// test record

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
		
	int rid = ((number_of_pages - 1) * MAX_RECORD_AMOUNT) + (number_of_records - 1);
	printf("\n\t\t\trid = %d\n", rid);
	int slot_number = (number_of_records - 1);
	printf("\n\t\t\tSlot number = %d\n", slot_number);
	int page_number = number_of_pages - 1;
	printf("\n\t\t\tPage number = %d\n", page_number);

	// test record key
	bt_key_val *key_val = btree_search(table->header_page->b_tree, &rid);
        ck_assert(*(int *)key_val->key == rid);
        ck_assert(((RecordKeyValue *) (key_val->val))->slot_number == slot_number);
	printf("\n\t\t\tRecordKey->key_val->val slot_number = %d\n", ((RecordKeyValue *) (key_val->val))->slot_number);
        ck_assert(((RecordKeyValue *) (key_val->val))->page_number == page_number);
	printf("\n\t\t\tRecordKey->key_val->val page_number = %d\n", ((RecordKeyValue *) (key_val->val))->page_number);

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
	printf("\n\t\t\tkey_val->key = %s, val = %d\n", b_tree_key_val1->key, * (int *)b_tree_key_val1->val);
        ck_assert(strcmp(b_tree_key_val1->key, data[0]) == 0);
        ck_assert(* (int *)b_tree_key_val1->val == rid);

	return errorid;
}


START_TEST(test_insert) {

	printf("\nTESTING insert\n");
	
	// create the database
	util_createDatabase();
	DataBuffer *dataBuffer = initializeDataBuffer();

	

	// create a table
	char *table_name1 = "test_table1";
	char field_first_name[] = "VARCHAR FIRST_NAME";
        char field_age[] = "INT AGE";
        char field_date_of_birth[] = "VARCHAR DATE_OF_BIRTH";
        char field_telephone_no[] = "VARCHAR TELEPHONE_NO";
	char *fields[] = {field_first_name, field_age, field_date_of_birth, field_telephone_no};
        int number_of_fields = 4;
        create(table_name1, fields, number_of_fields);	

	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name1);
	char *index_name1 = "FIRST_NAME";
	Index *index = createIndex(index_name1, table->indexes);


	// create and insert a record 1
	char first_name1[] = "Conor";
        char age1[] = "33";
        char date_of_birth1[] = "12-05-1990";
        char telephone_no1[] = "086123456";	
	char *data1[] = {first_name1, age1, date_of_birth1, telephone_no1};	
	insert(data1, 4, table_name1, "test_database"); // INSERT
	util_testCorrectness(table, index, index_name1, data1, fields, number_of_fields);		


	// create and insert a record 2
	char first_name2[] = "Damian";
        char age2[] = "44";
        char date_of_birth2[] = "05-09-1995";
        char telephone_no2[] = "086654321";
        char *data2[] = {first_name2, age2, date_of_birth2, telephone_no2};
        insert(data2, 4, table_name1, "test_database"); // INSERT
	util_testCorrectness(table, index, index_name1, data2, fields, number_of_fields);


	// testing new page created due to max record amount per page
	char first_name3[] = "Freddie";
        char age3[] = "55";
        char date_of_birth3[] = "24-02-1965";
        char telephone_no3[] = "08624681";
        char *data3[] = {first_name3, age3, date_of_birth3, telephone_no3};
        insert(data3, 4, table_name1, "test_database"); // INSERT
        util_testCorrectness(table, index, index_name1, data3, fields, number_of_fields);	

	// test number of pages increased by 1
	ck_assert(table->number_of_pages == 2);

	// test last page has one record which is the new record just inserted
	ck_assert(table->pages[table->number_of_pages - 1]->number_of_records == 1);

	// test the 0th page has only the first two records inserted
	ck_assert(table->pages[0]->number_of_records == 2);

	// test the record key of the last inserted record has a slot number of 0 and a page number of x
	// test the rid has increased
	
	free(table);
	util_deleteDatabase();

} END_TEST



START_TEST(test_select_record) {
	
	printf("\nTESTING Select Record\n");

        // create the database
        util_createDatabase();
        DataBuffer *dataBuffer = initializeDataBuffer();
	
	// create a table
        char *table_name1 = "test_table1";
        char field_first_name[] = "VARCHAR FIRST_NAME";
        char field_age[] = "INT AGE";
        char field_date_of_birth[] = "VARCHAR DATE_OF_BIRTH";
        char field_telephone_no[] = "VARCHAR TELEPHONE_NO";
        char *fields[] = {field_first_name, field_age, field_date_of_birth, field_telephone_no};
        int number_of_fields = 4;
	create(table_name1, fields, number_of_fields);
	
        
        Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name1);
        char *index_name1 = "FIRST_NAME";
        Index *index = createIndex(index_name1, table->indexes);


        // create and insert a record 1
        char first_name1[] = "Conor";
        char age1[] = "33";
        char date_of_birth1[] = "12-05-1990";
        char telephone_no1[] = "086123456";
        char *data1[] = {first_name1, age1, date_of_birth1, telephone_no1};
        insert(data1, 4, table_name1, "test_database"); // INSERT
        

        // create and insert a record 2
        char first_name2[] = "Damian";
        char age2[] = "44";
        char date_of_birth2[] = "05-09-1995";
        char telephone_no2[] = "086654321";
        char *data2[] = {first_name2, age2, date_of_birth2, telephone_no2};
        insert(data2, 4, table_name1, "test_database"); // INSERT
        

        // testing new page created due to max record amount per page
        char first_name3[] = "Freddie";
        char age3[] = "55";
        char date_of_birth3[] = "24-02-1965";
        char telephone_no3[] = "08624681";
        char *data3[] = {first_name3, age3, date_of_birth3, telephone_no3};
        insert(data3, 4, table_name1, "test_database"); // INSERT
       


	// SEARCH
	char *result1 = selectRecord("test_database", table_name1, "AGE", "FIRST_NAME", first_name2);
	printf("\n\t\tResult1 %s result1 = %s, age2 = %s\n", first_name2, result1, age2);
	ck_assert(strcmp(result1, age2) == 0);
	

	util_deleteDatabase();
} END_TEST


Suite * storage_suite(void)
{
	Suite *s;
	TCase *tc_create;
	TCase *tc_create_and_delete_database;
	TCase *tc_insert;
	TCase *tc_select;

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


	/* Add test cases to suite */
	suite_add_tcase(s, tc_create);
	suite_add_tcase(s, tc_create_and_delete_database);
	suite_add_tcase(s, tc_insert);	
	suite_add_tcase(s, tc_select);
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
