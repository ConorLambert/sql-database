#include <stdio.h>
#include <check.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
//#include "../src/server/storage/diskio.h"
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


	// create an index
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name1);
	Index *index = createIndex("FIRST_NAME", table->indexes);


	// create and insert a record
	char first_name1[] = "Conor";
        char age1[] = "33";
        char date_of_birth1[] = "12-05-1990";
        char telephone_no1[] = "086123456";	
	char *data1[] = {first_name1, age1, date_of_birth1, telephone_no1};	
	insert(data1, 4, table_name1, "test_database");


	// TEST FOR CORRECTNESS
	// counter
	int i;
	int rid = 0;
	int slot_number = 0;
	int page_number = 0;


	// test format
	for(i = 0; i < number_of_fields; ++i){
                char *type = strtok(fields[i], " ");
                char *name = strtok(NULL, " ");
                ck_assert(strcmp(table->format->fields[i]->type, type) == 0);
                ck_assert(strcmp(table->format->fields[i]->name, name) == 0);
        }

	
	// test record
	Record * record = table->pages[0]->records[0];
	for(i = 0; i < number_of_fields; ++i) {
		ck_assert(strcmp(record->data[i], data1[i]) == 0);
		printf("\n\t\trecord->data[%d] = %s, data1[%d] = %s\n", i, record->data[i], i, data1[i]);
	}
	

	// test record key
	bt_key_val *key_val = btree_search(table->header_page->b_tree, &rid);
        ck_assert(*(int *)key_val->key == rid);
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
	bt_key_val * b_tree_key_val1 = btree_search(index->b_tree, "Conor");
        ck_assert(strcmp(b_tree_key_val1->key, "Conor") == 0);
        ck_assert(* (int *)b_tree_key_val1->val == 0);


	util_deleteDatabase();

} END_TEST




Suite * storage_suite(void)
{
	Suite *s;
	TCase *tc_create;
	TCase *tc_create_and_delete_database;
	TCase *tc_insert;

	s = suite_create("SQL Access");

	/* Create test case */
	tc_create = tcase_create("Create Table");
	tcase_add_test(tc_create, test_create);
	
	tc_create_and_delete_database = tcase_create("Create and Delete Database");
	tcase_add_test(tc_create_and_delete_database, test_create_and_delete_database);

	tc_insert = tcase_create("Insert Data");
	tcase_add_test(tc_insert, test_insert);
	
	/* Add test cases to suite */
	suite_add_tcase(s, tc_create);
	suite_add_tcase(s, tc_create_and_delete_database);
	suite_add_tcase(s, tc_insert);	
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