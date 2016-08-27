#include <stdio.h>
#include <check.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include "../src/server/access/sqlaccess.h"
#include "../libs/libcfu/src/cfuhash.h"
#include "../libs/libbtree/btree.h"


void util_createDatabase(){
	createDatabase("test_database");		
}


void util_deleteDatabase(){	
	deleteDatabase("test_database");
}


void util_deleteTestFiles() {
	system("rm test_serialize.csd");
	system("rm test_serialize.csi");
	system("rm test_serialize.csf");
}


void util_freeTable(Table *table) {
        int i;
        free(table->header_page);
        for(i = 0; i < table->number_of_pages; ++i)
                free(table->pages[i]);
        free(table);
}


int util_testCorrectness(Table *table, Index *index, char *index_name, char *data[], char *fields[], int number_of_fields) {

        int errorid = 0;

        // counter
        int i, j, k;
       
        // get the last record inserted
        int number_of_pages = table->number_of_pages;
        int number_of_records = table->pages[number_of_pages - 1]->number_of_records;
        Record * record = table->pages[number_of_pages - 1]->records[number_of_records - 1];

        for(i = 0; i < number_of_fields; ++i)
                ck_assert(strcmp(record->data[i], data[i]) == 0);

        int rid = ((number_of_pages - 1) * MAX_RECORD_AMOUNT) + (number_of_records - 1);
        int slot_number = (number_of_records - 1);
        int page_number = number_of_pages - 1;
        
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
        bt_key_val * b_tree_key_val1 = btree_search(index->b_tree, data[0]);
        ck_assert(strcmp(b_tree_key_val1->key, data[0]) == 0);
        ck_assert(* (int *)b_tree_key_val1->val == rid);

        return errorid;
}


START_TEST(test_commit_table) {
	printf("\nTESTING Committing Table\n");

	util_createDatabase();
        DataBuffer *dataBuffer = initializeDataBuffer();

        char field_first_name[] = "VARCHAR FIRST_NAME";
        char field_age[] = "INT AGE";
        char field_date_of_birth[] = "VARCHAR DATE_OF_BIRTH";
        char field_telephone_no[] = "CHAR(7) TELEPHONE_NO";

        char *fields[] = {field_first_name, field_age, field_date_of_birth, field_telephone_no};
        int number_of_fields = 4;

	char *table_name1 = "test_table";
	
	create(table_name1, fields, number_of_fields);

	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name1);


	// TEST INSERT DATA
	// create an index
	char index_name1[] = "FIRST_NAME";
	Index *index = createIndex(index_name1, table);

	// insert test data
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


	// commit table
	commitTable(table, table_name1, "test_database");

	//deleteTable(table);

	// open table
	//table = openTable(table_name1, "test_database");      


      	util_deleteDatabase();

} END_TEST


Suite * storage_suite(void)
{
	Suite *s;
	TCase *tc_commit;

	s = suite_create("Commit");

	/* Create test case */
	tc_commit = tcase_create("Commit Table");
	tcase_add_test(tc_commit, test_commit_table);
	
	/* Add test cases to suite */
	suite_add_tcase(s, tc_commit);

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
