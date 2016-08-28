#include <stdio.h>
#include <check.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include "../src/server/access/sqlaccess.h"
#include "../libs/libcfu/src/cfuhash.h"
#include "../libs/libbtree/btree.h"


Table * util_createTable(char *table_name) {
        return createTable(table_name);
}

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


void util_deleteTestFile() {
        system("rm test_serialize.csd");
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

	printf("\n\n\n");
	

	printf("\nIm here -1\n");

	// open table
	Table *table1 = openTable(table_name1, "test_database");      

	printf("\nIm here 0\n");

	ck_assert(table->rid == table1->rid);
	ck_assert(table->size == table1->size);
	ck_assert(table->increment == table1->increment);
	ck_assert(table->number_of_pages == table1->number_of_pages);
	ck_assert(table->page_position == table1->page_position);
	ck_assert(table->record_type == table1->record_type);

	ck_assert(table->format->number_of_fields == table1->format->number_of_fields);
	ck_assert(table->format->format_size == table->format->format_size);
		
	int i, j, k;
	for(i = 0; i < table->format->number_of_fields; ++i) {
		ck_assert(strcmp(table->format->fields[i]->type, table1->format->fields[i]->type) == 0);	
		ck_assert(strcmp(table->format->fields[i]->name, table1->format->fields[i]->name) == 0);	
		ck_assert(table->format->fields[i]->size == table1->format->fields[i]->size);
	}


	ck_assert(table->indexes->size == table1->indexes->size);
	ck_assert(table->indexes->space_available == table1->indexes->space_available);
	ck_assert(table->indexes->number_of_indexes == table1->indexes->number_of_indexes);

	for(i = 0; i < table->indexes->number_of_indexes; ++i) {
		ck_assert(strcmp(table->indexes->indexes[i]->index_name, table->indexes->indexes[i]->index_name) == 0);
		ck_assert(strcmp(table->indexes->indexes[i]->b_tree->key_type, table1->indexes->indexes[i]->b_tree->key_type) == 0);
		ck_assert(table->indexes->indexes[i]->header_size == table->indexes->indexes[i]->header_size);
		ck_assert(table->indexes->indexes[i]->btree_size == table->indexes->indexes[i]->btree_size);
	}
		

	
	for(i = 0; i < table->number_of_pages; ++i) {
		ck_assert(table->pages[i]->number == table1->pages[i]->number);
		ck_assert(table->pages[i]->space_available == table1->pages[i]->space_available);
		ck_assert(table->pages[i]->number_of_records == table1->pages[i]->number_of_records);
		ck_assert(table->pages[i]->record_position == table1->pages[i]->record_position);
		ck_assert(table->pages[i]->number == table1->pages[i]->number);

		
		for(j = 0; j < MAX_RECORD_AMOUNT; ++j) {
		
			if(table->pages[i]->slot_array[j] == 0)
				continue;
			ck_assert(table->pages[i]->slot_array[j] == table1->pages[i]->slot_array[j]);
			ck_assert(table->pages[i]->records[j]->rid == table1->pages[i]->records[j]->rid);
			ck_assert(table->pages[i]->records[j]->size_of_data == table1->pages[i]->records[j]->size_of_data);
			ck_assert(table->pages[i]->records[j]->size_of_record == table1->pages[i]->records[j]->size_of_record);
		
				
			for(k = 0; k < table->format->number_of_fields; ++k)  {
				printf("\n%s, %s\n", table->pages[i]->records[j]->data[k], table1->pages[i]->records[j]->data[k]);
				ck_assert(strcmp(table->pages[i]->records[j]->data[k], table1->pages[i]->records[j]->data[k]) == 0);
			}

			RecordKey * recordKey = findRecordKey(table, table->pages[i]->records[j]->rid);
			RecordKey * recordKey1 = findRecordKey(table1, table1->pages[i]->records[j]->rid);

			ck_assert(recordKey->rid == recordKey1->rid);			
			ck_assert(recordKey->value->page_number == recordKey1->value->page_number);
			ck_assert(recordKey->value->slot_number == recordKey1->value->slot_number);
				
			for(k = 0; k < table->indexes->number_of_indexes; ++k) {
				char destination[50];
				char destination1[50];

				getColumnData(table->pages[i]->records[j], table->indexes->indexes[k]->index_name, destination, table->format); 
				getColumnData(table1->pages[i]->records[j], table1->indexes->indexes[k]->index_name, destination1, table1->format);

				IndexKey *indexKey = findIndexKey(table->indexes->indexes[k], destination);
				IndexKey *indexKey1 = findIndexKey(table1->indexes->indexes[k], destination1);
								
				ck_assert(strcmp(indexKey->key, indexKey1->key) == 0);				
				ck_assert(indexKey->value == indexKey1->value);
			}
		}

	}	

      	util_deleteDatabase();

} END_TEST


START_TEST(test_serialization_table_btree) {

        printf("\nTESTING Serialization/Deserialization Table Tree\n");

        Table *table = util_createTable("test_table.csd");

        char field_first_name[] = "VARCHAR FIRST_NAME";
        char field_age[] = "INT AGE";
        char field_date_of_birth[] = "VARCHAR DATE_OF_BIRTH";
        char field_telephone_no[] = "VARCHAR TELEPHONE_NO";

        char *fields[] = {field_first_name, field_age, field_date_of_birth, field_telephone_no};
        int number_of_fields = 4;

        createFormat(table, fields, number_of_fields);

        btree *btree = createBtree("INT", "RECORD", sizeof(int), sizeof(RecordKeyValue));
        table->header_page->b_tree = btree;

        // insert some test data into the tree
        int values[] = {5, 9, 3, 7, 1, 2, 8, 6, 0, 4};

        int i;
        for (i=0;i<10;i++) {
                RecordKey *recordKey = createRecordKey(values[i], values[i], values[i]);
                insertRecordKey(recordKey, table);
        }

        //display the tree
        print_subtree(btree,btree->root);

        FILE *fp = fopen("test_serialize.csd", "wb+");
        serializeTree(btree, btree->root, fp);
        btree_destroy(btree);
        fclose(fp);


        printf("\n\n\n");

        fp = fopen("test_serialize.csd", "rb+");
        btree = createBtree("INT", "RECORD", sizeof(int), sizeof(RecordKeyValue));
        table->header_page->b_tree = btree;
        table->header_page->b_tree->root = deserializeTree(fp, "TABLE", table);
        print_subtree(btree, btree->root);
        //btree_destroy(btree);
        fclose(fp);

        util_deleteTestFile();

} END_TEST


START_TEST(test_serialization_index_btree) {
        printf("\nTESTING Serialization/Deserialization Fixed Size Index Tree\n");

        Table *table = util_createTable("test_table.csd");

        char field_first_name[] = "VARCHAR FIRST_NAME";
        char field_age[] = "INT AGE";
        char field_date_of_birth[] = "VARCHAR DATE_OF_BIRTH";
        char field_telephone_no[] = "CHAR(7) TELEPHONE_NO";

        char *fields[] = {field_first_name, field_age, field_date_of_birth, field_telephone_no};
        int number_of_fields = 4;


        createFormat(table, fields, number_of_fields);

        char index_name[] = "TELEPHONE_NO";
        Index *index = createIndex(index_name, table);
	printf("\n\t\t\tIM HERE\n");

        // insert some test data into the tree
        char *values[] = {"2955690", "2950987", "2958765", "2956743", "2954321", "2952468", "2953214", "2957654", "2953579", "2953267"};

        int i;
        for (i=0;i<10;i++) {
                IndexKey *indexKey = createIndexKey(values[i], i);
                insertIndexKey(indexKey, index);
        }

        //display the tree
        print_subtree(index->b_tree, index->b_tree->root);

        FILE *fp = fopen("test_serialize.csd", "wb+");
        serializeTree(index->b_tree, index->b_tree->root, fp);
        btree_destroy(index->b_tree);
        fclose(fp);

        printf("\n\n\n");

        fp = fopen("test_serialize.csd", "rb+");
        index->b_tree = createBtree("CHAR(7)", "INT", getSizeOf("CHAR(7)"), sizeof(int));
        index->b_tree->root = deserializeTree(fp, index_name, table);
        print_subtree(index->b_tree, index->b_tree->root);
        //btree_destroy(index->b_tree);
        fclose(fp);

        util_deleteTestFile();
} END_TEST



START_TEST(test_serialization_index_variable_btree) {
        printf("\nTESTING Serialization/Deserialization Index Tree\n");

        Table *table = util_createTable("test_table.csd");

        char field_first_name[] = "VARCHAR FIRST_NAME";
        char field_age[] = "INT AGE";
        char field_date_of_birth[] = "VARCHAR DATE_OF_BIRTH";
        char field_telephone_no[] = "VARCHAR TELEPHONE_NO";

        char *fields[] = {field_first_name, field_age, field_date_of_birth, field_telephone_no};
        int number_of_fields = 4;

        createFormat(table, fields, number_of_fields);

        char index_name[] = "FIRST_NAME";
        Index *index = createIndex(index_name, table);

        // insert some test data into the tree
        char *values[] = {"CONOR", "DONALD", "MICHAEL", "ADRIAN", "ADAM", "RORY", "AONGO", "FINTON", "DIARMUD", "RICHARD"};

        int i;
        for (i=0;i<10;i++) {
                IndexKey *indexKey = createIndexKey(values[i], i);
                insertIndexKey(indexKey, index);
        }

        //display the tree
        print_subtree(index->b_tree, index->b_tree->root);

        FILE *fp = fopen("test_serialize.csd", "wb+");
        serializeTree(index->b_tree, index->b_tree->root, fp);
        btree_destroy(index->b_tree);
        fclose(fp);

        printf("\n\n\n");

        fp = fopen("test_serialize.csd", "rb+");
        index->b_tree = createBtree("VARCHAR", "INT", getSizeOf("VARCHAR"), sizeof(int));
        index->b_tree->root = deserializeTree(fp, index_name, table);
        print_subtree(index->b_tree, index->b_tree->root);
        //btree_destroy(index->b_tree);
	fclose(fp);

        util_deleteTestFile();
} END_TEST


Suite * storage_suite(void)
{
	Suite *s;
	TCase *tc_commit;
	TCase *tc_serialize_tree;	

	s = suite_create("Commit");

	/* Create test case */
	tc_commit = tcase_create("Commit Table");
	tcase_add_test(tc_commit, test_commit_table);
	
	tc_serialize_tree = tcase_create("Serialize/Deserialize Tree");
        tcase_add_test(tc_serialize_tree, test_serialization_table_btree);
        tcase_add_test(tc_serialize_tree, test_serialization_index_btree);
        tcase_add_test(tc_serialize_tree, test_serialization_index_variable_btree);

	/* Add test cases to suite */
	suite_add_tcase(s, tc_commit);
	suite_add_tcase(s, tc_serialize_tree);

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
