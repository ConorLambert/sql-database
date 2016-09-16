#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include "check_utility.h"


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
	_setup();
}

void teardown(void) {
	_teardown();	
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
	// char **selectRecord(char *database_name, char *table_name, char **target_column_name, int number_of_target_columns, char *condition_column_name, char *condition_value, int number_of_condition_column_values);


	// rid search
	char **result = selectRecord("test_database", table_name1, "FIRST_NAME", "rid", "0");
	printf("\n**result = %s\n", result[0]);
	printf("\n\t\tActual Result = %s Expected Result = %s, SELECT FIRST_NAME FROM %s WHERE rid = 0\n", result[0], first_name1, table_name1);
	ck_assert(strcmp(result[0], first_name1) == 0);


	// index search	
	char **result1 = selectRecord("test_database", table_name1, "AGE", "FIRST_NAME", first_name2);
	printf("\n**result1 = %s\n", result1[0]);
	printf("\n\t\tActual Result = %s Expected Result = %s, SELECT AGE FROM %s WHERE FIRST_NAME = %s\n", result1[0], age2, table_name1, first_name2);
	ck_assert(strcmp(result1[0], age2) == 0);
	
	
	// sequential search
	char **result2 = selectRecord("test_database", table_name1, "TELEPHONE_NO", "AGE", age3);
	printf("\n**result2 = %s, expected_result = %s\n", result2[0], telephone_no3);
	ck_assert(strcmp(result2[0], telephone_no3) == 0);	


	// more then one record with the same column value
	char *first_name4 = malloc(strlen("Freddie") + 1);
        strcpy(first_name4, "Freddie");
        char *age4 = malloc(strlen("55") + 1);
        strcpy(age4, "55");
        char *date_of_birth4 = malloc(strlen("24-02-1965") + 1);
        strcpy(date_of_birth4, "24-02-1965");
        char *telephone_no4 = malloc(strlen("08624681ii") + 1);
        strcpy(telephone_no4, "08624681ii");
        char **data4 = malloc(4 * sizeof(char *));
        data4[0] = first_name4;
        data4[1] = age4;
        data4[2] = date_of_birth4;
        data4[3] = telephone_no4;

        insert(data4, number_of_fields1, table_name1, test_database);

	char **result2ii = selectRecord("test_database", table_name1, "TELEPHONE_NO", "AGE", age3);
	
	printf("\n**result2ii[0] = %s, expected_result = %s\n**result2ii[1] = %s, expected_result = %s", result2ii[0], telephone_no3, result2ii[1], telephone_no4);
	ck_assert(strcmp(result2ii[0], telephone_no3) == 0);	
	ck_assert(strcmp(result2ii[1], telephone_no4) == 0);
	

	char **result1ii = selectRecord("test_database", table_name1, "TELEPHONE_NO", "FIRST_NAME", first_name3);
	printf("\n**result1ii[0] = %s, expected_result = %s\n**result1ii[1] = %s, expected_result = %s", result1ii[0], telephone_no3, result1ii[1], telephone_no4);
	ck_assert(strcmp(result1ii[0], telephone_no3) == 0);	
	ck_assert(strcmp(result1ii[1], telephone_no4) == 0);	



	int i;
	for(i = 0; i < number_of_fields1; ++i)
		free(data4[i]);


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
