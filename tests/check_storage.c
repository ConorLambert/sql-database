#include <stdio.h>
#include <check.h>
#include <stdlib.h>
#include "../src/server/storage/diskio.h"
#include "../libs/libbtree/btree.h"


char **data;
int size_of_data;

Record *record;

Table *table;

char *column_names[4];
char *data_types[4];
int number_of_fields;


 
// UTILITY FUNCTIONS

Record * util_createRecord(){
	
	number_of_fields = 4;

	data = malloc(number_of_fields * sizeof(char *));

	data[0] = malloc(strlen("Conor") + 1);
	strcpy(data[0], "Conor");
	data[1] = malloc(strlen("25") + 1);
	strcpy(data[1], "25");
	data[2] = malloc(strlen("12-05-1990") + 1);
	strcpy(data[2], "12-05-1990");
	data[3] = malloc(strlen("2955690") + 1);
	strcpy(data[3], "2955690");

	size_of_data = 0;
	
	int i;
	char **p = data;
	for(i = 0; i < number_of_fields; ++i, ++p)
		size_of_data += strlen(*p) * sizeof(**p);		
	printf("\n\tSize of data is %d, number_of_fields is %d\n", sizeof(**data), strlen(*data));
	ck_assert(size_of_data == (24 * sizeof(**data)));
	
	record = createRecord(data, number_of_fields, size_of_data);

	return record;
}

void util_freeRecord(Record *record) {
	printf("\n\tfreeing record data\n");
	
	printf("\n\tfreeing record\n");
	freeRecord(record, number_of_fields);
}


Table * util_createTable() {
	return createTable("test_table");
}

void util_createFormat(Table *table){

	column_names[0] = "FIRST_NAME";
	column_names[1] = "AGE";
	column_names[2] = "DATE_OF_BIRTH";
	column_names[3] = "TELEPHONE_NO";

	data_types[0] = "VARCHAR";
	data_types[1] = "INT";
	data_types[2] = "VARCHAR";
	data_types[3] = "VARCHAR";

        number_of_fields = 4;

        createFormat(table, column_names, data_types, number_of_fields);	
}

void util_freeTable(Table *table) {
	deleteTable(table);
}


Indexes * util_createIndexes(table) {
	return createIndexes(table);
}

void util_freeIndexes(Indexes *indexes) {
	int i;
	for(i = 0; i < indexes->number_of_indexes; ++i){
		free(indexes->indexes[i]);
	}
	free(indexes);
}


	



// RECORD

START_TEST(test_create_record) {
	fprintf(stderr, "\nTESTING Create Record\n");
	Record *record = util_createRecord();
	
	ck_assert(record->rid == 0);
	ck_assert(record->size_of_data == size_of_data);
        ck_assert(record->size_of_record == (sizeof(record->rid) + sizeof(record->size_of_data) + sizeof(record->size_of_record)+ record->size_of_data));
	
	int i;
	for(i = 0; i < number_of_fields; ++i){
		printf("\n\trecord->data[%d] = %s, data[%d] = %s \n", i, record->data[i], i, data[i]);
		ck_assert(strcmp(record->data[i], data[i]) == 0);
	}


	printf("\n\tFreeing record\n");	
	//util_freeRecord(record);
} END_TEST


START_TEST(test_insert_record) {
	fprintf(stderr, "\nTESTING inserting record\n");
	Table *table = util_createTable();
	
	Page *page = table->pages[0];
	Record *record = util_createRecord();
	insertRecord(record, page, table);
	ck_assert(table->rid == 1);
	ck_assert(record->rid == table->rid - 1);
	ck_assert(page->number_of_records == 1);
	ck_assert(page->records[page->number_of_records - 1] == record);
        	
	//util_freeRecord(record);	
	//util_freeTable(table);
}END_TEST




// FORMAT
START_TEST(test_create_format) {
	printf("\nTESTING Create Format\n");

	table = util_createTable();	
	
	util_createFormat(table);		

	int i;
	for(i = 0; i < number_of_fields; ++i){	
		printf("\ntable->format->fields[i]->type = %s\n", table->format->fields[i]->type);
		printf("\ntable->format->fields[i]->name = %s\n", table->format->fields[i]->name);
		ck_assert(strcmp(table->format->fields[i]->type, data_types[i]) == 0);	
		ck_assert(strcmp(table->format->fields[i]->name, column_names[i]) == 0);
	}
	
	//util_freeTable(table);
} END_TEST


START_TEST(test_get_column_data) {
	Table *table = util_createTable();
	Page *page1 = createPage(table);
	Indexes *indexes = util_createIndexes(table);	
	util_createFormat(table);
	util_createRecord();
       
	char result_telephone_number[50];
	char result_date_of_birth[50];
	char result_age[50];
	char result_name[50];
	
	getColumnData(record, "TELEPHONE_NO", result_telephone_number, table->format);
	ck_assert(strcmp(result_telephone_number, "2955690") == 0);	

	getColumnData(record, "DATE_OF_BIRTH", result_date_of_birth, table->format);
	ck_assert(strcmp(result_date_of_birth, "12-05-1990") == 0);	
	
	getColumnData(record, "AGE", result_age, table->format);
	ck_assert(strcmp(result_age, "25") == 0);
	
	getColumnData(record, "FIRST_NAME", result_name, table->format);
	ck_assert(strcmp(result_name, "Conor") == 0);

} END_TEST



// PAGES
 
START_TEST(test_create_page) {
	fprintf(stderr, "\nTESTING creating pages\n");
	table = util_createTable();

	fprintf(stderr, "\n\tTESTING creating page 1\n");
	Page *page1 = createPage(table);
	ck_assert(page1 == table->pages[1]);
	ck_assert(page1->number == 1);
	ck_assert(page1->space_available == (getpagesize() - sizeof(page1->number) - sizeof(page1->number_of_records) - sizeof(page1->record_position)  - sizeof(page1->space_available) - (MAX_RECORD_AMOUNT * sizeof(unsigned long)))); // last one is slot array
        ck_assert(page1->number_of_records == 0);
       	
	fprintf(stderr, "\n\tTESTING creating page 2\n");
	Page *page2 = createPage(table);
	ck_assert(page2 == table->pages[2]);
	ck_assert(page2->number == 2);
	//ck_assert(page2->space_available == getpagesize());
        ck_assert(page2->number_of_records == 0);
       	
	fprintf(stderr, "\n\tTESTING creating page 3\n");
	Page *page3 = createPage(table);
	ck_assert(page3 == table->pages[3]);
	ck_assert(page3->number == 3);
	//ck_assert(page3->space_available == getpagesize());
        ck_assert(page3->number_of_records == 0);
        
	fprintf(stderr, "\n\tTESTING table matches pages created\n");
	fprintf(stderr, "\n\ttable->size = %d, pagesize() = %d\n", table->size, getpagesize());
 	//ck_assert(table->size == (5 * getpagesize()));
	ck_assert(table->rid == 0);
        ck_assert(table->increment == 10);
	ck_assert(table->number_of_pages == 4);
	ck_assert(table->header_page != NULL); 	

	//util_freeTable(table); 	
}END_TEST


START_TEST(test_create_header_page) {
	fprintf(stderr, "\nTESTING header page\n");
	table = util_createTable();
	HeaderPage *header_page = createHeaderPage(table);
	fprintf(stderr, "\n\tHeader page space_available = %d\n", header_page->space_available);
	//ck_assert(header_page->space_available == getpagesize());
	
	//util_freeTable(table);
}END_TEST




// TABLE

START_TEST(test_create_table) {
	fprintf(stderr, "\nTESTING Creating Table \n");
	table = util_createTable();

	//ck_assert(table->size == (getpagesize() * 2));
	ck_assert(table->rid == 0);
        ck_assert(table->increment == 10);
	ck_assert(table->number_of_pages == 1);
	ck_assert(table->header_page != NULL); 	
	
	//util_freeTable(table);

}END_TEST





// TABLE INDEXES

START_TEST(test_create_record_key){
	
	int rid = 10;
	int page_number = 15;
	int slot_number = 12;

	fprintf(stderr, "\nTESTING Creating Record Node\n");
	RecordKey *recordKey = createRecordKey(rid, page_number, slot_number);

	ck_assert(recordKey->rid == rid);
	ck_assert(recordKey->value->page_number == page_number);
	ck_assert(recordKey->value->slot_number == slot_number);
} END_TEST


START_TEST(test_insert_record_key){
	
	printf("\nTESTING Insert Record Key\n");
	table = util_createTable();
	
	int rid = 10;
        int page_number = 15;
        int slot_number = 12;
      
        RecordKey *recordKey = createRecordKey(rid, page_number, slot_number);
	
	insertRecordKey(recordKey, table);	

	bt_key_val *key_val = btree_search(table->header_page->b_tree, &recordKey->rid);
	ck_assert(*(int *)key_val->key == rid);
	ck_assert(((RecordKeyValue *) (key_val->val))->slot_number == slot_number);
	ck_assert(((RecordKeyValue *) (key_val->val))->page_number == page_number);
}END_TEST


// INDEXES

START_TEST(test_create_indexes){
	fprintf(stderr, "\nTESTING Creating indexes\n");
	table = util_createTable();
	Indexes *indexes = util_createIndexes(table);
        ck_assert(indexes->space_available == MAX_INDEX_SIZE);	
	ck_assert(indexes->number_of_indexes == 0);
        ck_assert(indexes->size == sizeof(indexes->space_available) + sizeof(indexes->size) + sizeof(indexes->number_of_indexes) + sizeof(indexes->indexes));
	ck_assert(table->indexes == indexes);
	//util_freeIndexes(indexes);
	//util_freeTable(table);
} END_TEST


START_TEST(test_create_index){

	fprintf(stderr, "\nTESTING Creating index\n");
	table = util_createTable();
	util_createFormat(table);
	Indexes *indexes = util_createIndexes(table);

	char index_name[] = "FIRST_NAME";
	Index *index = createIndex(index_name, table);
        ck_assert(index->header_size == sizeof(index->index_name) + sizeof(index->header_size) + sizeof(index->btree_size));
        ck_assert(index->b_tree != NULL);
        ck_assert(strcmp(index->index_name, index_name) == 0);

	ck_assert(indexes->number_of_indexes == 1);
	ck_assert(indexes->indexes[indexes->number_of_indexes - 1] == index);

	
 	char index_name2[] = "AGE";
	Index *index2 = createIndex(index_name2, table);
        ck_assert(index2->header_size == sizeof(index2->index_name) + sizeof(index2->header_size) + sizeof(index2->btree_size));
	ck_assert(index2->b_tree != NULL);        
        ck_assert(strcmp(index2->index_name, index_name2) == 0);

	ck_assert(indexes->number_of_indexes == 2);
	ck_assert(indexes->indexes[indexes->number_of_indexes - 1] == index2);
  
	//util_freeIndexes(indexes);	
	//util_freeTable(table);
	
} END_TEST


START_TEST(test_create_index_key) {
	printf("\nTESTING Create Index Key\n");

	char key1[] = "Conor";
	int value1 = 20;	
	IndexKey *indexKey1 = createIndexKey(key1, value1);
	ck_assert(indexKey1->key == key1);
	ck_assert(indexKey1->value == value1);
	ck_assert(indexKey1->size_of_key == sizeof(indexKey1->value) + sizeof(indexKey1->key) + sizeof(indexKey1->size_of_key));
	
	char key2[] = "Donal";
	int value2 = 22;
	IndexKey *indexKey2 = createIndexKey(key2, value2);
	ck_assert(indexKey2->value == value2);
	ck_assert(indexKey2->key == key2);
	ck_assert(indexKey2->size_of_key == sizeof(indexKey2->value) + sizeof(indexKey2->key) + sizeof(indexKey2->size_of_key));
} END_TEST



START_TEST(test_insert_index_key) {
	printf("\nTESTING Insert Index Key\n");
	table = util_createTable();
	printf("\nAfter create index0\n");
	util_createFormat(table);
	printf("\nAfter create index1\n");
        Indexes *indexes = createIndexes(table);
	printf("\nAfter create index2\n");
	Index *index = createIndex("FIRST_NAME", table);
	
	printf("\nAfter create index\n");
		
	char key1[] = "Conor";
	int value1 = 20;
	IndexKey *indexKey1 = createIndexKey(key1, value1);
	insertIndexKey(indexKey1, index);
	bt_key_val * b_tree_key_val1 = btree_search(index->b_tree,  key1);
	ck_assert(strcmp(b_tree_key_val1->key, key1) == 0);
	ck_assert(* (int *)b_tree_key_val1->val == value1);
	
	printf("\nAfter create key1\n");
	
	char key2[] = "John";
	int value2 = 25;
	IndexKey *indexKey2 = createIndexKey(key2, value2);
	insertIndexKey(indexKey2, index);
	bt_key_val * b_tree_key_val2 = btree_search(index->b_tree,  key2);
	ck_assert(strcmp(b_tree_key_val2->key, key2) == 0);
	ck_assert(*(int *)b_tree_key_val2->val == value2);
	
	printf("\nAfter key2\n");

	util_freeIndexes(indexes);
	free(indexKey1);
	free(indexKey2);
		
} END_TEST




START_TEST(test_get_path_to_file){
	char table[] = "test_table";	
	char database[] = "test_database";
	char extension[] = ".csd";

	char path_to_table[50];
	getPathToFile(extension, table, database, path_to_table);

	printf("path_to_table = %s\n", path_to_table);
	ck_assert(strcmp(path_to_table, "data/test_database/test_table.csd") == 0);	
}END_TEST




Suite * storage_suite(void)
{
	Suite *s;
	TCase *tc_records;
	TCase *tc_format;
	TCase *tc_select;
	TCase *tc_pages;
	TCase *tc_tables;
	TCase *tc_nodes;
	TCase *tc_files;

	s = suite_create("Disk Storage");

	/* Records test case */
	tc_records = tcase_create("Records");
	tcase_add_test(tc_records, test_create_record);
	tcase_add_test(tc_records, test_insert_record);	

	/* Format test case*/
	tc_format = tcase_create("Format");
        tcase_add_test(tc_format, test_create_format);

	/* Selecting test case*/
	tc_select = tcase_create("Selecting");
	tcase_add_test(tc_select, test_get_column_data);

	/* Page test case */
	tc_pages = tcase_create("Pages");
	tcase_add_test(tc_pages, test_create_page);
	tcase_add_test(tc_pages, test_create_header_page);

	/* Table test case */
	tc_tables = tcase_create("Tables");
	tcase_add_test(tc_tables, test_create_table);

	/* Node test case */ 
	tc_nodes = tcase_create("Nodes");
	tcase_add_test(tc_nodes, test_create_record_key);
	tcase_add_test(tc_nodes, test_insert_record_key);
	tcase_add_test(tc_nodes, test_create_index_key);
	tcase_add_test(tc_nodes, test_insert_index_key);
	tcase_add_test(tc_nodes, test_create_indexes);
	tcase_add_test(tc_nodes, test_create_index);
	

	/* File Access test case */
	tc_files = tcase_create("Files");
	tcase_add_test(tc_files, test_get_path_to_file);

	/* Add test cases to suite */
	suite_add_tcase(s, tc_records);
	suite_add_tcase(s, tc_format);
	suite_add_tcase(s, tc_select);
	suite_add_tcase(s, tc_pages);
	suite_add_tcase(s, tc_tables);
	suite_add_tcase(s, tc_nodes);
	suite_add_tcase(s, tc_files);

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
