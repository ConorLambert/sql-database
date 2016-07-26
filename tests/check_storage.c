#include <stdio.h>
#include <check.h>
#include <stdlib.h>
#include "../src/server/storage/diskio.h"


START_TEST(test_create_record) {

	char data[] = "some test data";
	fprintf(stderr, "\nTESTING data \"%s\"\n", data);
	printf("Size of the data %d\n", sizeof(data));
	Record *record = createRecord(data);
	printf("Record.id = %d\n", record->rid == 0);
	ck_assert(record->rid == 0);
	ck_assert(record->size_of_data == ((strlen(data) + 1) * sizeof(char)));
        ck_assert(record->size_of_record == (sizeof(record->rid) + sizeof(record->size_of_data) + sizeof(record->size_of_record) + sizeof(record->data)));
	ck_assert(strcmp(record->data, data) == 0);
	
	free(record->data);
	free(record);

} END_TEST


START_TEST(test_insert_record) {
	fprintf(stderr, "\nTESTING inserting record\n");
	Table *table = createTable("some_name");
	Page *page = table->pages[0];
	Record *record = createRecord("some_record_data");
	insertRecord(record, page, table);
	ck_assert(table->rid == 1);
	ck_assert(record->rid == table->rid - 1);
	ck_assert(page->number_of_records == 1);
        ck_assert(page->records[page->number_of_records - 1] == record);
        ck_assert(page->slot_array[page->number_of_records - 1] == getpagesize() - page->space_available - record->size_of_record);
}END_TEST

 
START_TEST(test_create_page) {
	fprintf(stderr, "\nTESTING creating pages\n");
	char table_name[] = "some_table_0";
	Table *table = createTable(table_name);

	fprintf(stderr, "\n\tTESTING creating page 1\n");
	Page *page1 = createPage(table);
	ck_assert(page1 == table->pages[1]);
	ck_assert(page1->number == 1);
	ck_assert(page1->space_available == getpagesize());
        ck_assert(page1->number_of_records == 0);
        ck_assert(page1->record_type == -1); 
	
	fprintf(stderr, "\n\tTESTING creating page 2\n");
	Page *page2 = createPage(table);
	ck_assert(page2 == table->pages[2]);
	ck_assert(page2->number == 2);
	ck_assert(page2->space_available == getpagesize());
        ck_assert(page2->number_of_records == 0);
        ck_assert(page2->record_type == -1); 
	
	fprintf(stderr, "\n\tTESTING creating page 3\n");
	Page *page3 = createPage(table);
	ck_assert(page3 == table->pages[3]);
	ck_assert(page3->number == 3);
	ck_assert(page3->space_available == getpagesize());
        ck_assert(page3->number_of_records == 0);
        ck_assert(page3->record_type == -1);       

	fprintf(stderr, "\n\tTESTING table matches pages created\n");
	fprintf(stderr, "\n\ttable->size = %d, pagesize() = %d\n", table->size, getpagesize());
 	ck_assert(table->size == (5 * getpagesize()));
	ck_assert(table->rid == 0);
        ck_assert(table->increment == 10);
	ck_assert(table->number_of_pages == 4);
	ck_assert(table->header_page != NULL); 	

	free(table);
	free(table->pages[0]);
	free(page1);
	free(page2);
	free(page3);
 	
}END_TEST


START_TEST(test_create_header_page) {
	fprintf(stderr, "\nTESTING header page\n");
	Table *table = createTable("some_table");
	HeaderPage *header_page = createHeaderPage(table);
	fprintf(stderr, "\nHeader page space_available = %d\n", header_page->space_available);
	ck_assert(header_page->space_available == getpagesize());
	
	free(header_page);
}END_TEST


START_TEST(test_create_table) {
	char table_name[] = "some_table";
	fprintf(stderr, "\nTESTING table \"%s\"\n", table_name);
	Table *table = createTable(table_name);

	ck_assert(table->size == (getpagesize() * 2));
	ck_assert(table->rid == 0);
        ck_assert(table->increment == 10);
	ck_assert(table->number_of_pages == 1);
	ck_assert(table->header_page != NULL); 	
	
	free(table->pages[0]);
	free(table);

}END_TEST


START_TEST(test_create_record_node){
	int rid = 10;
	int page_number = 15;
	int slot_number = 12;

	fprintf(stderr, "\nTESTING node\n");
	RecordNode recordNode = createRecordNode(rid, page_number, slot_number);

	fprintf(stderr, "\nRecord node rid -> %d\n", recordNode.rid);
	fprintf(stderr, "\nRecord node page_number -> %d\n", recordNode.page_number);
	fprintf(stderr, "\nRecord node slot_number -> %d\n", recordNode.slot_number);
	ck_assert(recordNode.rid == rid);
	ck_assert(recordNode.page_number == page_number);
	ck_assert(recordNode.slot_number == slot_number);
} END_TEST


START_TEST(test_create_indexes){
	fprintf(stderr, "\nTESTING Creating indexes\n");
	//Indexes *indexes = createIndexes("test_table");

        //ck_assert(indexes->space_available == MAX_INDEX_SIZE);	
	//ck_assert(indexes->number_of_indexes == 0);
        //ck_assert(indexes->size == sizeof(indexes->space_available) + sizeof(indexes->size) + sizeof(indexes->number_of_indexes) + sizeof(indexes->indexes));
	//free(indexes);
} END_TEST


START_TEST(test_create_index){

	fprintf(stderr, "\nTESTING Creating index\n");
	Indexes *indexes = createIndexes("test_indexes");

	char index_name[] = "test_index";
	Index *index = createIndex(index_name, indexes);
        ck_assert(index->size == 0);
        ck_assert(index->number_of_nodes == 0);
        ck_assert(strcmp(index->index_name, index_name) == 0);

	ck_assert(indexes->number_of_indexes == 1);
	ck_assert(indexes->indexes[indexes->number_of_indexes - 1] == index);

	
 	char index_name2[] = "test_index2";
	Index *index2 = createIndex(index_name2, indexes);
        ck_assert(index2->size == 0);
        ck_assert(index2->number_of_nodes == 0);
        ck_assert(strcmp(index2->index_name, index_name2) == 0);

	ck_assert(indexes->number_of_indexes == 2);
	ck_assert(indexes->indexes[indexes->number_of_indexes - 1] == index2);
  
	free(index);
	free(index2);
	free(indexes);
} END_TEST



START_TEST(test_create_index_node) {
        Indexes *indexes = createIndexes("test_indexes");
	Index *index = createIndex("name", indexes);

	char key[] = "Conor";
	int rid = 20;
	IndexNode *indexNode = createIndexNode(index, key, rid);
	
	ck_assert(strcmp(indexNode->key, key) == 0);
        ck_assert(indexNode->rid == rid);
        ck_assert(indexNode->size_of_node == sizeof(indexNode->key) + sizeof(indexNode->rid) + sizeof(indexNode->size_of_node));

	ck_assert(index->number_of_nodes == 1);
        ck_assert(index->indexNodes[index->number_of_nodes - 1] == indexNode);



	char key2[] = "John";
	int rid2 = 25;
	IndexNode *indexNode2 = createIndexNode(index, key2, rid2);
	
	ck_assert(strcmp(indexNode2->key, key2) == 0);
        ck_assert(indexNode2->rid == rid2);
        ck_assert(indexNode2->size_of_node == sizeof(indexNode2->key) + sizeof(indexNode2->rid) + sizeof(indexNode2->size_of_node));

	ck_assert(index->number_of_nodes == 2);
        ck_assert(index->indexNodes[index->number_of_nodes - 1] == indexNode2);


	free(indexes);
	free(index);
	free(indexNode);
	free(indexNode2);
} END_TEST




START_TEST(test_get_path_to_file){
	char table[] = "test_table";	
	char database[] = "test_database";
	char extension[] = ".csd";

	char path_to_table[50];
	getPathToFile(extension, table, database, path_to_table);

	printf("path_to_table = %s\n", path_to_table);
	ck_assert(strcmp(path_to_table, "test_database/test_table.csd") == 0);	
}END_TEST



// test commits
START_TEST(test_commit_table){
	
}END_TEST




Suite * storage_suite(void)
{
	Suite *s;
	TCase *tc_records;
	TCase *tc_pages;
	TCase *tc_tables;
	TCase *tc_nodes;
	TCase *tc_files;

	s = suite_create("Disk Storage");

	/* Records test case */
	tc_records = tcase_create("Records");
	tcase_add_test(tc_records, test_create_record);
	tcase_add_test(tc_records, test_insert_record);	

	/* Page test case */
	tc_pages = tcase_create("Pages");
	tcase_add_test(tc_pages, test_create_page);
	tcase_add_test(tc_pages, test_create_header_page);

	/* Table test case */
	tc_tables = tcase_create("Tables");
	tcase_add_test(tc_tables, test_create_table);

	/* Node test case */ 
	tc_nodes = tcase_create("Nodes");
	tcase_add_test(tc_nodes, test_create_record_node);
	tcase_add_test(tc_nodes, test_create_index_node);
	tcase_add_test(tc_nodes, test_create_indexes);
	tcase_add_test(tc_nodes, test_create_index);


	/* File Access test case */
	tc_files = tcase_create("Files");
	tcase_add_test(tc_files, test_get_path_to_file);

	/* Add test cases to suite */
	suite_add_tcase(s, tc_records);
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
