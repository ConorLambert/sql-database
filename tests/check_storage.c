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



Suite * storage_suite(void)
{
	Suite *s;
	TCase *tc_records;
	TCase *tc_pages;
	TCase *tc_tables;

	s = suite_create("Disk Storage");

	/* Records test case */
	tc_records = tcase_create("Records");
	tcase_add_test(tc_records, test_create_record);
	tcase_add_test(tc_records, test_insert_record);	

	/* Page test case */
	tc_pages = tcase_create("Pages");
	tcase_add_test(tc_pages, test_create_page);
	tcase_add_test(tc_pages, test_create_header_page);

	/* Table test */
	tc_tables = tcase_create("Tables");
	tcase_add_test(tc_tables, test_create_table);

	/* Add test cases to suite */
	suite_add_tcase(s, tc_records);
	suite_add_tcase(s, tc_pages);
	suite_add_tcase(s, tc_tables);

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
