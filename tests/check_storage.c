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
	
}END_TEST
 

Suite * storage_suite(void)
{
    Suite *s;
    TCase *tc_records;

    s = suite_create("Disk Storage");

    /* Records test case */
    tc_records = tcase_create("Records");
    tcase_add_test(tc_records, test_create_record);

    /* Page test case */


    /* Table test */


    /* Add test cases to suite */
    suite_add_tcase(s, tc_records);

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
