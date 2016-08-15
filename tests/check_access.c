#include <stdio.h>
#include <check.h>
#include <stdlib.h>
#include "../src/server/access/sqlaccess.h"
#include "../libs/libcfu/src/cfuhash.h"


START_TEST(test_create) {
	printf("\nTESTING Create Table\n");
	
	char *table_name = "test_table";

	char field_first_name[] = "VARCHAR FIRST_NAME";
        char field_age[] = "INT AGE";
        char field_date_of_birth[] = "VARCHAR DATE_OF_BIRTH";
        char field_telephone_no[] = "VARCHAR TELEPHONE_NO";
        char *fields[] = {field_first_name, field_age, field_date_of_birth, field_telephone_no};
        
	int number_of_fields = 4;

	DataBuffer * dataBuffer = initializeDataBuffer();
	create(table_name, fields, number_of_fields);

	ck_assert(cfuhash_exists(dataBuffer->tables, table_name));

} END_TEST


Suite * storage_suite(void)
{
	Suite *s;
	TCase *tc_create;

	s = suite_create("SQL Access");

	/* Create test case */
	tc_create = tcase_create("Create Table");
	tcase_add_test(tc_create, test_create);
	
	/* Add test cases to suite */
	suite_add_tcase(s, tc_create);
	
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
