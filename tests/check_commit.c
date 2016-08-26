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


void util_deleteTestFile() {
	system("rm test_serialize.csd");
}


START_TEST(test_commit_table) {
	printf("\nTESTING Committing Table\n");

        Table *table = util_createTable("test_table.csd");

        char field_first_name[] = "VARCHAR FIRST_NAME";
        char field_age[] = "INT AGE";
        char field_date_of_birth[] = "VARCHAR DATE_OF_BIRTH";
        char field_telephone_no[] = "CHAR(7) TELEPHONE_NO";

        char *fields[] = {field_first_name, field_age, field_date_of_birth, field_telephone_no};
        int number_of_fields = 4;

	createFormat(table, fields, number_of_fields);

	// create an index
	char index_name[] = "FIRST_NAME";
	Index *index = createIndex(index_name, table);

	// insert test data
     
       
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
