#include <check.h>
#include <stdlib.h>
#include "../src/sql_client.h"

START_TEST(test_parse_arguments)
{

} END_TEST

START_TEST(test_open_config_file)
{
	
}END_TEST
 
Suite * sql_client_suite(void)
{
    Suite *s;
    TCase *tc_arguments;

    s = suite_create("SQL-Client");

    /* Arguments test case */
    tc_arguments = tcase_create("Arguments");
    tcase_add_test(tc_arguments, test_parse_arguments);


    /* Config File test case */


    /* ... */


    /* Add test cases to suite */
    suite_add_tcase(s, tc_arguments);

    return s;
}

int main(void)
{
   int number_failed;
   Suite *s;
   SRunner *sr;

   s = sql_client_suite();
   sr = srunner_create(s);

   srunner_run_all(sr, CK_NORMAL);
   number_failed = srunner_ntests_failed(sr);
   srunner_free(sr);
   return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
