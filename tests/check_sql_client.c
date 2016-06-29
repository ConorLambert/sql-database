#include <check.h>
#include <stdlib.h>
#include "../src/client/sql_client.h"

START_TEST(test_parse_arguments)
{
	
	fputs("\nTESTING Valid Input\n", stderr);
	char *argv[] = {{"test_program"}, {"-u"}, {"conor"}, {"-h"}, {"44.22.66"}};	
	ck_assert(parseArguments(5, argv) == 1);


	fputs("\nTESTING Option Not Recognised\n", stderr);
	char *argv1[] = {{"test_program"}, {"-l"}, {"conor"}, {"-h"}, {"44.22.66"}};
	ck_assert(parseArguments(5, argv1) == -1);

	
	// NO HOST DEFINED
	printf("\nTESTING No Host Defined\n");
	char *argv2[] = {{"test_program"}, {"-u"}, {"conor"}, {"-h"}};
        ck_assert(parseArguments(4, argv2) == -1);

	
	// NO USERNAME DEFINED
	printf("\nTESTING No Username Defined\n");
        char *argv3[] = {{"test_program"}, {"-u"}, {"-h"}, {"44.22.66"}};
        ck_assert(parseArguments(4, argv3) == -1);

	// NO USERNAME DEFINED
        printf("\nTESTING No Username or Host Defined\n");
        char *argv4[] = {{"test_program"}, {"-u"}, {"-h"}};
        ck_assert(parseArguments(3, argv4) == -1);

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
