#include <check.h>
#include <stdlib.h>
#include "../src/client/sql_client.h"

START_TEST(test_parse_arguments)
{
	
	fputs("\nTESTING Valid Input\n", stderr);
	char *argv[] = {{"test_program"}, {"-u"}, {"conor"}, {"-h"}, {"44.22.66"}};	
	ck_assert(parseArguments(5, argv) == 0);


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

	// NO USERNAME OR HOST DEFINED
        printf("\nTESTING No Username or Host Defined\n");
        char *argv4[] = {{"test_program"}, {"-u"}, {"-h"}};
        ck_assert(parseArguments(3, argv4) == -1);

	//testingPasswords();
} END_TEST

// requires modified source code in associated file to be tested
void testingPasswords() {
	 // VALID PASSWORD INPUT ENDING
        printf("\nTESTING Valid Password Entry - Ending\n");
        char *argv5[] = {{"test_program"}, {"-u"}, {"conor"}, {"-p"}, {"-h"}, {"44.22.66"}};
        ck_assert(parseArguments(6, argv5) == 0);

        // VALID PASSWORD INPUT MIDDLE
        printf("\nTESTING Valid Password Entry - Middle\n");
        char *argv6[] = {{"test_program"}, {"-u"}, {"conor"}, {"-h"}, {"44.22.66"}, {"-p"}};
        ck_assert(parseArguments(6, argv6) == 0);

        // VALID PASSWORD INPUT START
        printf("\nTESTING Valid Password Entry - Start\n");
        char *argv7[] = {{"test_program"}, {"-p"}, {"-u"}, {"conor"}, {"-h"}, {"44.22.66"}};
        ck_assert(parseArguments(6, argv7) == 0);

        // INVALID PASSWORD INPUT POST-UNSERNAME
        printf("\nTESTING Invalid Password Entry - Post-Username\n");
        char *argv8[] = {{"test_program"}, {"-u"}, {"-p"}, {"conor"}, {"-h"}, {"44.22.66"}};
        ck_assert(parseArguments(6, argv8) == -1);

        // INVALID PASSWORD INPUT POST HOST
        printf("\nTESTING Invalid Password Entry - Post-Host\n");
        char *argv9[] = {{"test_program"}, {"-u"}, {"conor"}, {"-h"}, {"-p"}, {"44.22.66"}};
        ck_assert(parseArguments(6, argv9) == -1);
}

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
