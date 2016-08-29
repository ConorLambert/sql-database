#include "../src/server/access/sqlaccess.h"
#include "../libs/libcfu/src/cfuhash.h"
#include "../libs/libbtree/btree.h"

char *test_database;

DataBuffer *dataBuffer;

Table *table1;

Index *index1;

char *table_name1;
char *index_name1;

char *field_first_name1;
char *field_age1;
char *field_date_of_birth1;
char *field_telephone_no1;
char **fields1;

char *first_name1;
char *age1;
char *date_of_birth1;
char *telephone_no1;
char **data1;

char *first_name2;
char *age2;
char *date_of_birth2;
char *telephone_no2;
char **data2;

char *first_name3;
char *age3;
char *date_of_birth3;
char *telephone_no3;
char **data3;

int number_of_fields1;


// UTLITY FUNCTIONS
Table * util_createTable(char *table_name);
void util_createDatabase();
void util_deleteDatabase();
void util_deleteTestFile();
void util_freeDataBuffer();
void util_createTable1();
void util_createFields1();
void util_createAndInsertRecord1();
void util_createAndInsertRecord2();
void util_createAndInsertRecord3();


// SETUP and TEARDOWN
void _setup(void);
void _teardown(void);
