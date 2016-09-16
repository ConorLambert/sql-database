#include "check_utility.h"


// UTLITY FUNCTIONS
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


void util_freeDataBuffer() {
	cfuhash_destroy (dataBuffer->tables);
	free(dataBuffer);
}

void util_initializeFields() {

	column_names1[0] = "FIRST_NAME";
	column_names1[1] = "AGE";
	column_names1[2] = "DATE_OF_BIRTH";
	column_names1[3] = "TELEPHONE_NO";

	data_types1[0] = "VARCHAR";
	data_types1[1] = "INT";
	data_types1[2] = "VARCHAR";
	data_types1[3] = "CHAR(7)";

        number_of_fields1 = 4;
}


void util_createTable1(){
	table_name1 = malloc(strlen("test_table1") + 1);
	strcpy(table_name1, "test_table1");
}


void util_createFields1() {
	util_initializeFields();
        create(table_name1, column_names1, data_types1, number_of_fields1);
}


void util_createAndInsertRecord1() {
        first_name1 = malloc(strlen("Conor") + 1);
        strcpy(first_name1, "Conor");
        age1 = malloc(strlen("33") + 1);
        strcpy(age1, "33");
        date_of_birth1 = malloc(strlen("12-05-1990") + 1);
        strcpy(date_of_birth1, "12-05-1990");
        telephone_no1 = malloc(strlen("086123456") + 1);
        strcpy(telephone_no1, "086123456");
        data1 = malloc(4 * sizeof(char *));
        data1[0] = first_name1;
        data1[1] = age1;
        data1[2] = date_of_birth1;
        data1[3] = telephone_no1;

	insert(data1, number_of_fields1, table_name1, test_database); // INSERT
}


void util_createAndInsertRecord2() {
	first_name2 = malloc(strlen("Damian") + 1);
        strcpy(first_name2, "Damian");
        age2 = malloc(strlen("44") + 1);
        strcpy(age2, "44");
        date_of_birth2 = malloc(strlen("05-09-1995") + 1);
        strcpy(date_of_birth2, "05-09-1995");
        telephone_no2 = malloc(strlen("086654321") + 1);
        strcpy(telephone_no2, "086654321");
        data2 = malloc(4 * sizeof(char *));
        data2[0] = first_name2;
        data2[1] = age2;
        data2[2] = date_of_birth2;
        data2[3] = telephone_no2;
	
	insert(data2, number_of_fields1, table_name1, test_database); 
}


void util_createAndInsertRecord3() {
	first_name3 = malloc(strlen("Freddie") + 1);
        strcpy(first_name3, "Freddie");
        age3 = malloc(strlen("55") + 1);
        strcpy(age3, "55");
        date_of_birth3 = malloc(strlen("24-02-1965") + 1);
        strcpy(date_of_birth3, "24-02-1965");
        telephone_no3 = malloc(strlen("08624681") + 1);
        strcpy(telephone_no3, "08624681");
        data3 = malloc(4 * sizeof(char *));
        data3[0] = first_name3;
        data3[1] = age3;
        data3[2] = date_of_birth3;
        data3[3] = telephone_no3;

	insert(data3, number_of_fields1, table_name1, test_database);
}


// SETUP and TEARDOWN
void _setup(void) {
        test_database = malloc(strlen("test_database") + 1);
        strcpy(test_database, "test_database");
        util_createDatabase();
        dataBuffer = initializeDataBuffer();
  
	util_createTable1();
   
	util_createFields1();
        
        table1 = (Table *) cfuhash_get(dataBuffer->tables, table_name1);

        createFormat(table1, column_names1, data_types1, number_of_fields1);

        index_name1 = "FIRST_NAME";
        index1 = createIndex(index_name1, table1);

	util_createAndInsertRecord1(); 
        util_createAndInsertRecord2();
        util_createAndInsertRecord3();
}


void _teardown(void) {
	printf("\nbefore drop table\n");
        drop(table_name1);
	printf("\ndropped table\n");
	util_freeDataBuffer(dataBuffer);
	printf("\nfreeing buffer\n");
        util_deleteDatabase();
	printf("\n\delewted database\n");
	
}
