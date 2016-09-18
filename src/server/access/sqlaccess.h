/* This file was automatically generated.  Do not edit! */
#include "../storage/diskio.h"
#include "../../../libs/libcfu/src/cfuhash.h"
#include "../../../libs/libutility/utility.h"


#define MAX_TARGET_COLUMNS 20
#define MAX_CONDITIONS 5
#define MAX_CONDITION_COLUMNS 20
#define MAX_CONDITION_VALUES MAX_CONDITION_COLUMNS
#define MAX_TABLES 5

#define GREATER_THAN_SYMBOL '$'
#define LESS_THAN_SYMBOL '#'
#define MAX_TARGET_COLUMNS 20
#define MAX_CONDITIONS 5
#define MAX_CONDITION_COLUMNS 20
#define MAX_CONDITION_VALUES MAX_CONDITION_COLUMNS
#define MAX_TABLES 5

#define GREATER_THAN_SYMBOL '$'
#define LESS_THAN_SYMBOL '#'



typedef struct DataBufferType {
	int length;
        cfuhash_table_t *tables;	
} DataBuffer;

DataBuffer * initializeDataBuffer();
int addTableToBuffer(char *table_name, Table *table);
int createDatabase(char *name);
int deleteDatabase(char *name);
int alterRecord(char *database_name, char *table_name, char *target_column_name, char *target_column_value, char *condition_column_name, char *condition_value);
int drop(char *table);
int create(char *table_name, char *column_names[], char *data_types[], int number_of_fields);
char **selectRecord(char *database_name, char *table_name, char *target_column_name, char *condition_column_name, char *condition_value);
//char **selectRecord(char **tables, int number_of_tables, char **target_columns, int number_of_target_columns, char *conditions);
int addConstraintForeignKeys(char *target_table_name, int number_of_foreign_keys, char **foreign_keys, char **foreign_key_names, char **foreign_key_tables);
int addConstraintPrimaryKeys(char *target_table_name, int number_of_primary_keys, char **primary_keys);
int commit(char *table_name, char *database_name);
int update(char *field,int size,char *value,char *table);
//int update(char *table_name, char **columns, char **values, int number_of_columns, char *where_conditions);
int deleteRecord(char *database_name, char *table_name, char *condition_column_name, char *condition_value);
//int deleteRecord(char *table_name, char *where_conditions)

int insert(char *table_name, char **columns, int number_of_columns, char **data, int number_of_data);
int alterTableRenameTable(char *table_name, char *new_name);
//int alterTableAddColumn(char *database_name, char *table_name, char *column_name, char *data_type);
int alterTableAddColumns(char *table_name, char **identifiers, char **types, int number_of_identifiers);
//int alterTableDropColumns(char *database_name, char *table_name, char *column_name);
int alterTableDropColumns(char *table_name, char **column_names, int number_of_columns);
//int alterTableRenameColumn(char *database_name, char *table_name, char *target_column, char *new_name);
int alterTableRenameColumn(char *table_name, char *target_column, char *new_name);

