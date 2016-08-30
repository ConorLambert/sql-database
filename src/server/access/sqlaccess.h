/* This file was automatically generated.  Do not edit! */
#include "../storage/diskio.h"
#include "../../../libs/libcfu/src/cfuhash.h"

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
int create(char *table_name, char *fields[], int number_of_fields);
char **selectRecord(char *database_name, char *table_name, char *target_column_name, char *condition_column_name, char *condition_value);
int addConstraintForeignKey(char *target_table_name, char *origin_table_name, char *field);
int commit(char *table_name, char *database_name);
int update(char *field,int size,char *value,char *table);
int deleteRecord(char *database_name, char *table_name, char *condition_column_name, char *condition_value);
int insert(char *data[], int size, char *table_name, char *database_name);
int alterTableChangeColumn(char *database_name, char *table_name, char *target_column, char *new_name);
int alterTableDeleteColumn(char *database_name, char *table_name, char *column_name);
int alterTableAddColumn(char *database_name, char *table_name, char *column_name, char *data_type);
