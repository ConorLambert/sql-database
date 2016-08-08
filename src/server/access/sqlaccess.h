/* This file was automatically generated.  Do not edit! */
#include "../storage/diskio.h"

void intitializeDataBuffer();
int addTableToBuffer(char *table_name, Table *table);
int createDatabase(char *name);
int alter();
int drop(char *table);
int create(char *table_name, char *fields[], int number_of_fields);
char *selectRecord(char *condition,char *table_name, char *database_name);
int commit(char *table_name, char *database_name);
int update(char *field,int size,char *value,char *table);
int deleteRecord(char *field,int size,char *table);
int insert(char *data[], int size, char *table_name, char *database_name);

