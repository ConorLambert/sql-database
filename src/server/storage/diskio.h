#include "../../../libs/libbtree/btree.h"

#define MAX_TABLE_SIZE 5
#define MAX_TABLE_AMOUNT 10
#define MAX_RECORD_AMOUNT 2 
#define MAX_INDEX_AMOUNT 10
#define MAX_INDEX_SIZE 20000
#define SLOT_SIZE 20
#define ORDER_OF_BTREE 5
#define MAX_NODE_AMOUNT MAX_RECORD_AMOUNT / ORDER_OF_BTREE   // maximum numb    er of nodes per index.

#define MAX_FORMAT_SIZE 30
#define MAX_FIELD_AMOUNT 20
#define MAX_FIELD_SIZE 50


typedef struct IndexKey {
	char *key;
	int value;
	int size_of_key;
} IndexKey;

typedef struct Index {
        char index_name[50]; // named after whatever column is used as an index
        int header_size; // size of this entire index
	int btree_size;
	btree *b_tree;
} Index;


typedef struct Indexes {
        int size;
        int space_available;
	int number_of_indexes;
        Index *indexes[MAX_INDEX_AMOUNT];
} Indexes;



Index * createIndex(char *index_name, Indexes *indexes);

int insertIndexKey(IndexKey *indexKey, Index *index);

int createIndexFile(char *table_name);

int commitIndex(char *destination_file, Index *index); 

typedef struct RecordKeyValue {
	int page_number;
	int slot_number;
} RecordKeyValue;
 
typedef struct RecordKey {
         int rid;
         RecordKeyValue *value;
} RecordKey;







typedef struct RecordType {
	int rid;
	int number_of_fields;
        int size_of_data; // size of record in bytes
        int size_of_record; // complete size of record including this field
        char **data; // array of strings where the ith string represents the ith columns data
} Record;


typedef struct HeaderPageType {
        int space_available;
        btree *b_tree;
} HeaderPage;
  

typedef struct PageType {
        char number;
        int space_available;
        int number_of_records;
        int slot_array[SLOT_SIZE];
        int record_type; // fixed or variable length
        Record *records[MAX_RECORD_AMOUNT];
} Page;


typedef struct Field {
        int size;
        char type[20];
        char name[MAX_FIELD_SIZE];
} Field;



// FORMAT FUNCTIONALITY
// each field in *fields consists of the size of the name of the field, the name of the field itself and the field type
// each field will be a char*
// field positions gives the offset of a field within the *fields i.e. the 0th field is at position 0, the 1th field is at position 1
typedef struct Format {
        int number_of_fields;
        int format_size;
        Field *fields[MAX_FIELD_AMOUNT];
} Format;



typedef struct TableType {
        int size;       // size of all records only i.e. excluding header files
        int rid;        // primary key, increases with each new record added
        int increment;  // how much the primay key increases each new insertion
        int number_of_pages; // total count for all pages of a table
        HeaderPage *header_page;
        Page *pages[MAX_TABLE_SIZE];
        Indexes *indexes;
	Format *format;	
} Table;

typedef struct DatabaseType {
	char *name;
	int number_of_tables;
	Table *tables[MAX_TABLE_AMOUNT];
} Database;


Indexes * createIndexes(Table *table);
// create a format struct from format "sql query"
int createFormat(Table *table, char *fields[], int number_of_fields);

int getColumnData(Record *record, char *column_name, char *destination, Format *format);

int createField(char *type, char *name, Format *format); 
RecordKey * createRecordKey(int rid, int page_number, int slot_number);
RecordKey * findRecordKey(Table *table ,int key);
int insertRecordKey(RecordKey *recordKey, Table *table);
Record *createRecord(char **data, int number_of_fields, int size);
int insertRecord(Record *record, Page *page, Table *table);
int commitRecord(Record *record, Table *table);
Record * searchRecord(Table *table, char *condition);
Page *createPage(Table *table);
void mapPages(Table *table, char *map_table);
void closeMap(char *map_table);
Table* initializeTable(char *map_table);
char *mapTable(char *path_to_table);
Table* openTable(char *table_name,char *database);
Table* createTable(char *table_name);
int getPathToFile(char *extension, char *table_name, char *database, char *destination);
int commitTable(char *table_name, Table *table, char *database_name);
HeaderPage* createHeaderPage(Table *table);
int createFolder(char *folder_name);
int deleteFolder(char *name);
