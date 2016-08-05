/* This file was automatically generated.  Do not edit! */

#define MAX_TABLE_SIZE 5
#define MAX_RECORD_AMOUNT 250
#define MAX_INDEX_AMOUNT 10
#define MAX_INDEX_SIZE 20000
#define SLOT_SIZE 20
#define ORDER_OF_BTREE 5
#define MAX_NODE_AMOUNT MAX_RECORD_AMOUNT / ORDER_OF_BTREE   // maximum numb    er of nodes per index.


typedef struct IndexNode IndexNode;
typedef struct RecordNode RecordNode;

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


Indexes * createIndexes(char *table_name);

Index * createIndex(char *index_name, Indexes *indexes);

int insertIndexKey(IndexKey *indexKey, Index *index);

int createIndexFile(char *table_name);

int commitIndex(char *destination_file, Index *index); 
 
typedef struct RecordKey {
         int rid;
         int page_number;
         int slot_number;
} RecordKey;


RecordKey createRecordKey(int rid, int page_number, int slot_number);
int insertRecordKey(RecordKey *recordKey, RecordNode *rootNode);
RecordKey * findRecordKey(int rid);
RecordNode * createRecordNode();


typedef struct RecordType {
	int rid;
        int size_of_data; // size of record in bytes
        int size_of_record; // complete size of record including this field
        char *data; // offset within record of data
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


typedef struct TableType {
        int size;       // size of all records only i.e. excluding header files
        int rid;        // primary key, increases with each new record added
        int increment;  // how much the primay key increases each new insertion
        int number_of_pages; // total count for all pages of a table
        HeaderPage *header_page;
        Page *pages[MAX_TABLE_SIZE];
        Indexes *indexes;
	
} Table;


Record *createRecord(char *data);
int insertRecord(Record *record, Page *page, Table *table);
int commitRecord(Record *record, Table *table);
Record searchRecord(Table *table, char *condition);
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
