#include "../../../libs/libbtree/btree.h"

// RECORD
#define VARIABLE_LENGTH 0
#define FIXED_LENGTH 1

// BTREE
#define ORDER_OF_BTREE 2

// MAX Values
#define MAX_RECORD_AMOUNT 50    // how many records per page
#define MAX_INDEX_SIZE 20000    // how big can the index file associated with a table be
#define MAX_INDEXES_AMOUNT 10   // used as a limit on the number of pages in the index file
#define MAX_FIELD_AMOUNT 20     // how many fields/columns a table can have
#define MAX_FIELD_SIZE 50       // how big a field name can be
#define MAX_TABLE_SIZE 5        // number of pages



// COMMIT properties
int BLOCK_SIZE;                                 // size of page
const char FILLER = 0;                          // use to fill remaining parts of page when committing
const char STARTING_NODE_MARKER = '{';          // start and end markers for node of btree
const char ENDING_NODE_MARKER = '}';




int BLOCK_SIZE;


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
        Index *indexes[MAX_INDEXES_AMOUNT];
} Indexes;




int insertIndexKey(IndexKey *indexKey, Index *index);

int createIndexFile(char *table_name);

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
	int record_position;
        unsigned long slot_array[MAX_RECORD_AMOUNT];
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
	int page_position;
	int record_type;
        HeaderPage *header_page;
        Page *pages[MAX_TABLE_SIZE];
        Indexes *indexes;
	Format *format;	
} Table;

typedef struct DatabaseType {
	char *name;
	int number_of_tables;
	Table *tables[MAX_TABLE_SIZE];
} Database;

Indexes * createIndexes(Table *table);

int createFormat(Table *table, char *fields[], int number_of_fields);

int getColumnData(Record *record, char *column_name, char *destination, Format *format);
Index * createIndex(char *index_name, Table *table);

int createField(char *type, char *name, Format *format); 
RecordKey * createRecordKey(int rid, int page_number, int slot_number);
RecordKey * findRecordKey(Table *table ,int key);
int insertRecordKey(RecordKey *recordKey, Table *table);
Record *createRecord(char **data, int number_of_fields, int size);
int insertRecord(Record *record, Page *page, Table *table);
unsigned long commitRecord(Record *record, Format *format, FILE *tp, int record_type);
Index * hasIndex(char *field, Table *table);
Record * indexSearch(Index *index, char *value, Table *table);
Record * searchRecord(Table *table, char *field, char *value);
Record * sequentialSearch(char *field, char *value, Table *table);
Page *createPage(Table *table);
void mapPages(Table *table, char *map_table);
void closeMap(char *map_table);
Table* initializeTable(char *map_table);
char *mapTable(char *path_to_table);
Table* openTable(char *table_name,char *database);
Table* createTable(char *table_name);
int deleteTable(Table *table);
int freeHeaderPage(HeaderPage *headerPage);
int freeFormat(Format *format);
int freeIndexes(Indexes *indexes);
int freeIndex(Index *index);
int freePage(Page *page);
int freeRecord(Record *record, int number_of_fields);
int getPathToFile(char *extension, char *table_name, char *database, char *destination);
int commitTable(Table *table, char *table_name, char *database_name); 
HeaderPage* createHeaderPage(Table *table);
int createFolder(char *folder_name);
int deleteFolder(char *name);
int commitFormat(Format *format, FILE *fp);
bt_node * deserializeTree(FILE *fp, char *tree_type, Table *table);
