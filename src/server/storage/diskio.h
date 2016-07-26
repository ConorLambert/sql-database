/* This file was automatically generated.  Do not edit! */

#define MAX_TABLE_SIZE 5
#define MAX_RECORD_AMOUNT 250
#define MAX_INDEX_AMOUNT 10
#define MAX_INDEX_SIZE 20000
#define SLOT_SIZE 20


typedef struct RecordType {
	int rid;
        int size_of_data; // size of record in bytes
        int size_of_record; // complete size of record including this field
        char *data; // offset within record of data
} Record;


typedef struct HeaderPageType {
        int space_available;
        // TO DO Reference to B-Tree
} HeaderPage;
  

typedef struct PageType {
        char number;
        int space_available;
        int number_of_records;
        int slot_array[SLOT_SIZE];
        int record_type; // fixed or variable length
        Record *records[MAX_RECORD_AMOUNT];
} Page;


typedef struct IndexNodeType {
        char key[50];   // if index is FirstName, this Nodes key may be "John"
        int rid;
        int size_of_node;
} IndexNode;


typedef struct Index {
        char index_name[50]; // named after whatever column is used as an index
        int size; // size of this entire index
        int number_of_nodes;
        IndexNode *indexNodes[MAX_RECORD_AMOUNT]; // pointer to each node NOTE may not need it because I have B-Tree

        // TO DO pointer to a B-Tree to hold all index nodes
        // This B-Tree holds a list of IndexNodes
} Index;


typedef struct Indexes {
        int size;
        int space_available;
	int number_of_indexes;
        Index *indexes[MAX_INDEX_AMOUNT];
} Indexes;

Indexes * createIndexes(char *table_name);

Index * createIndex(char *index_name, Indexes *indexes);

IndexNode * createIndexNode(Index *index, char *key, int rid);
 
int createIndexFile(char *table_name);

int commitIndex(char *destination_file, Index *index); 
 

typedef struct RecordNodeType {
        int rid;
        int page_number;
        int slot_number;
} RecordNode;


typedef struct TableType {
        int size;       // size of all records only i.e. excluding header files
        int rid;        // primary key, increases with each new record added
        int increment;  // how much the primay key increases each new insertion
        int number_of_pages; // total count for all pages of a table
        HeaderPage *header_page;
        Page *pages[MAX_TABLE_SIZE];
        // TO DO Reference to B-TREE
} Table;

Record* createRecord(char *data);
int insertRecord(Record *record, Page *page, Table *table);
int commitRecord(Record *record, Table *table);
Record searchRecord(Table *table, char *condition);
RecordNode createRecordNode(int rid, int page_number, int slot_number);
int insertRecordNode(RecordNode *recordNode);
RecordNode findRecordNode(int rid);
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
