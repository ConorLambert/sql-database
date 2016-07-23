/* This file was automatically generated.  Do not edit! */

#define MAX_TABLE_SIZE 5
#define MAX_RECORD_AMOUNT 250
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
        Record records[MAX_RECORD_AMOUNT];
} Page;


typedef struct NodeType {
        int rid;
        int page;
        int slot_number;
} Node;


typedef struct TableType {
        int size;       // size of all records only i.e. excluding header files
        int rid;        // primary key, increases with each new record added
        int increment;  // how much the primay key increases each new insertion
        int number_of_pages; // total count for all pages of a table
        HeaderPage header_page;
        Page pages[MAX_TABLE_SIZE];
        // TO DO Reference to B-TREE
} Table;

Record createRecord(char *data);
int insertRecord(Record record, Table *table);
int commitRecord(Record record, Table *table);
Record searchRecord(Table *table, char *condition);
Node createNode(Page *page, Record *record);
int insertNode(Node *node);
Node findNode(int rid);
Page createPage(Table *table);
void mapPages(Table *table, char *map_table);
void closeMap(char *map_table);
Table initializeTable(char *map_table);
char *mapTable(char *path_to_table);
Table openTable(char *table_name,char *database);
Table createTable(char *table_name);
int pathToTable(char *table_name, char *database, char *destination);
int addPageToTable(Page page, Table *table);
int commitTable(char *table_name, Table *table, char *database_name);
HeaderPage createHeaderPage();
int createFolder(char folder_name);
