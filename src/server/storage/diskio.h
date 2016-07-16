/* This file was automatically generated.  Do not edit! */
extern int page_number;
extern int AUTO_INCREMENT;
extern int rid;
#define SLOT_SIZE 20
#define PAGE_SIZE 512
struct Record {
        int _rid;
        char *data; // column data
        int size; // size of record in bytes
};
struct Page {
        int number;
        int space_available;
        int number_of_records;
        // last address position
        void* slot_array[SLOT_SIZE]; // TO DO: define size
};
struct Node {
        int _rid;
        int page;
        int slot_number;
};
struct Page createPage();
struct Record createRecord(char *data);
int append(struct Record record);
int addSlot(char *address, struct Page page);
int insertNode(struct Node node);
struct Node createNode(struct Page page);
int addNode(struct Page page);


