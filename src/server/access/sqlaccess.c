#include <stdio.h>
#include <stdlib.h>
#include "sqlaccess.h"

#define MAX_RESULT_SIZE 100
#define MAX_RESULT_ROW_SIZE 100


ResultSet * createResultSet() {
	ResultSet *resultSet = malloc(sizeof(ResultSet));
	resultSet->record = NULL;
	resultSet->page_number = 0;
	resultSet->slot_number = -1;
	resultSet->node_pos = malloc(sizeof(node_pos));
	resultSet->node_pos->node = NULL;
	resultSet->node_pos->index = 0;
	resultSet->index = NULL;
	resultSet->next = NULL;
	return resultSet;
}

int addResult(ResultSet *resultSet, Record *record, int page_number, int slot_number) {
	resultSet->record = record;
	resultSet->page_number = page_number;
	resultSet->slot_number = slot_number;
	return 0;
}


// DATA BUFFER

DataBuffer * dataBuffer;

DataBuffer * initializeDataBuffer() {
	dataBuffer = malloc(sizeof(dataBuffer));
	dataBuffer->length = 0;
	dataBuffer->tables = cfuhash_new_with_initial_size(MAX_TABLE_SIZE); 
	cfuhash_set_flag(dataBuffer->tables, CFUHASH_FROZEN_UNTIL_GROWS);
	dataBuffer->resultSet = createResultSet();
	return dataBuffer;
}


bool evaluate_logical(char *logic, char *x, char *y) {
	if(isOperator(logic, GREATER_THAN_SYMBOL_STRING)){
                return atoi(x) >= atoi(y);
        } else if(isOperator(logic, LESS_THAN_SYMBOL_STRING)) {
                return atoi(x) <= atoi(y);
        } else if(isOperator(logic, "!=")) {
                return (strcmp(x, y) != 0);
        } else if(isOperator(logic, "=")) {
                return (strcmp(x, y) == 0);
	} else if(isOperator(logic, ">")) {
		return atoi(x) > atoi(y);
	} else if (isOperator(logic, "<")) {
		return atoi(x) < atoi(y);
	}
}


bool evaluate_binary(char *logic, bool x, bool y) {
	if(isOperator(logic, "&"))
                return x && y;
	else if(isOperator(logic, "|"))
		return x || y;
}


char *getRecordData1(Table *table, char *target_column_name, int page_number, int slot_number) {
	Record *record = getRecord(table, page_number, slot_number);		
	char *result = malloc(record->size_of_data);
	
	getColumnData(record, target_column_name, result, getTableFormat(table));

	return result;
}



//returns -1 if no room left in the table
int addTableToBuffer(char *table_name, Table *table) {
	if(dataBuffer->length < MAX_TABLE_SIZE) {
		cfuhash_put(dataBuffer->tables, table_name, table);
		dataBuffer->length++;
		return 0;
	} else {
		return -1;
	}
}


// CREATE DATABASE
int createDatabase(char *name) {
	createFolder(name);
}


int deleteDatabase(char *name){
	int result = deleteFolder(name);
}


int create(char *table_name, char *column_names[], char *data_types[], int number_of_fields) {
	Table *table = createTable(table_name);
	createIndexes(table);
	createFormat(table, column_names, data_types, number_of_fields);
	addTableToBuffer(table_name, table);		
	return 0;
}


int addConstraintPrimaryKeys(char *target_table_name, int number_of_primary_keys, char **primary_keys) {

	Table *target_table = (Table *) cfuhash_get(dataBuffer->tables, target_table_name);

	int i;
	for(i = 0; i < number_of_primary_keys; ++i) {	
		int pos = locateField(target_table->format, primary_keys[i]);		
		addPrimaryKey(target_table, target_table->format->fields[pos]);	
	}

}


int addConstraintForeignKeys(char *target_table_name, int number_of_foreign_keys, char **foreign_keys, char **foreign_key_names, char **foreign_key_tables) {

	Table *target_table = (Table *) cfuhash_get(dataBuffer->tables, target_table_name);

	int i;
	for(i = 0; i < number_of_foreign_keys; ++i) {	
		Table *origin_table = (Table *) cfuhash_get(dataBuffer->tables, foreign_key_tables[i]);
		int pos_origin = locateField(origin_table->format, foreign_key_names[i]);		
		int pos = locateField(target_table->format, foreign_keys[i]);
		addForeignKey(target_table, origin_table, origin_table->format->fields[pos_origin], target_table->format->fields[pos]);	
	}

}


int insert(char *table_name, char **columns, int number_of_columns, char **data, int number_of_data) {

	printf("\nI HERE %s\n", table_name);

	Table *table;
	
	// if table is not in memory
	if(!cfuhash_exists(dataBuffer->tables, table_name)){
		// add table to memory
		//table = openTable(table_name, database_name);
		addTableToBuffer(table_name, table);
	}
	
	// get table from memory
	table = cfuhash_get(dataBuffer->tables, table_name);
	if(!table) {
		return -1;
	}
			
	Record *record = NULL;

	// RECORD		
	if(number_of_columns > 0) {	// if the query has specified columns

		Format *format = getTableFormat(table);
		
		// initialize record
		record = initializeRecord(getNumberOfFields(format));

		int j, pos = 0;
		for(j = 0; j < number_of_columns; ++j) {
			pos = locateField(format, columns[j]);
			setDataAt(record, pos, data[j]);			
		}
	} else {	// else no columns where specified
		// create record from data
		record = createRecord(data, number_of_data, 0);
	}

	// get last page to insert record
	Page *page = getPage(table, getNumberOfPages(table) - 1);

	// check if there is enough room for the new record
	// is there enough room on the page to insert record
        if((getPageSpaceAvailable(page) - getRecordSize(record)) <= 0) {
		printf("\nCREATING NEW PAGE\n");
		page = createPage(table); // create a new page
	}
        
	// insert record into table 
	insertRecord(record, page, table);

	// create a table record key from the record to place in a B-Tree node
	RecordKey *recordKey = createRecordKey(getRecordRid(record), getPageNumber(page), getPageNumberOfRecords(page) - 1);

	// insert node into table B-Tree
	insertRecordKey(recordKey, table);

	// INDEXES
	// get set of indexes associated with table
	Indexes *indexes = getIndexes(table);
	
	// for every index of the table (excluding primary index)
	int i;
	char buffer[100];
	for(i = 0; i < getNumberOfIndexes(indexes); ++i) {
		
		// fetch the data located underneath that column
		getColumnData(record, getIndexName(getIndexNumber(indexes, i)), buffer, getTableFormat(table));			
		// create index key (from newly inserted record)
		IndexKey *indexKey = createIndexKey(buffer, getRecordRid(record));	
		// insert index key into index
		insertIndexKey(indexKey, getIndexNumber(indexes, i));	
		free(indexKey);
	}

	free(recordKey);
	
	return 0;

}

int destroyResultSet(ResultSet *resultSet) {
	resultSet->record = NULL;
	if(resultSet->node_pos) {
		if(resultSet->node_pos->node)
			free(resultSet->node_pos->node);
		free(resultSet->node_pos);
	}
	resultSet->index = NULL;
	resultSet->next = NULL;
	free(resultSet);
	return 0;

}


int deleteRecords(Table *table) {
	ResultSet *head = dataBuffer->resultSet, *tmp = dataBuffer->resultSet;
	
	// for each record of the in resultSet
	while(tmp != NULL && tmp->record != NULL) {
		if(tmp->record) {
			printf("\ndeleting row, page_number %d, slot_number %d\n", tmp->page_number, tmp->slot_number);
			deleteRow(table, tmp->page_number, tmp->slot_number);
		}

		printf("\nim here\n");
		tmp = tmp->next;		
		destroyResultSet(head);
		head = tmp;
	}

	dataBuffer->resultSet = createResultSet();

	return 0;
}


bool isBinaryNode(Node *node) {
	if(node->left->left)
		return true;
	else
		return false;	
}

bool isSequentialSearch() {
	if(dataBuffer->resultSet->record)
		return true;
	else 
		return false;
}

// for every node in regards to canSearchIndex
	// left subtree is always true
	// if current node value is OR operator then right subtree true (repeat this line for next node)
// bool evaluateExpression1(Node *root, Table *table, bool canIndxSearch, char **data)
	// data will be malloced before calling this function and used to store any record data that has been obtained
	// canIndexSearch along with data will dictate what action occurs at each sub-expression
	// return value dictates what action the parent node will take depending on whether the parent is an AND or OR


// only one index search per stack
bool _evaluateExpression(Node *root, Table *table, bool canIndexSearch, int level) {

	bool result;

	if(isBinaryNode(root)) { // if its a binary operator
		bool left_result, right_result, continue_left = true, continue_right = true;
		printf("\n\t\t\tisBinaryNode\n");
	
		if(root->left)
			left_result = _evaluateExpression(root->left, table, canIndexSearch, level + 1);
		else {
			printf("\n\t\t\tTree Restructured: Follow On\n"); return _evaluateExpression(root->right, table, true, level + 1);
		}

		printf("\n\t\t\tAfter evaluating expression\n");

		// if it was an index search with no result then stop searching the left
		if(!left_result && !isSequentialSearch()) {
			printf("\n\t\t\tIndex search, no result\n"); root->left = NULL;
		}

		if(isOperator(root->value, "|") && left_result){ // if is OR operator and left result is true then return true
			result = true; // continue (dont check right subtree)
			printf("\n\t\t\tis OR and True\n");
			goto end;				
		}
		if(isOperator(root->value, "&") && !left_result){ // if is AND operator and left result is false then return false
			result = false; // continue 		
			printf("\n\t\t\tIs AND and false\n");
			//if(!isSequentialSearch()) {
			//	printf("\nNot sequential\n");
				goto end;
			//}
			
		}	
		
		if(isOperator(root->value, "&") && left_result) {	// if is AND operator and left result is true then check the right subtree
			printf("\n\t\t\tIs AND and true\n");
			right_result = _evaluateExpression(root->right, table, false, level + 1);
		} else if(isOperator(root->value, "|" && !left_result)) {	// if is OR operator and left result is false then check the right subtree
			if(isSequentialSearch()) {// is it sequential search
				printf("\n\t\t\tIs OR and false, isSequential\n");
				right_result = _evaluateExpression(root->right, table, false, level + 1);
			} else {
				printf("\n\t\t\tIs OR and false and Possible Index Search\n");
				right_result = _evaluateExpression(root->right, table, true, level + 1);
			}
		}
	
		printf("\n\t\t\tEvaluating result\n");
		result = evaluate_binary(root->value, left_result, right_result);			
	
		//end: return result;
	} else {

		Record *record = NULL;
		char **data;
		int page_number = 0;
		int slot_number = 0;
		//bool result = false;
		Index *index = NULL;
		node_pos *node_pos = NULL;
		RecordKey *recordKey = NULL;

		printf("\n\t\t\t\tSub-Expression: %s %s %s\n", root->left->value, root->value, root->right->value);

		if(!canIndexSearch) {	// record is already there for us to check against
			// goto part
			printf("\n\t\t\t\t!canIndexSearch\n");
			record = dataBuffer->resultSet->record;
			goto evaluate_result;
		} else if (strcmp(root->left->value, "rid") == 0) {
				printf("\n\t\t\t\tIs rid\n");
				recordKey = findRecordKey(table, atoi(root->right->value));
				goto check_index;
		} else if(index = getIndex(root->left->value, table)) {
				printf("\n\t\t\t\tIs Index\n");
				if(!dataBuffer->resultSet->node_pos->node || dataBuffer->resultSet->index != index) {
					printf("\n\t\t\t\tNew index\n");
					dataBuffer->resultSet->node_pos->node = getIndexBtreeRoot(index);
					if(!dataBuffer->resultSet->node_pos->node)
						printf("\n\t\t\t\tNo index found\n");			
				}
				printf("\n\t\t\t\tFind IndexKeyFrom index, %d\n", dataBuffer->resultSet->node_pos->index);
				dataBuffer->resultSet->index = index;

				IndexKey *indexKey = findIndexKeyFrom(index, dataBuffer->resultSet->node_pos, root->right->value);
				printf("\n\t\t\t\tAfter Find IndexKeyFrom\n");
				if(indexKey != NULL) {
					printf("\n\t\t\t\tIndexKey found index %d, indexKey->key %s\n", dataBuffer->resultSet->node_pos->index, indexKey->key);
					recordKey = findRecordKey(table, indexKey->value);
					dataBuffer->resultSet->node_pos->index++;
					printf("\n\t\t\t\tAfter incrementation index %d\n", dataBuffer->resultSet->node_pos->index);
					node_pos = dataBuffer->resultSet->node_pos;
				} else {
					printf("\n\t\t\t\tIndexKey not found\n");
			}
				
				free(indexKey);
				printf("\ngoto check_index\n");
				goto check_index;
		} else {	
			int i, j;
			printf("\n\t\t\t\tSequential Search\n");
			if(dataBuffer->resultSet->record) {			
				printf("\n\t\t\t\tWe are continuing on from previos search\n");
				if(getLastRecordOfPage(table->pages[dataBuffer->resultSet->page_number]) == dataBuffer->resultSet->record) {
					i = dataBuffer->resultSet->page_number + 1;	// move to the next page of the current records page
					j = 0;						// reset j back to 0 to start at the first record on the next page
				} else {
					i = dataBuffer->resultSet->page_number;
					j = dataBuffer->resultSet->slot_number + 1;	// else move to the next slot array position
				}	
			} else {
				printf("\n\t\t\t\tPrevious result had no match\n");
				i = dataBuffer->resultSet->page_number;
				j = dataBuffer->resultSet->slot_number + 1;
		 	}
		
			for(; i < table->page_position; ++i){
				if(table->pages[i] == NULL)
					continue;
			
				printf("\nfor records, j = %d, record_position = %d\n", j, table->pages[i]->record_position);
				for(; j < table->pages[i]->record_position; ++j) {
					if(table->pages[i]->records[j] == NULL)
						continue;
				
					record = table->pages[i]->records[j];	
					page_number = i;
					slot_number = j;
					printf("\n\t\t\t\tNext record is page %d, slot %d\n", page_number, slot_number);

					goto evaluate_result;
				}
			}
		}

		// if we have found a match for our delete query, then delete that row from the table
		check_index: 
			printf("\n\t\t\t\tchecking index\n");
			if(recordKey != NULL) {
				printf("\n\t\t\t\trecordKey not NULL\n");
				record = getRecord(table, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));
				page_number = getRecordKeyPageNumber(recordKey);
				slot_number = getRecordKeySlotNumber(recordKey);				
				free(recordKey);
				result = true;
				goto check_result;
				printf("\nAdded record\n");
			} else {
				printf("\nRecord does not exist\n");
				record = NULL;
				result = false;
				goto check_result;
			}

		// if record found	
		evaluate_result: 
			printf("\nEvaluating result\n");	
			data = getRecordData(record);
			int pos = locateField(getTableFormat(table), root->left->value);
			result = evaluate_logical(root->value, data[pos], root->right->value);	
			printf("\nResult evaluated to %d\n", result);
			
		check_result:	
			printf("\n\t\t\t\tcheck_result %d\n", result);
			if(record) {
				printf("\n\t\t\t\twe have a record\n");
				if(dataBuffer->resultSet->record != record) {					
					printf("\n\t\t\t\tdifferent record\n");
					if(dataBuffer->resultSet->record) {						
						printf("\n\t\t\t\tAlready record there\n");
						dataBuffer->resultSet->next = createResultSet();
						dataBuffer->resultSet = dataBuffer->resultSet->next;
					}	
					printf("\n\t\t\t\tAdding result\n");
					addResult(dataBuffer->resultSet, record, page_number, slot_number);				
					dataBuffer->resultSet->index = index;	// if not an index search then these will null anyway
					dataBuffer->resultSet->node_pos = node_pos;	
				}
			}		

		printf("\n\t\t\t\tend_result, %d\n", result);
		//return result;	
	}

	end:
	// if we are at the root node and the result was false, then clear any record in the data buffer for the next search
	if(level == 0) {	
		printf("\nLevel is 0, result is %d, !result = %d\n", result, !result);
		if(!result && !dataBuffer->resultSet->index){	// record always remains in the buffer until we have definitively found out we dont need it
			printf("\n\t\t\tNegative result\n");
			dataBuffer->resultSet->record = NULL;
		}
	}
	printf("\n\t\t\tReturning result\n");
	return result;	
}


int evaluateExpression(Node *root, Table *table) {
	printf("\n\t\tevaluateExpression\n");
	Page *last_page = table->pages[table->page_position - 1];
	Record *last_record = last_page->records[last_page->record_position - 1];
	ResultSet *head = dataBuffer->resultSet;
	bool result;
	int i = 0;

	printf("\n\t\tbefore while true\n");
	while(true) {
		result = _evaluateExpression(root, table, true, 0);		
		if(last_record == dataBuffer->resultSet->record){	// if its the last record 
			printf("\n\t\tisLastRecord\n"); break;
		} else if((dataBuffer->resultSet->page_number == table->page_position - 1) && (dataBuffer->resultSet->slot_number == last_page->record_position - 1)){
			printf("\n\t\texhausted\n"); break;
		}else if(dataBuffer->resultSet->index && !result) {	// if the it was an index search and the result was false 
			printf("\n\t\tindex search, no results\n"); break;
		}
		printf("\nend of iteration\n");
	}
	printf("\n\t\tafter while true\n");
	// reset resultSet back to head again for further procesing
	dataBuffer->resultSet = head;

	if(head->record)
		return 0;	// TO DO: return number of records that returned true
	else
		return -1;
	
}
	


/* 
	first_name = 'Conor' AND age = 40
	last_name = 'PHILIP' OR (first_name = 'Conor' AND age = 40)
*/
int deleteRecord1(char *table_name, char *where_clause) {
	printf("\nIN DELETE RECORD 1 %s\n", where_clause);

	// overall we want to find the page and slot number of where the record is located
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	Index *index = NULL;
	RecordKey *recordKey = NULL;
	Record *record = NULL;

	Node *root = buildExpressionTree(where_clause);	
	if(evaluateExpression(root, table) == 0) {
		printf("\n\t\tdeleteRecords(table)\n");
		deleteRecords(table);
	} else {
		printf("\nNo records found\n");
		destroyResultSet(dataBuffer->resultSet);
		dataBuffer->resultSet = createResultSet();
	}

	

	// else no match was found so return -1
	return 0;	
}


int deleteRecord(char *database_name, char *table_name, char *condition_column_name, char *condition_value) {

	// overall we want to find the page and slot number of where the record is located
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	Index *index;
	RecordKey *recordKey = NULL;
	Record *record;

	printf("\nIn delete record\n");

	// check if the condition column is the rid or an index for quicker search, else perform a sequential search	
	// what is returned in each case is the page_number and slot_number of the record
	if(strcmp(condition_column_name, "rid") == 0) {
		recordKey = findRecordKey(table, atoi(condition_value));
	} else if((index = hasIndex(condition_column_name, table)) != NULL) {
		IndexKey *indexKey = findIndexKey(index, condition_value);
		if(indexKey != NULL) 
			recordKey = findRecordKey(table, indexKey->value);
		free(indexKey);
	} else {
		record = sequentialSearch(condition_column_name, condition_value, table, 0, 0);
		if(record != NULL) 
			recordKey = findRecordKey(table, record->rid);			
	}
	
	// if we have found a match for our delete query, then delete that row from the table
	if(recordKey != NULL) {
		deleteRow(table, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));
		free(recordKey);
		return 0;
	}

	
	// TO DO
	// move the record_position of the page to the preceeding record


	// else no match was found so return -1
	return -1;	
}


int update(char *field, int size, char *value, char *table) {	
	return -1;
}




char **selectRecordRid(Table *table, char *target_column_name, char *condition_column_name, char *condition_value) {

	printf("\npeforming record key search\n");

	Record *record;
       	RecordKey *recordKey = NULL;

	// result set variables
	char **result = malloc(sizeof(char *));
	
	recordKey = findRecordKey(table, atoi(condition_value));				
	
	// if we have found a match for our query
	if(recordKey != NULL) {			
		result[0] = getRecordData1(table, target_column_name, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));	
		free(recordKey);    
		recordKey = NULL; 
		return result;
	} else {
		free(result);
		return NULL;
	}
}


char **selectRecordIndex(Table *table, Index *index, char *target_column_name, char *condition_column_name, char *condition_value) {

	Record *record;       
        RecordKey *recordKey = NULL;

	// result set variables
	char **result = malloc(getHighestRid(table) * sizeof(char *));
	int k = 0;

	// index search variables
	node_pos *starting_node_pos = malloc(sizeof(node_pos));
	starting_node_pos->node = getIndexBtreeRoot(index);
	starting_node_pos->index = 0;

	while(1) { 
		IndexKey *indexKey = findIndexKeyFrom(index, starting_node_pos, condition_value);		
		if(indexKey != NULL) {
			recordKey = findRecordKey(table, getIndexKeyValue(indexKey));
			++starting_node_pos->index;				
			free(indexKey);
		}
		
		// if we have found a match for our query
		if(recordKey != NULL) {			
			result[k++] = getRecordData1(table, target_column_name, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));	
			free(recordKey);    
			recordKey = NULL;
		} else {
			break;	
		}
	}

	free(starting_node_pos);

	// if we got at least one record, return the result set
	if(k > 0)
		return result;		
	else {
		free(result);
		return NULL;
	}

}


char **selectRecordSequential(Table *table, char *target_column_name, char *condition_column_name, char *condition_value) {

       	Record *record = NULL;
        RecordKey *recordKey = NULL;

	// result set variables
	// mutiple records may be found
	char **result = malloc(table->rid * sizeof(char *));
	int k = 0;

	printf("\nafter node_pos initializing\n");

	// sequential search variables
	int i = 0, j = 0; // i is page number, j is slot_array and k is for result set

	while(1) { 
		record = sequentialSearch(condition_column_name, condition_value, table, i, j);
		
		printf("\nstart of sequential search\n");
	
		if(record != NULL) {
			recordKey = findRecordKey(table, getRecordRid(record));			
			Page *page = getPage(table, getRecordKeyPageNumber(recordKey));

			// check which counters need incrementing (and decrementing)
			if(getLastRecordOfPage(page) == record) {
				i = getRecordKeyPageNumber(recordKey) + 1;	// move to the next page of the current records page
				j = 0;					// reset j back to 0 to start at the first record on the next page
			} else {
				i = getRecordKeyPageNumber(recordKey);
				j = getRecordKeySlotNumber(recordKey) + 1;	// else move to the next slot array position
			}	

			printf("\nafter sequential search %s, i = %d, k = %d, j = %d\n", record->data[0], i, k, j);				
		}

		// if we have found a match for our query
		if(recordKey != NULL) {				
			result[k++] = getRecordData1(table, target_column_name, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));	
			free(recordKey);    
			recordKey = NULL;
		} else {
			break;	
		}
	}	

	// if we got at least one record, return the result set
	if(k > 0)
		return result;
	else {
		free(result);		
		return NULL;
	}
}


// returns target column data
char **selectRecord(char *database_name, char *table_name, char *target_column_name, char *condition_column_name, char *condition_value) {
	
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	Index *index = NULL;    
	
	// check if the condition column is the rid or an index for quicker search, else perform a sequential search
	if(strcmp(condition_column_name, "rid") == 0)
		return selectRecordRid(table, target_column_name, condition_column_name, condition_value);	
	else if((index = hasIndex(condition_column_name, table)) != NULL) 
		return selectRecordIndex(table, index, target_column_name, condition_column_name, condition_value); 
	else 
		return selectRecordSequential(table, target_column_name, condition_column_name, condition_value);		
	
}


int commit(char *table_name, char *database_name) {
	Table *table = cfuhash_get(dataBuffer->tables, table_name);

	commitTable(table, table_name, database_name);
	
	return 0;
}


int drop(char *table_name) {

	if(cfuhash_exists(dataBuffer->tables, table_name)){		
		printf("\ncfuhash_exists\n");
		// get table
		Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
		printf("\ndeleting table\n");	
		// free table (and all its entries)
		deleteTable(table);

		printf("cfuhashdelete");
		// remove table from dataBuffer
		cfuhash_delete(dataBuffer->tables, table_name);
		
		return 0;

	} else {

		return -1;

	}
}


// TO DO
int alterRecord(char *database_name, char *table_name, char *target_column_name, char *target_column_value, char *condition_column_name, char *condition_value) {

	// overall we want to find the page and slot number of where the record is located
        Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
        Index *index;
	Record *record;
        RecordKey *recordKey = NULL;

        // check if the condition column is the rid or an index for quicker search, else perform a sequential search
        // what is returned in each case is the page_number and slot_number of the record
        if(strcmp(condition_column_name, "rid") == 0) {
                recordKey = findRecordKey(table, atoi(condition_value));
        } else if((index = hasIndex(condition_column_name, table)) != NULL) {
                IndexKey *indexKey = findIndexKey(index, condition_value);
                if(indexKey != NULL)
                        recordKey = findRecordKey(table, getIndexKeyValue(indexKey));
                free(indexKey);
        } else {
                record = sequentialSearch(condition_column_name, condition_value, table, 0, 0);
                if(record != NULL)
                        recordKey = findRecordKey(table, getRecordRid(record));
        }

        // if we have found a match for our delete query, then delete that row from the table
        if(recordKey != NULL) {
                record = getRecord(table, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));
		int pos = locateField(getTableFormat(table), target_column_name);
        	setDataAt(record, pos, target_column_value);
                free(recordKey);
	}

	return -1;	
}


int alterTableAddColumns(char *table_name, char **identifiers, char **types, int number_of_identifiers) {
	// get table
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);

	int i;
	for(i = 0; i < number_of_identifiers; ++i) {
		// create new field setting field and type
		createField(types[i], identifiers[i], table->format);
	}
}


int alterTableDropColumns(char *table_name, char **column_names, int number_of_columns) {

	// get table
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);

	// locate field to be deleted
	Format *format = getTableFormat(table);

	int x;
	for(x = 0; x < number_of_columns; ++x) {

		int pos = locateField(format, column_names[x]);			

		// shift deleted field + 1 down		
		int i, j, k, pc, rc;

		Field *field = NULL;
		// IDEA:
		// could be getNumberOfFields - pos
		for( i = pos, field = getField(format, i); i < getNumberOfFields(format) -1; ++i, field = getField(format, i)) 
			table->format->fields[i] = table->format->fields[i + 1];
		table->format->fields[i] = NULL;	// set the previously unavailable field to available

		Page *page = NULL;
		Record *record = NULL;
		char *data = NULL;
		// for each page of the table
		for(i = 0, pc = 0; pc < getNumberOfPages(table) && i < MAX_TABLE_SIZE; ++i){
			page = getPage(table, i);

			if(page == NULL)
				continue;
		
			// for each record of that table
			for(j = 0, rc = 0; rc < getPageNumberOfRecords(page) && j < MAX_RECORD_AMOUNT; ++j) {
				record = getRecord(table, i, j);

				if(record == NULL)
					continue;
				
				free(getDataAt(record, pos));	
				
				for(k = pos; k < getNumberOfFields(format) -1; ++k) {
					printf("\nIN: %d - %s , %s\n", j, table->pages[i]->records[j]->data[k], table->pages[i]->records[j]->data[k + 1]);
					table->pages[i]->records[j]->data[k] = table->pages[i]->records[j]->data[k + 1];
				}

				table->pages[i]->records[j]->data[k] = NULL;
							
				++rc;	
			}

			++pc;
		}
		
		
		table->format->number_of_fields--;

	}

	return 0;
}


int alterTableRenameTable(char *table_name, char *new_name) {
	// cfuhash_replace	
	//Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	
}


int alterTableRenameColumn(char *table_name, char *target_column, char *new_name) {
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);

	Format *format = getTableFormat(table);
	int pos = locateField(format, target_column);
	Field *field = getField(format, pos);	
	setName(field, new_name);
			
	return 0;
}

