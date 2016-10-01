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
	resultSet->status = -1;	
	return resultSet;
}

int addResult(ResultSet *resultSet, Record *record, int page_number, int slot_number) {
	resultSet->record = record;
	resultSet->page_number = page_number;
	resultSet->slot_number = slot_number;
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








/***************************************************************************************** DATABASE ****************************************************************************************************/

int createDatabase(char *name) {
	createFolder(name);
}


int deleteDatabase(char *name){
	int result = deleteFolder(name);
}









/************************************************************************************** TABLE ***********************************************************************************************************/

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







/*********************************************************************************** INSERT *************************************************************************************************************/

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









/******************************************************************************** EXPRESSION ************************************************************************************************/

bool isBinaryNode(Node *node) {
	if(isBinaryOperator(node->value))
		return true;
	else
		return false;
}


bool isPending() {
	if(dataBuffer->resultSet->status == PENDING_SUCCESSFUL || dataBuffer->resultSet->status == PENDING_UNSUCCESSFUL)
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
	printf("\nroot->value %s\n", root->value);

	if(isBinaryNode(root)) { // if its a binary operator
		bool left_result, right_result, continue_left = true, continue_right = true;
		printf("\n\t\t\tisBinaryNode\n");
		
		if(root->left)
			left_result = _evaluateExpression(root->left, table, canIndexSearch, level + 1);
		else {
			printf("\n\t\t\tTree Restructured: Follow On\n"); 
			result = _evaluateExpression(root->right, table, true, level + 1);	
			goto end;
		}

		printf("\n\t\tAfter left subtree\n");
	
		// record status
		STATUS temp_status = dataBuffer->resultSet->status;

		// if root value is AND
		if(isOperator(root->value, "&")) {
			printf("\n\t\tROOT AND:\n");
			// if !left result , return false
			if(!left_result) {
				printf("\n\t\t\tROOT AND: Left Was False\n");
				result = false;
				goto end;
			} else { // left result is true
				printf("\n\t\t\tROOT AND: Left Was True\n");
				// set status to PENDING SUCCESSFUL
				dataBuffer->resultSet->status = PENDING_SUCCESSFUL;

				//call right subtree with no index search allowed					
				right_result = _evaluateExpression(root->right, table, false, level + 1);
			}
		} else if(isOperator(root->value, "|")) { // root value is OR
				printf("\n\t\tROOT OR:\n");
				// if left_result, return true
				if(left_result) {
					printf("\n\t\t\tROOT OR: Left Was True\n");
					result = true;
					goto end;
				} else { // left result is false/unsuccessful
					// if record was an index search
					if(dataBuffer->resultSet->status == INDEX_SEARCH_UNSUCCESSFUL) {
						printf("\n\t\t\tROOT OR: Left Was Index Search Unsuccessful\n");
						// no more root left
						root->left = NULL;
						//call right subtree allowing index search
						right_result = _evaluateExpression(root->right, table, true, level + 1);
					} else if(dataBuffer->resultSet->status == SEQUENTIAL_SEARCH_UNSUCCESSFUL){ // record was sequential search
						printf("\n\t\t\tROOT OR: Left Was Sequential Search\n");
						// set status to PENDING_UNSUCCESSFUL
						dataBuffer->resultSet->status = PENDING_UNSUCCESSFUL;
						// call right subtree with no index search	
						right_result = _evaluateExpression(root->right, table, false, level + 1);		
					}
				}
		}
	
		// evaluate left_result && right_result
		printf("\n\t\tEvaluating result\n");
		result = evaluate_binary(root->value, left_result, right_result);			
	} else {
				
		Record *record = NULL;
		char **data;
		int page_number = 0;
		int slot_number = 0;
		Index *index = NULL;
		node_pos *node_pos = NULL;
		RecordKey *recordKey = NULL;
		STATUS status;

		printf("\n\t\t\t\tSub-Expression: %s %s %s\n", root->left->value, root->value, root->right->value);
		
		// if status is PENDING_X (i.e checking against record already obtained)
		if(dataBuffer->resultSet->status == PENDING_SUCCESSFUL || dataBuffer->resultSet->status == PENDING_UNSUCCESSFUL) {
			printf("\n\t\t\t\tPending\n");
			// these variables are initialized to 0 above but if we checking against a record already in the buffer then we want these variables to match that record in the buffer
			record = dataBuffer->resultSet->record;
			page_number = dataBuffer->resultSet->page_number;
			slot_number = dataBuffer->resultSet->slot_number;
			goto evaluate_result;				
		} else if(canIndexSearch && (getIndex(root->left->value, table) || strcmp(root->left->value, "rid") == 0)) {
			 if (strcmp(root->left->value, "rid") == 0) {
				printf("\n\t\t\t\tIs rid\n");
				recordKey = findRecordKey(table, atoi(root->right->value));
				goto check_index;
			} else if(index = getIndex(root->left->value, table)) {
				printf("\n\t\t\t\tIs Index\n");
				// if the previous index search was successful then we already have the index we want to search
				// MAY not need this
				if(dataBuffer->resultSet->status != INDEX_SEARCH_SUCCESSFUL && !dataBuffer->resultSet->index){
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
					printf("\n\t\t\t\tIndexKey found index %d, indexKey->key %s, indexKey->val %d\n", dataBuffer->resultSet->node_pos->index, indexKey->key, indexKey->value);
					recordKey = findRecordKey(table, indexKey->value);
					dataBuffer->resultSet->node_pos->index++;	// increment the index to allow another index search on the same index 
					printf("\n\t\t\t\tAfter incrementation index %d\n", dataBuffer->resultSet->node_pos->index);
					node_pos = dataBuffer->resultSet->node_pos;	// 
				} else {
					printf("\n\t\t\t\tIndexKey not found\n");
					
				}
				
				free(indexKey);
				printf("\ngoto check_index\n");
				goto check_index;				
			}
		} else { // perform sequential search
			printf("\n\t\t\t\tHas No Index: Performing Sequential Search\n");
			// if status is SEQUENTIAL_SEARCH_SUCCESSFUL OR SEQUENTIAL_SEARCH_UNSUCCESSFUL or first time peforming a sequential search
			// start search after previous record	(make sure not on last record of page)
			if(dataBuffer->resultSet->status == SEQUENTIAL_SEARCH_SUCCESSFUL || dataBuffer->resultSet->status == SEQUENTIAL_SEARCH_UNSUCCESSFUL)
				printf("\n\t\t\tContuining on from Previous Search\n");

			int i, j;
			if(getLastRecordOfPage(table->pages[dataBuffer->resultSet->page_number]) == dataBuffer->resultSet->record) {
				i = dataBuffer->resultSet->page_number + 1;	// move to the next page of the current records page
				j = 0;						// reset j back to 0 to start at the first record on the next page
			} else {
				if(dataBuffer->resultSet->record)
					printf("\nrid = %d, dataBuffer->resultSet->slot_number %d\n", dataBuffer->resultSet->record->rid, dataBuffer->resultSet->slot_number);
				else
					printf("\nno record there\n");
				i = dataBuffer->resultSet->page_number;
				j = dataBuffer->resultSet->slot_number + 1;	// else move to the next slot array position
			}
		
			// we arrive here no matter which of the above if else happens	
			// obtain the next record in the sequence
			// the reason for for loops is because the next record may not be in the next slot position
			for(; i < table->page_position; ++i) {
				if(table->pages[i] == NULL)
					continue;
		
				printf("\nfor records, j = %d, last_record_position = %d\n", j, table->pages[i]->last_record_position);
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
			printf("\n\t\t\tfor ended with nothing found at %d\n", j);
		}

	
		// if we have found a match for our delete query, then delete that row from the table
		check_index: 
			printf("\n\t\t\t\tchecking index\n");
			if(recordKey != NULL) {
				printf("\n\t\t\t\trecordKey not NULL, page_number %d, slot_number %d\n", getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));
				record = getRecord(table, getRecordKeyPageNumber(recordKey), getRecordKeySlotNumber(recordKey));
				page_number = getRecordKeyPageNumber(recordKey);
				slot_number = getRecordKeySlotNumber(recordKey);				
				free(recordKey);
				result = true;
				status = INDEX_SEARCH_SUCCESSFUL;
				goto check_result;
				printf("\nAdded record\n");
			} else {
				printf("\nRecord does not exist\n");
				record = NULL;
				result = false;
				status = INDEX_SEARCH_UNSUCCESSFUL;				
				goto check_result;
			}

		// if record found	
		evaluate_result: 
			printf("\nEvaluating result of %s %s %s\n", root->left->value, root->value, root->right->value);	
			data = getRecordData(record);
			int pos = locateField(getTableFormat(table), root->left->value);
			printf("\ndata[pos] = %s\n", data[pos]);
			result = evaluate_logical(root->value, data[pos], root->right->value);			
			if(result)
				status = SEQUENTIAL_SEARCH_SUCCESSFUL;
			else
				status = SEQUENTIAL_SEARCH_UNSUCCESSFUL;
			printf("\nResult evaluated to %d\n", result);
			
		check_result:	
			printf("\n\t\t\t\tcheck_result %d\n", result);
			
			// if its not pending, and theres a record already there then create a new one
			// the only time a record should be in the buffer is between left and right subtrees NOT between iterations
			// so if there is a record there and its not pending then this must be from the previous iteration (which was successful)	
			if(dataBuffer->resultSet->record)
				printf("\nThere is record : isPending()? %d\n", isPending());
			else
				printf("\nThere is no record there\n");
			if(!isPending() && dataBuffer->resultSet->record) {					
				printf("\n\t\t\tCreating New Result Set\n");
				dataBuffer->resultSet->next = createResultSet();
				dataBuffer->resultSet = dataBuffer->resultSet->next;
			} // if the above fails then there is already a result set there to be filled
			
			// the only time a record should be in the buffer is between left and right subtrees NOT between iterations
			// however, page number, slot number and node_pos should remain in the buffer between iterations whether successful or not
			// the only time we should be in the right subtree is either:
			//						if the root operator is AND and left_result is true
			//						or the root operator is OR and right_result is false
			if(status == SEQUENTIAL_SEARCH_SUCCESSFUL || status == SEQUENTIAL_SEARCH_UNSUCCESSFUL) {
				printf("\n\t\t\tstatus == SEQUENTIAL_SEARCH_SUCCESSFUL || status == SEQUENTIAL_SEARCH_UNSUCCESSFUL\n");				
				dataBuffer->resultSet->record = record;
				dataBuffer->resultSet->page_number = page_number;
				dataBuffer->resultSet->slot_number = slot_number;
				dataBuffer->resultSet->status = status;
			} else if(status == INDEX_SEARCH_SUCCESSFUL) {
				printf("\n\t\t\tstatus == INDEX_SEARCH_SUCCESSFUL\n");
				// the index criteria has already been filled in during the index process above
				dataBuffer->resultSet->record = record;
				dataBuffer->resultSet->page_number = page_number;
				dataBuffer->resultSet->slot_number = slot_number;
				dataBuffer->resultSet->status = status;
			} else if(status == INDEX_SEARCH_UNSUCCESSFUL){				
				printf("\n\t\t\tstatus == INDEX_SEARCH_UNSUCCESSFUL\n");
				// reset result set back to default and return
				dataBuffer->resultSet->record = NULL;
				dataBuffer->resultSet->index = NULL;
				dataBuffer->resultSet->node_pos = malloc(sizeof(node_pos));	
				dataBuffer->resultSet->node_pos->node = NULL;
				dataBuffer->resultSet->node_pos->index = 0;
				dataBuffer->resultSet->page_number = 0;
				dataBuffer->resultSet->slot_number = -1;
				dataBuffer->resultSet->status = status;
			}

			printf("\n\t\t\t\tend_result, %d\n", result);	
			//return result;
	}

	end:
	// if we are at the root node and the result was false, then clear any record in the data buffer for the next search
	if(level == 0) {	
		printf("\nLevel is 0, result is %d, !result = %d\n", result, !result);
		if(!result){	// record always remains in the buffer until we have definitively found out we dont need it
			printf("\n\t\t\tNegative result\n");
			// in the case of a sequential search, the record will be removed BUT the page_number and slot_number will remain so the next iteration of the while loop can continue on from the next record in the sequence
			dataBuffer->resultSet->record = NULL;
		}
	}

	printf("\n\t\t\tReturning result\n");
	return result;	
}


int evaluateExpression(Node *root, Table *table) {
	printf("\n\t\tevaluateExpression\n");
	Page *last_page = table->pages[table->page_position - 1];
	Record *last_record = getLastRecordOfPage(last_page);
	ResultSet *head = dataBuffer->resultSet;
	bool result;
	int i = 0;

	printf("\n\t\tbefore while true\n");
	while(true) {
		printf("\nstart of iteration\n");
		result = _evaluateExpression(root, table, true, 0);
		if(last_record == dataBuffer->resultSet->record){	// if its the last record 
			printf("\nisLastRecord\n");
			break;
		} else if((dataBuffer->resultSet->page_number == table->page_position - 1) && (dataBuffer->resultSet->slot_number == last_page->last_record_position)){
			printf("\n\t\texhausted\n"); break;
		}else if(dataBuffer->resultSet->status == INDEX_SEARCH_UNSUCCESSFUL) {	// if the it was an index search and the result was false 
			printf("\n\t\tindex search, no results\n"); 
			break;
		}
		
		printf("\n\n\n\n\n\nEND of iteration\n");		
	}

	printf("\n\t\tafter while true\n");
	// reset resultSet back to head again for further procesing
	dataBuffer->resultSet = head;

	if(head->record) {
		printf("\nhead->record->rid = %d\n", head->record->rid);
		return 0;
	} // TO DO: return number of records that returned true
	else
		return -1;
	
}
	




/************************************************************************************ DELETE ************************************************************************************************************/

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


int deleteRecord(char *table_name, char *where_clause) {
	printf("\nIN DELETE RECORD 1 %s\n", where_clause);

	// overall we want to find the page and slot number of where the record is located
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	if(table->number_of_pages == 1 && table->pages[0]->number_of_records == 0) {
		printf("\nNo more records to search\n");
		return -1;	// no records to delete
	}

	Index *index = NULL;
	RecordKey *recordKey = NULL;
	Record *record = NULL;

	printf("\nbefore build expression tree\n");
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








/********************************************************************************** SELECT **************************************************************************************************************/

char ***_selectRecords(Table *table, char **target_columns, int number_of_target_columns) {
	ResultSet *head = dataBuffer->resultSet, *tmp = dataBuffer->resultSet;
	char ***result = malloc(MAX_RESULT_ROW_SIZE * sizeof( char ***));
	char **data;
	
	int i = 0, j, pos;
	// for each record of the in resultSet
	while(tmp != NULL && tmp->record != NULL) {
		if(tmp->record) {
			printf("\nselecting row, page_number %d, slot_number %d\n", tmp->page_number, tmp->slot_number);
			result[i] =  malloc(MAX_TARGET_COLUMNS * sizeof(char **));
			// for each target_column
			for(j = 0; j < number_of_target_columns; ++j) {
				printf("\ntarget_colmn[%d] = %s\n", j, target_columns[j]);
				data = getRecordData(tmp->record);
				pos = locateField(table->format, target_columns[j]);
				result[i][j] = data[pos];				
				printf("\nresult[%d][%d] = %s\n", i, j, result[i][j]);
			}
		}

		tmp = tmp->next;		
		destroyResultSet(head);
		head = tmp;
		++i;
	}

	// TO DO: Use a certain character or character sequence to indicate the end of the result set so the client can properly loop through the result set
	return result;		
}


char ***selectRecord(char *table_name, int number_of_tables, char **target_columns, int number_of_target_columns, char *conditions) {
	printf("\n\t\t\tIn Select Record\n");
	
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	Index *index = NULL;    
	char ***result = NULL;

			
	if(table->number_of_pages == 1 && table->pages[0]->number_of_records == 0) {
		printf("\nNo more records to search\n");
		return -1;	// no records to delete
	}


	printf("\nbefore build expression tree\n");
	Node *root = buildExpressionTree(conditions);	
	
	if(evaluateExpression(root, table) == 0) {
		printf("\n\t\tdeleteRecords(table)\n");
		result = _selectRecords(table, target_columns, number_of_target_columns);
	} else {
		printf("\nNo records found\n");
		result = NULL;
		destroyResultSet(dataBuffer->resultSet);
	}
	
	dataBuffer->resultSet = createResultSet();	

	return result;
}








/****************************************************************************************** UPDATE *****************************************************************************************************/

int _update(Table *table, char **columns, char **values, int number_of_columns) {
	ResultSet *head = dataBuffer->resultSet, *tmp = dataBuffer->resultSet;
	
	int i = 0, j, pos;
	// for each record of the in resultSet
	while(tmp != NULL && tmp->record != NULL) {
		if(tmp->record) {			
			for(j = 0; j < number_of_columns; ++j) { 	
				printf("\naltering row, page_number %d, slot_number %d\n", tmp->page_number, tmp->slot_number);
				printf("\ncolumns[%d] = %s\n", j, columns[j]);
				int pos = locateField(getTableFormat(table), columns[j]);
				printf("\n%d, values[%d] = %s\n", pos, j, values[j]);
        			setDataAt(tmp->record, pos, values[j]);
			}
		}

		tmp = tmp->next;		
		destroyResultSet(head);
		head = tmp;
	}

	return 0;		
}


int update(char *table_name, char **columns, char **values, int number_of_columns, char *conditions){	
	printf("\n\t\t\tIn Select Record\n");
	
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	Index *index = NULL;    
	int result;		
	
	if(table->number_of_pages == 1 && table->pages[0]->number_of_records == 0) {
		printf("\nNo more records to search\n");
		return -1;	// no records to delete
	}

	printf("\nbefore build expression tree\n");
	Node *root = buildExpressionTree(conditions);	
	
	if(evaluateExpression(root, table) == 0) {
		printf("\n\t\tupdateRecords(table)\n");
		result = _update(table, columns, values, number_of_columns);
	} else {
		printf("\nNo records found\n");
		result = NULL;
		destroyResultSet(dataBuffer->resultSet);
	}
	
	dataBuffer->resultSet = createResultSet();	
	return result;
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



int alterTableAddColumns(char *table_name, char **identifiers, char **types, int number_of_identifiers) {
	printf("\nIn alter table add columns\n");
	// get table
	Table *table = (Table *) cfuhash_get(dataBuffer->tables, table_name);

	int i;
	for(i = 0; i < number_of_identifiers; ++i) {
		printf("\nin for loop\n");
		// create new field setting field and type
		createField(types[i], identifiers[i], table->format);
		printf("\nend of for loop\n");
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




/*********************************************************************** JOIN ***********************************************************************************/

char *extractCondition(char *start) {
	char *end = start + 1;
	while(end[0] != ' ' && end[0] != '\0')
		++end;
	char *condition = malloc((end - start) + 1);
	strlcpy(condition, start, (end - start) + 1);		
	return condition;
}


int evaluateJoin(char *table_name, char *join_table, char *join_type, char *on_conditions, char *where_conditions) {
	// parse on_conditions based on which field goes to which table
	char *table1_condition;
	char *table2_condition;

	Table *table1 = (Table *) cfuhash_get(dataBuffer->tables, table_name);
	Table *table2 = (Table *) cfuhash_get(dataBuffer->tables, join_table);

	// check which table, if any (or both) have the target join columns as indexes or primary keys for faster search
	char *end = strstr(on_conditions, ".");
	char *start = end + 1;
	if(strncmp(table_name, on_conditions, end - on_conditions) == 0) {
		table1_condition = extractCondition(start);
		while(start[0] != '.') // move all the way to the next condition
			++start;		
		++start; // nudge to starting letter
		table2_condition = extractCondition(start);
	} else {
		table2_condition = extractCondition(start);
		while(start[0] != '.') // move all the way to the next condition
			++start;		
		++start;	// nudge to starting letter
		table1_condition = extractCondition(start);
	}

	printf("\ntable1_condition = %s, table_condition2 = %s\n", table1_condition, table2_condition);


	ResultSet *head = dataBuffer->resultSet;

	// is table 2 condition a primary key there	
	bool is_join_condition_a_join_table_primary_key = isPrimaryKey(table2, table2_condition);
	bool is_join_condition_a_join_table_index_key;
	
	if(hasIndex(table2_condition, table2)) {
		printf("\nhasIndex\n");
		is_join_condition_a_join_table_index_key = true;
	} else {
		printf("\nhas no index\n");
		is_join_condition_a_join_table_index_key = false;
	}
	bool is_table_condition_a_table_primary_key = isPrimaryKey(table1, table1_condition);
	bool is_table_condition_a_table_index_key;
	if(hasIndex(table1_condition, table1))
		is_table_condition_a_table_index_key = true;
	else
		is_table_condition_a_table_index_key = false;
	
	
	// fastest search
	if(is_join_condition_a_join_table_primary_key && is_table_condition_a_table_index_key) {
		printf("\nFastest Search\n");
		// create tracker for table1 index position (table1 and table2 could have had their values inserted in different orders)
			// for each index node in table1
			// get the index nodes data
			// perform index search on table2 based on join_type
				// if found
					// add to result set
			// loop back		

	} else if(is_join_condition_a_join_table_index_key && is_table_condition_a_table_index_key)  {
		// same as above
		printf("\nFastest search ii\n");

	} else {
		// slower seqeuntial search (but enhance it slightly by seeing if either one are an index)
		printf("\nSlower search\n");

		
		// for each record in table1 
		int i = 0, j = 0;
		Record *record;	
		char *data;	

		for(i = 0; i < table1->page_position; ++i){
			if(table1->pages[i] == NULL)
				continue;
	
			// for each record of that table
			for(j = 0; j < table1->pages[i]->record_position; ++j) {
				if(table1->pages[i]->records[j] == NULL)
					continue;
	
				data = getDataAt(table1->pages[i]->records[j], locateField(table1->format, table1_condition)); 
				printf("\ndata %s\n", data);
				record = sequentialSearch(table2_condition, data, table2, 0, 0);
				if(record) {
					dataBuffer->resultSet->record = record;
					dataBuffer->resultSet->next = createResultSet();
					dataBuffer->resultSet = dataBuffer->resultSet->next;
				}
			}
		}
	}

	free(table1_condition);
	free(table2_condition);

	if(head->record) {
		printf("\nRecords found\n");
	} else {
		printf("\nNo records found\n");
	}
}


// SELECT column_name(s) FROM table1 INNER JOIN table2 ON table1.column_name=table2.column_name;
char *** selectInnerJoin(char *table_name, char *join_table, char **display_fields, int number_of_display_fields, char *on_conditions, char *where_conditions, char **order_bys) {
	printf("\nIn Select Inner Join\n");	

	evaluateJoin(table_name, join_table, "INNER JOIN", on_conditions, where_conditions);	
	
	return NULL;
}


char *** selectJoin(char *table_name, char *join_table, char *join_type, char **display_fields, int number_of_display_fields, char *on_conditions, char *where_conditions, char **order_bys) {
	printf("\nIn Select Join, join_type %s\n", join_type);
	// find what join and execute appropriate path
	if(strcmp(join_type, "INNER JOIN") == 0)
		return selectInnerJoin(table_name, join_table, display_fields, number_of_display_fields, on_conditions, where_conditions, order_bys);

	return NULL;	
}


	
