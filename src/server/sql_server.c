#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include "access/sqlaccess.h"

int sockfd, newsockfd, clilen;
char buffer[256];
struct sockaddr_in serv_addr, cli_addr;
int portno;

typedef struct credentials {
	char username[30];
	char password[30];
} credentials;

#define VALID "VALID"
#define INVALID "INVALID"



typedef struct Privileges {
	Table *users;
	cfuhash_table_t *user_tables;	// each user has table which lists the tables and whatever privilege the user has on that table	
	cfuhash_table_t *user_dbs;	// each user has table which lists the databases and whatever privilege the user has on that database
} Privileges;

Privileges *privileges;

Privileges * createPrivileges() {
	Privileges *privileges = malloc(sizeof(Privileges));	
	privileges->users = createTable("Users");
	privileges->user_tables = cfuhash_new_with_initial_size(MAX_TABLE_SIZE);
	cfuhash_set_flag(privileges->user_tables, CFUHASH_FROZEN_UNTIL_GROWS);
	privileges->user_dbs = cfuhash_new_with_initial_size(MAX_TABLE_SIZE); 
	cfuhash_set_flag(privileges->user_dbs, CFUHASH_FROZEN_UNTIL_GROWS);
	return privileges;
}

int destroyPrivileges(Privileges *privileges) {
	deleteTable(privileges->users);
	cfuhash_destroy(privileges->user_tables);
	cfuhash_destroy(privileges->user_dbs);
}


/*
	parse request to find out what the request wants
	return integer indicating the success of the parse 
*/
int parseRequest(char request[]) {
	// find out what the request is and act accordingly
	return 0;	
}

void acceptRequest(char buffer[]) {
	int n = read(newsockfd, buffer, 255);   
	if (n < 0) {
		perror("ERROR reading from socket");
		exit(1);
	}

	// parse request and store any response in buffer
	if(parseRequest(buffer) == -1){
		perror("ERROR processing request");
		exit(1);
	}
}

int checkUserInfo(credentials *user) {
	puts("Checking user info\n");
	
	// check login data against User table in database
}

void readMessage(char *buffer, int size) {
        int n = read(newsockfd, buffer, size);
	printf("Characters read %d\n", n);
	
        if (n < 0) {
                perror("ERROR reading from socket");
                exit(1);
        }
	buffer[n] = 0;
}


/*
	send response back to user and return the number of characters written
*/
void sendResponse(char response[]) {
	int n = write(newsockfd, response, 256);
	if (n < 0) {
		perror("ERROR writing to socket");
		exit(1);
	}	
}


credentials * getUserInfo() {

	puts("Getting user info\n");
	credentials *user = malloc(sizeof(credentials));
	bzero(user, sizeof(user));

	// get username and password

	// first get the username
	
	printf("sizeof user->username %d\n", sizeof(user->username));
	readMessage(user->username, sizeof(user->username));
	// send back response indicating validness
	sendMessage(VALID, sizeof(VALID));

	// now get the username
	readMessage(user->password, sizeof(user->password));
        // send back response indicating validness
        sendMessage(VALID, sizeof(VALID));

	printf("Username %s, Password %s\n", user->username, user->password);

	return user;
}


void sendMessage(char message[], int size) {
        int n = write(newsockfd, message, size);
        if (n < 0) {
                perror("ERROR writing to socket");
                exit(1);
        }
}


void processRequests() {
	char buffer[256];
   	bzero(buffer, 256);
	
	fputs("Processing Requests\n", stdout);
	while(1) {
		acceptRequest(buffer);
		sendResponse(buffer);
	}
}


void openSocket() {
	
	fputs("Opening Socket\n", stdout);
	/* First call to socket() function */
   	sockfd = socket(AF_INET, SOCK_STREAM, 0);
   
   	if (sockfd < 0) {
      		perror("ERROR opening socket");
      		exit(1);
   	}		
}	

void initializeSocket() {

	fputs("Initializing Socket\n", stdout);
	/* Initialize socket structure */
   	bzero((char *) &serv_addr, sizeof(serv_addr));
   
   	serv_addr.sin_family = AF_INET;
   	serv_addr.sin_addr.s_addr = INADDR_ANY;
   	serv_addr.sin_port = htons(portno);
}

void bindSocket() {

	fputs("Binding Socket\n", stdout);
	if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
      		perror("ERROR on binding");
      		exit(1);
   	}	
}

void _listen() {

	fputs("Listening\n", stdout);
	listen(sockfd,5);
}

void _accept() {

	fputs("Accepting\n", stdout);
	clilen = sizeof(cli_addr);
   
   	while (1) {
      		newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
		
      		if (newsockfd < 0) {
         		perror("ERROR on accept");
         		exit(1);
      		}
      
      		/* Create child process */
		fputs("Forking\n", stdout);
      		int pid = fork();
		
      		if (pid < 0) {
         		perror("ERROR on fork");
         		exit(1);
      		}
      
      		if (pid == 0) {
         		/* This is the client process */
			fputs("I am the child\n", stdout);
         		close(sockfd);
			sendResponse("Connection Successful, Send me the credentials\n");
			credentials *user = getUserInfo();	
			checkUserInfo(user);
         		processRequests();
         		exit(0);
      		}
      		else {
         		close(newsockfd);
      		}
		
	 } /* end of while */
}


void loadUserTables() {
	// get path to default administrative database "db" and find and load into memory the table Users from this folder
	privileges = malloc(sizeof(Privileges));
	privileges->users = openTable("Users", "db");	// db is the administrative and default database
	privileges->user_tables = cfuhash_new_with_initial_size(MAX_TABLE_SIZE);
	cfuhash_set_flag(privileges->user_tables, CFUHASH_FROZEN_UNTIL_GROWS);
	privileges->user_dbs = cfuhash_new_with_initial_size(MAX_TABLE_SIZE);
	cfuhash_set_flag(privileges->user_dbs, CFUHASH_FROZEN_UNTIL_GROWS);
	
	// for each entry (user) in the table, find and load their associated tables into memory
	int i, j;
	Table *table;
	for(i = 0; i < privileges->users->page_position; ++i) {
		if(!privileges->users->pages[i])
			continue;

		for(j = 0; j < privileges->users->pages[i]->record_position; ++j) {
			if(!privileges->users->pages[i]->records[j])
				continue;

			// get the rid of the user 

				// prepend rid to string "_tables" (Step 1)

				// table = openTable(string, "db");

				// cfuhash_put(privileges->tables, rid, table);
		
				// repeat for dbs from step 1
		}
	}
		
}


void startup() {
	portno = 5001;	

	//loadUserTables();

	openSocket();

	initializeSocket();
	
	bindSocket();
	
	_listen();

	_accept();
}
