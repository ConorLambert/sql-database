//#include "SQL_Module.h"
        
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include <termios.h>
#include "../../libs/libcfu/src/cfuhash.h"

typedef int bool;
#define true 1
#define false 0

int portno = 5001;
int sockfd;
struct sockaddr_in serv_addr;

char password[30];
char username[30] = {0};
char host[30] = {0};
cfuhash_table_t *arguments = NULL;

void argumentInit() {
	
	// setup hash table to hold arguments	
	arguments = cfuhash_new_with_initial_size(50); 
	cfuhash_set_flag(arguments, CFUHASH_FROZEN_UNTIL_GROWS);

	// add arguments to hash table
	cfuhash_put(arguments, "-u", username);
	cfuhash_put(arguments, "-h", host);
	cfuhash_put(arguments, "-p", password);
}


void readMessage(char *buffer, int size) {
	int n = read(sockfd, buffer, size);
	if (n < 0) {
                perror("ERROR reading from socket");
                exit(1);
        }
        buffer[n] = 0;
}


/*
        send response back to user and return the number of characters written
*/
void sendMessage(char *message, int size) {
	printf("Sending message %s\n", message);
        int n = write(sockfd, message, size);
	printf("N = %d\n", n);
        if (n < 0) {
                perror("ERROR writing to socket");
                exit(1);
        }
}



// SENDING AND RECEIVING

void parseInput(char *input, char *destination) {
	// parseQuery()
	puts("Parsing Input\n");
}

void submitRequest(char* request) {
	puts("Submitting Request\n");
	int n = write(sockfd, request, 256);
        if (n < 0) {
        	perror("ERROR writing to socket");
                exit(1);
        }
}

void acceptResponse(char *response) {
	puts("Accepting Response\n");
	int n = read(sockfd, response, 255);
        if (n < 0) {
                perror("ERROR reading from socket");
                exit(1);
        }
}

int parseResponse(char *response) {

	puts("Parsing Response\n");
	/*
	if(parseResponse(response) == -1){
        	perror("ERROR processing request");
               	exit(1);
        }
	*/

	return 0;	
}

void acceptInput() {

        char line[256];
        char input[256];
	char request[256];
	char response[256];

        // infinite loop
        while(1) {
                // display prompt
                fputs(">> ", stderr);
                // accept input
                fgets(line, sizeof line, stdin);
                sscanf(line, "%s", input);
                // if input is "exit"
                if(strcmp(input, "exit") == 0)
                        exit(0); // quit program
                else {
                        fprintf(stdout, "String Entered %s, Characters read : %d\n", input, strlen(line) - 1);
			parseInput(input, request);
			submitRequest(request);
			acceptResponse(response);
			parseResponse(response);
		}
        }
}




// CONNECTING

int connectToServer() {

	char buffer[256];
	puts("Connecting to Server\n");
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
   
	if (sockfd < 0) {
	     perror("ERROR opening socket");
	     return -1;
   	}

	
	puts("bzero\n");	
	bzero((char *) &serv_addr, sizeof(serv_addr));
   	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(5001); 
	

	puts("inet_pton\n");
	printf("host %s\n", host);
	if(inet_pton(AF_INET, cfuhash_get(arguments, "-h"), &serv_addr.sin_addr) <= 0)
    	{
        	printf("\n inet_pton error occured\n");
        	return -1;
    	} 

	char str[256];
	// now get it back and print it
	inet_ntop(AF_INET, &(serv_addr.sin_addr), str, INET_ADDRSTRLEN);
	printf("IP address: %s\n", str); // prints "192.0.2.33"
	

	puts("connect\n");
    	if( connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0)
    	{	
       		printf("\nError : Connect Failed \n");
       		return -1;

    	} 


	puts("bzero buffer\n");
	bzero(buffer, sizeof(buffer));

	puts("read\n");
    	int n = read(sockfd, buffer, sizeof (buffer) - 1);
	if (n < 0) {
        	perror("ERROR reading from socket");
                return -1;
        }

	puts("printing buffer\n");
       	buffer[n] = 0;
       	if(fputs(buffer, stdout) == EOF)	// this will display succes message sent from the server
        {
            		printf("\nError : fputs error\n");
			return -1;
        }

	return 0;
} 	

void sendCredentials() {

	puts("Sending Credentials\n");
	// printf("sizeof -u = %d, sizeof conor = %d\n", sizeof(cfuhash_get(arguments, "-u")));

	char response[10];	

	strcpy(username, cfuhash_get(arguments, "-u"));
	sendMessage(username, sizeof(username));
	readMessage(response, sizeof(response));
	if(strcmp(response, "INVALID") == 0) {
		perror("Server says invalid username");
	}


	strcpy(password, cfuhash_get(arguments, "-p"));
	sendMessage(password, sizeof(password));
	readMessage(response, sizeof(response));
	if(strcmp(response, "INVALID") == 0) {
		perror("Server says invalid password");
	}

}		


void startClient() {
	connectToServer();
	sendCredentials();
	acceptInput();
}


char * promptPasswordEntry() {
	
	int ch = 0;
	int i;
	for(i = 0, ch = getch(); ch != '\n'; ++i) {
		password[i] = ch;
		ch = getch();
	}

	password[i] = '\0';

	// TEST
	printf("Password entered is %s\n", password);
}

int getch() {
    	struct termios oldt, newt;
    	int ch;
    	tcgetattr(STDIN_FILENO, &oldt);
    	newt = oldt;
    	newt.c_lflag &= ~(ICANON | ECHO);
    	tcsetattr(STDIN_FILENO, TCSANOW, &newt);
    	ch = getchar();
    	tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
    	return ch;
}


char * testTriggerPasswordEntry() {
        //char input[30];
        //fputs("Please enter your password: ", stdout);
        //fgets(input, sizeof input, stdin);
        sscanf("somepassword", "%s", password);
        return password;
}


int isOption(char *argument) {

	printf("argument %s\n", argument);
	
	if(cfuhash_exists(arguments, argument)) {
		puts("Returning 0");
		return 0;
	}
	
	return -1;
}


/*
        Input arguments have a pattern of -option argument -option argument
        For example -u username -p password where username is the name of
        the user who wishes to log in and password is the password for that user
*/
int parseArguments(int argc, char *argv[]) {
             	
        // flag indicating the result
        bool passwordRequested = false;

	argumentInit();

	int i;
        for(i = 1; i < argc;)
        {	
		if(argv[i][0] != '-') {
			fputs("Invalid Option\n", stderr);
			return -1;
		}	

		char *option = argv[i];	
		if(strcmp(option, "-p") == 0){
			passwordRequested = true;
			++i;
			continue;
		}

		if(cfuhash_exists(arguments, option)) {
	
			char * argument = cfuhash_get(arguments, option);
			if(argument[0] != 0){
				fputs("Criteria already set\n", stderr);
                        	return -1;	
			} else if (i == argc - 1) {
				fputs("Invalid Argument Input\n", stderr);
				return -1;
			} else {
				if(isOption(argv[i + 1]) == 0)
					return -1;
				// calloc(30, (sizeof (char)));	
				strcpy(argument, argv[i + 1]);
				cfuhash_put(arguments, option, argument);
				i += 2;
			}
		} else {
  			fputs("Option not recognised\n", stderr);
                        return -1;
		}
	}

	if(passwordRequested) {
		fputs("Password: ", stdout);
		promptPasswordEntry();	
	}

	// TESTING
	// cfuhash_pretty_print(arguments, stdout);

        return 0;
}

