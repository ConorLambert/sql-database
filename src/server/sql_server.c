#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include "../networking/networking.h"

int sockfd, newsockfd, clilen;
struct sockaddr_in serv_addr, cli_addr;
int portno = 5001;

typedef struct credentials {
	char username[30];
	char password[30];
} credentials;

#define VALID "VALID"
#define INVALID "INVALID"

/*
	parse request to find out what the request wants
	return integer indicating the success of the parse 
*/
int parseRequest(char request[]) {
	// find out what the request is and act accordingly
	return 0;	
}

void acceptRequest(char buffer[]) {
	puts("Accepting request\n");
	readMessage(newsockfd, buffer, sizeof(buffer));
	
	// parse request and store any response in buffer
	if (parseRequest(buffer) == -1){
		perror("ERROR processing request");
		exit(1);
	}
}

void processRequests() {
	char buffer[256];
   	bzero(buffer, 256);
	
	fputs("Processing Requests\n", stdout);
	while(1) {
		acceptRequest(buffer);
		sendMessage(sockfd, buffer, sizeof(buffer));
	}
}

credentials * getUserInfo() {

	puts("Getting user info\n");
	credentials *user = malloc(sizeof(credentials));
	bzero(user, sizeof(user));

	// get username and password

	// first get the username
	readMessage(sockfd, user->username, sizeof(user->username));
	// send back response indicating validness
	sendMessage(sockfd, VALID, sizeof(VALID));

	// now get the username
	readMessage(sockfd, user->password, sizeof(user->password));
        // send back response indicating validness
        sendMessage(sockfd, VALID, sizeof(VALID));
}

int checkUserInfo() {
	puts("Checking user info\n");
	// check login data against User table in database
}

void startup() {
	puts("Starting up\n");
	sockfd = openSocket();

	serverInitializeServerAddress(serv_addr, portno);

	bindSocket(sockfd, serv_addr);
	char str[255];
        inet_ntop(AF_INET, &serv_addr.sin_addr, str, INET_ADDRSTRLEN);
        printf("Listening on port %d at address %s\n", portno, str); // prints "192.0.2.33"
	
	listenForClient(sockfd);

	sockfd = acceptNewClient(sockfd, cli_addr);
	
	// child process from here
	char *message = "Connection Successful, Send me the credentials \n";
	sendMessage(sockfd,  message, sizeof(message));

	credentials *user = getUserInfo();	

	printf("Username: %s, Password: %s\n", user->username, user->password);

	checkUserInfo();

        processRequests();

	exit(0);
}
