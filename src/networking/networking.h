#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>


void readMessage(int sockfd, char *buffer, int size);

void sendMessage(int sockfd, char message[], int size);

int openSocket();

void serverInitializeServerAddress(struct sockaddr_in serv_addr, int portno);

void clientInitializeClientAddress(struct sockaddr_in serv_addr, int portno, const char *host);

void bindSocket(int sockfd, struct sockaddr_in serv_addr);

void listenForClient(int sockfd);

int acceptNewClient(int sockfd, struct sockaddr_in cli_addr);

void establishConnection(int sockfd, struct sockaddr_in serv_addr);

void outputMessage(char *message);

int connectToServer(const char *host, int portno);



