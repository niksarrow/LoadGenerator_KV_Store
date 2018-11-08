#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <errno.h>
#include <signal.h>
#include <wait.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <map>
using namespace std;
#define NUM_OF_THREADS 1000

#define MAX_INPUT_SIZE 256
#define MAX_TOKEN_SIZE 64
#define MAX_NUM_TOKENS 16

#define SLEEP_NANOSEC 100
#define SIZE 4000
#define BS 1024 //Buffer Size OR SHARED MEMORY.

map<int,char*> mapOfWords;

int buffer[BS];
int put_index=-1;//index at which next item is to be produced
int get_index=-1;//index at which next item to be consumed
void * consume_numbers(void * id);
void * master(void *data);
void generate_numbers();
pthread_cond_t cv_full = PTHREAD_COND_INITIALIZER;
pthread_cond_t cv_empty = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;
struct sockaddr_in cliaddr;
int serv_fd;
char *ip;

char **tokenize(char *line)
{
  char **tokens = (char **)malloc(MAX_NUM_TOKENS * sizeof(char *));
  char *token = (char *)malloc(MAX_TOKEN_SIZE * sizeof(char));
  int i, tokenIndex = 0, tokenNo = 0;

  for(i =0; i < strlen(line); i++){

    char readChar = line[i];

    if (readChar == ' ' || readChar == '\n' || readChar == '\t'){
      token[tokenIndex] = '\0';
      if (tokenIndex != 0){
	tokens[tokenNo] = (char*)malloc(MAX_TOKEN_SIZE*sizeof(char));
	strcpy(tokens[tokenNo++], token);
	tokenIndex = 0; 
      }
    } else {
      token[tokenIndex++] = readChar;
    }
  }
 
  if(token!=NULL){
  	free(token);
  	token=NULL;
  }
  tokens[tokenNo] = NULL ;
  return tokens;
}

void *master(void *data)
{	
  	int port = *((int *)data);
  	struct sockaddr_in servaddr;
    //1. Socket File Descriptor
    if((serv_fd=socket(AF_INET,SOCK_STREAM,0)) == -1){
     	printf("Server Socket Creation Failed\n");
     	exit(1);
    }
     
    memset(&servaddr, 0, sizeof(servaddr));

	servaddr.sin_family=AF_INET;
	servaddr.sin_port=htons(port);
	servaddr.sin_addr.s_addr=inet_addr(ip);
 	//2. Bind
    if(bind(serv_fd,(struct sockaddr *)&servaddr,sizeof(servaddr))<0){ 
     	printf("Error in binding\n");
     	printf("Oh dear, something went wrong with bind()! %s\n", strerror(errno));
     	exit(1);
    }
     //3. Listen
    listen(serv_fd,5);
     //4. accept
	pthread_t thread[NUM_OF_THREADS];
	int ret_val[NUM_OF_THREADS];
	long parent_id = pthread_self();
	for(int i=0;i<NUM_OF_THREADS;i++){
		int id=i;
		if(ret_val[i]=pthread_create(&thread[i],NULL,consume_numbers,&id))
			printf("%d:,Thread Creation Failed\n!!",i);		
	}
	if(pthread_self()==parent_id)
		generate_numbers();
	for(int i=0;i<NUM_OF_THREADS;i++){
		pthread_join(thread[i],NULL);
	}
	
}

char free_tokens(char ** tokens){

	for(int i=0;tokens[i]!=NULL;i++){
		if(tokens[i]!=NULL)
			free(tokens[i]);
    }
    if(tokens!=NULL){
    	free(tokens);
    } 
}
int execute(char **tokens, int* cli_fd){

	const char * msg_c = "create";
	const char * msg_r = "read";
	const char * msg_u = "update";
	const char * msg_d = "delete";
	const char * msg_di = "disconnect";
	if(tokens == NULL){
		//printf("tokens is NULL\n");
		close(*cli_fd);
		*cli_fd = -1;
		return -1;
	}	
	if(tokens[0] == NULL){
		printf("tokens[0] is NULL %ld\n",pthread_self());
		close(*cli_fd);
		*cli_fd = -1;
		return -1;
	}
	if(!strcmp(tokens[0],msg_c)){
		int key = atoi(tokens[1]);
		if(mapOfWords.find(key) == mapOfWords.end()){
     		char *new_value = (char *) malloc((strlen(tokens[3]) + 1) * sizeof(char));
			bzero(new_value, strlen(tokens[3]) + 1);
			strcpy(new_value, tokens[3]);
     		mapOfWords[key] = new_value;

     		const char *msg_reply = "Key Value created.";
	     	if(write(*cli_fd,msg_reply,strlen(msg_reply))<0){
				printf("Error Writing to socket\n");
			}
		}
		else{
	     	const char *msg_reply = "Key already exist at server.";
			if(write(*cli_fd,msg_reply,strlen(msg_reply)) < 0){
				printf("Error Writing to socket\n");
			}
		}
	}
	else if(!strcmp(tokens[0],msg_di)){
     	const char *msg_reply = "OK";
	    if(write(*cli_fd,msg_reply,strlen(msg_reply))<0){
			printf("Error Writing to socket\n");
		}
		close(*cli_fd);
		*cli_fd = -1;
	}
	else if(!strcmp(tokens[0],msg_r)){
		if(tokens[1] == NULL){
			const char* msg_reply = "No key present.";
			write(*cli_fd,msg_reply,strlen(msg_reply));
			printf("tokens[1] is NULL %ld\n",pthread_self());
			close(*cli_fd);
			*cli_fd = -1;
			return -1;
		}
     	int key = atoi(tokens[1]);
		char* value = NULL;
		const char* msg_reply = "This key doesn't exist.";
		map<int, char*>::iterator it = mapOfWords.find(key);
		
		if (it != mapOfWords.end() && it->second != NULL) {
			size_t length = strlen(it->second);
			value = (char*) malloc((length + 1) * sizeof(char));
			bzero(value, (length + 1));
			strcpy(value, it->second);
		}
		if(value == NULL || strlen(value) == 0){
			if(write(*cli_fd,msg_reply,strlen(msg_reply)) < 0){
				printf("Error Writing to socket\n");
			}
		}	
		else{
			// memset(&buffer_msg, 0, SIZE);
			// strcpy(buffer_msg,value);
			if(write(*cli_fd,value,strlen(value))<0){
				printf("Error writing to client socket\n");
			}
		}
		if(value != NULL){
			free(value);
		}
	}
	

	else if(!strcmp(tokens[0],msg_u)){
		if(tokens[1] == NULL){
			const char* msg_reply = "No key present.";
			write(*cli_fd,msg_reply,strlen(msg_reply));
			printf("tokens[1] is NULL %ld\n",pthread_self());
			close(*cli_fd);
			*cli_fd = -1;
			return -1;
		}
		if(tokens[2] == NULL){
			const char* msg_reply = "No length value present.";
			if(write(*cli_fd,msg_reply,strlen(msg_reply))<0){
				close(*cli_fd);
				*cli_fd = -1;
				return -1;
			}
			printf("tokens[2] is NULL %ld\n",pthread_self());
			close(*cli_fd);
			*cli_fd = -1;
			return -1;
		}
		int key = atoi(tokens[1]);
		if(mapOfWords[key] == NULL){
			const char* msg_reply = "No such key present in Hash Table.";
			
			if(write(*cli_fd,msg_reply,strlen(msg_reply)) < 0){
				printf("Error Writing to socket\n");
				close(*cli_fd);
				*cli_fd = -1;
				return -1;
			}
		}
		else{
			//free(mapOfWords[key]);
			char *new_value = (char *) malloc((strlen(tokens[3]) + 1) * sizeof(char));
				  bzero(new_value, strlen(tokens[3]) + 1);
				  strcpy(new_value, tokens[3]);
				  mapOfWords[key] = new_value;
		     	
				const char* msg_reply = "Key Value Updated.";
				if(write(*cli_fd,msg_reply,strlen(msg_reply)) < 0){
					printf("Error Writing to socket\n");
					close(*cli_fd);
					*cli_fd = -1;
					return -1;
				}
		}
     	
    }

    else if(!strcmp(tokens[0],msg_d)){
    	if(tokens[1] == NULL){
			const char* msg_reply = "No key present.";
			write(*cli_fd,msg_reply,strlen(msg_reply));
			printf("tokens[1] is NULL %ld\n",pthread_self());
			close(*cli_fd);
			return -1;
		}
     	int key = atoi(tokens[1]);
     	if (mapOfWords[key] == NULL){
     		const char* msg_reply = "No such key present in Hash Table.";
			if(write(*cli_fd,msg_reply,strlen(msg_reply)) < 0){
				printf("Error Writing to socket\n");
				close(*cli_fd);
				*cli_fd = -1;
				return -1;
			}
     	}
		else{
			
				mapOfWords.erase(key);
 				const char* msg_reply = "Key Value pair deleted successfully.";
			
				if(write(*cli_fd,msg_reply,strlen(msg_reply)) < 0){
					printf("Error Writing to socket\n");
					close(*cli_fd);
					*cli_fd = -1;
					return -1;
				}	
		}	
	}
	

	return 0;

}
void * consume_numbers(void *id){

	char *buffer_msg = NULL;
	char  **tokens;
	int cli_fd;

	while(1){
		pthread_mutex_lock(&mutex1);
	    while (get_index == -1) {
	        printf("Buffer empty. Thread %ld sleeping.\n", pthread_self());
	    	pthread_cond_wait(&cv_empty,&mutex1);
	    }
			printf("\n Thread Active: %ld",pthread_self());
		printf("%d\n",get_index );	
		cli_fd=buffer[get_index];
		buffer[get_index]=-1;
		if(get_index==put_index){	
			get_index=put_index=-1;		
		}
		else{
			get_index=(get_index+1)%BS;
		}	 
		pthread_cond_signal(&cv_full);
		pthread_mutex_unlock(&mutex1);


		while(1){

			buffer_msg = (char *) malloc(MAX_INPUT_SIZE * sizeof(char));
			bzero(buffer_msg,MAX_INPUT_SIZE);
			if(cli_fd<0)
			{
				break;
			}
			if(read(cli_fd,buffer_msg,MAX_INPUT_SIZE) < 0){
				perror("196 Error: Reading from client\n");
				close(cli_fd);
				break;
			}
			else{
				tokens=tokenize(buffer_msg);
				int status = execute(tokens, &cli_fd);
				if(buffer_msg != NULL){
					free(buffer_msg);
				}
				free_tokens(tokens);
				if (status == -1)
					break;
			} 
			
		}
	}

}

void generate_numbers(){
	int clilen=sizeof(cliaddr);//sizeof(struct sockaddr_in);
	while(1){
			int cli_fd=accept(serv_fd,(struct sockaddr *)&cliaddr,(socklen_t*)&clilen);
			if(cli_fd<0){
     			perror("221 Error on accept\n");

    		}
		//printf("\nConnection accepted.\n");	
		pthread_mutex_lock(&mutex1);
			while((put_index==BS-1 && get_index == 0) || (get_index==(put_index+1)%BS)){
				//printf("I'm Sleeping\n");
				pthread_cond_wait(&cv_full,&mutex1);
			}
			//printf("I'm Awake now\n");
			if(get_index==-1){	
				get_index=put_index=0;
			}
			else if(put_index==BS-1 && get_index!=0){
				put_index=0;
			}
			else{
				put_index=(put_index+1)%BS;
			}
			buffer[put_index]=cli_fd;	
			//printf("Produced item %d\n", buffer[put_index]);				
			pthread_cond_signal(&cv_empty);
		pthread_mutex_unlock(&mutex1);
	}
}
int main(int argc, char *argv[])
{
	if (argc < 3) {
         fprintf(stderr,"ERROR, incorrect no of arguments\n");
         exit(1);
     }
    ip = argv[1]; 
    int port = atoi(argv[2]);

  	pthread_t prod_thread;
	int pid = pthread_self();
    
    //create master thread
  	pthread_create(&prod_thread, NULL, master, (void *)&port);
	pthread_join(prod_thread,NULL);	
  	
	return(0);
}

