#include<stdio.h>
#include<stdlib.h>
#include<time.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h> 
#include<unistd.h>
#include<string.h>
#include <arpa/inet.h>
// #include <iostream>

using namespace std;

#define N 1000
#define SIZE 256
#define MAX_NUM_TOKENS 256
#define MAX_TOKEN_SIZE 256
int cli_fd[N];
char msg[SIZE] = " 10 Hi This is the message\n";


char **tokenize(char *line){
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


void intermediate(char *line, void * cli_id){
		char  **tokens;
		int port,n;
		struct sockaddr_in serv_addr;
		char buffer[SIZE];//cout<<typeid(line).name();
		tokens=tokenize(line);
		int id = *((int *)cli_id);
	
		//cout<<*tokens[0];
	    if(!strcmp(tokens[0],"connect")){
	    	//printf("in connect\n");	
	    	printf("%d\n", id);
	   		printf("%d\n",cli_fd[id]);
	    	if(cli_fd[id]>0){
	    		printf("Error:Already Connected to one server\n");
	    		return;
	    	}
	    	else{
		   		cli_fd[id] = socket(AF_INET, SOCK_STREAM, 0);
			    if (cli_fd[id] < 0) {
			        printf("ERROR opening socket\n");
			    }

	
			    bzero((char *) &serv_addr, sizeof(serv_addr));
			    serv_addr.sin_family = AF_INET;

			    port = atoi(tokens[2]);//cout<<port;
			    serv_addr.sin_port = htons(port);
			    serv_addr.sin_addr.s_addr = inet_addr(tokens[1]);
			  
			    if (connect(cli_fd[id],(struct sockaddr *)&serv_addr,sizeof(serv_addr)) < 0) {
			    	cli_fd[id] = -1;
			        printf("ERROR connecting\n");
			    }
			    
			    else
			    	printf("OK\n");
			}
			char buffer_msg[SIZE];
			memset(&buffer_msg, 0, SIZE);
		    strcpy(buffer_msg,"create_check 95 7 nikhil");
			//n = write(cli_fd[id],buffer_msg,strlen(buffer_msg));
    		//if (n < 0) 
         		//printf("Error writing to socket");
	    }
	   	else if(!strcmp(tokens[0],"disconnect")){
	   		//printf("disconnect...\n");
	   		printf("%d\n", id);
	   		printf("%d\n",cli_fd[id]);
	   		if(cli_fd[id]<=0){
	   			printf("No active connection\n");
	   			return;
	   		}
	   		n = write(cli_fd[id],line,strlen(line));
    		if (n < 0) 
         		printf("Error writing to socket");
         	memset(&buffer, 0, SIZE);
         	n = read(cli_fd[id],buffer,SIZE);
    		if (n < 0) 
            	printf("Error reading from socket");
    		else
    			printf("%s\n",buffer);
	   		close(cli_fd[id]);
	   		cli_fd[id] = -1;
	   		//printf("\n You can connect to any server now.\n");
	   		return;
	   		//disconnect();
	   	}
	   	else if(!strcmp(tokens[0],"create")){
	   		//create(tokens);
	   		printf("%d\n", id);
	   		printf("%d\n",cli_fd[id]);
	   		if(cli_fd[id]<=0){
	   			printf("No active connection\n");
	   			return;
	   		}
	   		n = write(cli_fd[id],line,strlen(line));
    		if (n < 0) 
         		printf("Error writing to socket");
         	memset(&buffer, 0, SIZE);
         	n = read(cli_fd[id],buffer,SIZE);
    		if (n < 0) 
            	printf("Error reading from socket");
    		else
    			printf("%s\n",buffer);
	   	}
	   	else if(!strcmp(tokens[0],"read")){
	   		//read(tokens);
	   		printf("%d\n", id);
	   		printf("%d\n",cli_fd[id]);
	   		if(cli_fd[id]<=0){
	   			printf("No active connection\n");
	   			return;
	   		}
	   		n = write(cli_fd[id],line,strlen(line));
    		if (n < 0) 
         		printf("Error writing to socket");
         	memset(&buffer, 0, SIZE);
         	n = read(cli_fd[id],buffer,SIZE);
    		if (n < 0) 
            	printf("Error reading from socket");
    		else
    			printf("%s\n",buffer);
	   	}
	   	else if(!strcmp(tokens[0],"update")){
	   		//update(tokens);
	   		//printf("in update\n");
	   		printf("%d\n", id);
	   		printf("%d\n",cli_fd[id]);
	   		if(cli_fd[id]<=0){
	   			printf("No active connection\n");
	   			return;
	   		}
	   		n = write(cli_fd[id],line,strlen(line));
    		if (n < 0) 
         		printf("Error writing to socket");
         	memset(&buffer, 0, SIZE);
         	n = read(cli_fd[id],buffer,SIZE);
    		if (n < 0) 
            	printf("Error reading from socket");
    		else
    			printf("%s\n",buffer);
	   	}
	   	else if(!strcmp(tokens[0],"delete")){
	   		//delete(tokens);
	   		//printf("in del\n");
	   		printf("%d\n", id);
	   		printf("%d\n",cli_fd[id]);
	   		if(cli_fd[id]<=0){
	   			printf("No active connection\n");
	   			return;
	   		}
	   		n = write(cli_fd[id],line,strlen(line));
    		if (n < 0) 
         		printf("Error writing to socket");
         	memset(&buffer, 0, SIZE);
         	n = read(cli_fd[id],buffer,SIZE);
    		if (n < 0) 
            	printf("Error reading from socket");
    		else
    			printf("%s\n",buffer);
	   	}
	   	
	   	// Freeing the allocated memory	
       for(int i=0;tokens[i]!=NULL;i++){
	 		free(tokens[i]);
       }
       if(tokens!=NULL){
          free(tokens);
       }
}


void form_command(int cmd_no,int key, void * cli_id){
	// printf("%ld\n", pthread_self());
	char command[1000];
	char str_key[4];
	sprintf(str_key,"%d",key);
	int id = *((int *)cli_id);
	printf("form id%d\n", id);
	printf("form cli fd%d\n",cli_fd[id]);
	switch(cmd_no){
		case 0:printf("connect: %d\n",id);
				strcpy(command,"connect 127.0.0.1 4000\n");
				intermediate(command,cli_id);
				break;
		case 1:printf("create: %d\n",id);
				strcpy(command,"create ");
				strcat(command,str_key);
				strcat(command,msg);
				intermediate(command,cli_id);
		break;
		case 2:printf("read: %d\n",id);
				strcpy(command,"read ");
				strcat(command,str_key);
				strcat(command,"\n");
				intermediate(command,cli_id);
		break;
		case 3:printf("update: %d\n",id);
				strcpy(command,"update ");
				strcat(command,str_key);
				strcat(command,msg);
				intermediate(command,cli_id);
		break;
		case 4:printf("delete: %d\n",id);
				strcpy(command,"delete ");
				strcat(command,str_key);
				strcat(command,"\n");
				intermediate(command,cli_id);
		break;
		case 5:printf("disconnect: %d\n",id);
				strcpy(command,"disconnect");
				strcat(command,str_key);
				// intermediate(command,cli_id);
		break;
	}
}

void * generate_command(void * cli_id)
{	
	int id = *((int *)cli_id);
	srand(time(NULL));
	int i=0, key, cmd_no; 
	while(1){
		//printf("%ld : %d\n",pthread_self(), rand()%10);
		printf("------------------------ i = %d ----------------------------------\n", i);
		i++;
		cmd_no = rand()%6; 
		key = rand()%10000;
		// printf("%d\n",cli_fd[id]);
		form_command(cmd_no, key, cli_id);
	}
}

int main()
{
	pthread_t thread[N];
	int ret_val[N];
	long parent_id = pthread_self();
	int id[N];
	for(int i=0;i<N;i++){
		id[i] = i;
		cli_fd[i] = -1;
		if(ret_val[i]=pthread_create(&thread[i],NULL,generate_command,&id[i]))
			printf("%d:,Thread Creation Failed\n!!",i);		
	}
	for(int i=0;i<N;i++){
		pthread_join(thread[i],NULL);
	}
}
