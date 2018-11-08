#include<stdio.h>
#include<stdlib.h>
#include<time.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h> 
#include<unistd.h>
#include<string.h>
#include <arpa/inet.h>

#include<atomic>
#include <errno.h>
#include<unistd.h>
#include <signal.h>
#include <wait.h>
#include <pthread.h>
// #include <iostream>

using namespace std;

#define N 13
#define SIZE 256
#define MAX_NUM_TOKENS 128
#define MAX_TOKEN_SIZE 128
#define COMMAND_SIZE 128
int cli_fd[N];
char msg[SIZE] = " 10 Hi\n";
char ip[20];
char port[5];
int time_up = 1;

pthread_mutex_t lock_throughput = PTHREAD_MUTEX_INITIALIZER;

/*
std::atomic <unsigned long long int> total_requests;
std::atomic <unsigned long long int> total_create_successful;
std::atomic <unsigned long long int> total_read_successful;
std::atomic <unsigned long long int> total_update_successful;
std::atomic <unsigned long long int> total_delete_successful;
std::atomic <unsigned long long int> total_connect_successful;
std::atomic <unsigned long long int> total_disconnect_successful;
*/
unsigned long long int total_requests;
unsigned long long int total_successful;


pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

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


void intermediate(char *line, void * cli_id,  unsigned long long int* total_successful){
		char  **tokens;
		int port,n;
		struct sockaddr_in serv_addr;
		char buffer[SIZE];//cout<<typeid(line).name();
		tokens=tokenize(line);
		int id = *((int *)cli_id);
	
		//cout<<*tokens[0];
	        if(!strcmp(tokens[0],"connect")){
	    	//printf("in connect\n");	
	    	//printf("%d\n", id);
	   		//printf("%d\n",cli_fd[id]);
	    	         if(cli_fd[id]>0){
	    		//printf("Error:Already Connected to one server\n");
	    		// return;
	    		}
	    		else{
		   	    cli_fd[id] = socket(AF_INET, SOCK_STREAM, 0);
	           	    if (cli_fd[id] < 0) {
			        ;//printf("ERROR opening socket\n");
			    }
			    else{
	
				    bzero((char *) &serv_addr, sizeof(serv_addr));
				    serv_addr.sin_family = AF_INET;

				    port = atoi(tokens[2]);//cout<<port;
				    serv_addr.sin_port = htons(port);
				    serv_addr.sin_addr.s_addr = inet_addr(tokens[1]);
				  
				    if (connect(cli_fd[id],(struct sockaddr *)&serv_addr,sizeof(serv_addr)) < 0) {
				    	cli_fd[id] = -1;
						//printf("ERROR connecting\n");
				    }
				    else{
				    (*total_successful)++;
					// total_connect_successful++;
				    	;//printf("OK\n");
					//char buffer_msg[SIZE];
					//memset(&buffer_msg, 0, SIZE);
				    //strcpy(buffer_msg,"create_check 95 7 nikhil");
					//n = write(cli_fd[id],buffer_msg,strlen(buffer_msg));
					//if (n < 0) 
			 		//printf("Error writing to socket");	
				    }
			    }	
		       	
			    }
	   	 }
	   	else if(!strcmp(tokens[0],"disconnect")){
	   		//printf("disconnect...\n");
	   		//printf("%d\n", id);
	   		//printf("%d\n",cli_fd[id]);
	   		if(cli_fd[id]<=0){
	   			//printf("No active connection\n");
	   			//return;
	   		}
		   	else{
		   		n = write(cli_fd[id],line,strlen(line));
	    			if (n < 0){ 
	         			;//printf("Error writing to socket");
				}
				else{
				 	memset(&buffer, 0, SIZE);
				 	n = read(cli_fd[id],buffer,SIZE);
			    		if (n < 0){ 
					    	;//printf("Error reading from socket");
					}
			    		else{
			    			// total_disconnect_successful++;
			    			(*total_successful)++;
						;//printf("%s\n",buffer);
					}
				   	close(cli_fd[id]);
				   	cli_fd[id] = -1;
				   	//printf("\n You can connect to any server now.\n");
				   	// return;
				   	//disconnect();
				}
			}
	   	}
	   	else if(!strcmp(tokens[0],"create")){
	   		//create(tokens);
	   		//printf("%d\n", id);
	   		//printf("%d\n",cli_fd[id]);
	   		if(cli_fd[id]<=0){
	   			//printf("No active connection\n");
	   			// return;
	   		}
		   	else{
		   		n = write(cli_fd[id],line,strlen(line));
	    			if (n < 0){ 
	         			;//printf("Error writing to socket");
				}
				else{
			 		memset(&buffer, 0, SIZE);
			 		n = read(cli_fd[id],buffer,SIZE);
		    			if (n < 0){ 
			    			;//printf("Error reading from socket");
					}
		    			else{
						// total_create_successful++;
						(*total_successful)++;
		    				;//printf("%s\n",buffer);
					}
				}
	    		}	
	   	}
	   	else if(!strcmp(tokens[0],"read")){
	   		//read(tokens);
	   		//printf("%d\n", id);
	   		//printf("%d\n",cli_fd[id]);
	   		if(cli_fd[id]<=0){
	   			//printf("No active connection\n");
	   			// return;
	   		}
	   		else{
		   		n = write(cli_fd[id],line,strlen(line));
	    			if (n < 0){ 
	         			;//printf("Error writing to socket");
				}
				else{
			 		memset(&buffer, 0, SIZE);
			 		n = read(cli_fd[id],buffer,SIZE);
		    			if (n < 0){ 
			    			;//printf("Error reading from socket");
					}
		    			else{
						// total_read_successful++;
						(*total_successful)++;		    				
						// ;printf("Read:%s\n",buffer);
					}
				}
	    		}	
	   	}
	   	else if(!strcmp(tokens[0],"update")){
	   		//update(tokens);
	   		//printf("in update\n");
	   		//printf("%d\n", id);
	   		//printf("%d\n",cli_fd[id]);
	   		if(cli_fd[id]<=0){
	   			//printf("No active connection\n");
	   			// return;
	   		}
	   		else{
			   	n = write(cli_fd[id],line,strlen(line));
		    		if (n < 0){ 
			 		;//printf("Error writing to socket");
				}
				else{
				 	memset(&buffer, 0, SIZE);
				 	n = read(cli_fd[id],buffer,SIZE);
			    		if (n < 0){ 
				    		;//printf("Error reading from socket");
					}
			    		else{
						// total_update_successful++;
						(*total_successful)++;
			    			// ;printf("Update%s\n",buffer);
					}
				}
    			}
	   	}
	   	else if(!strcmp(tokens[0],"delete")){
	   		//delete(tokens);
	   		////printf("in del\n");
	   		//printf("%d\n", id);
	   		//printf("%d\n",cli_fd[id]);
	   		if(cli_fd[id]<=0){
	   			//printf("No active connection\n");
	   			// return;
	   		}
	   		else{
		   		n = write(cli_fd[id],line,strlen(line));
		    		if (n < 0){ 
			 		;//printf("Error writing to socket");
				}
				else{
				 	memset(&buffer, 0, SIZE);
				 	n = read(cli_fd[id],buffer,SIZE);
			    		if (n < 0){ 
				    		;//printf("Error reading from socket");
					}
			    		else{	
						// total_delete_successful++;
						(*total_successful)++;
			    			// ;printf("Delete%s\n",buffer);
					}
				}
    			}
	   	}
	   	
	   	// Freeing the allocated memory	
       for(int i=0;tokens[i]!=NULL;i++){
	 		free(tokens[i]);
       }
       if(tokens!=NULL){
          free(tokens);
       }
}


void form_command(int cmd_no,int key, void * cli_id, unsigned long long int * total_successful){
	// printf("%ld\n", pthread_self());
	char command[COMMAND_SIZE];
	char str_key[4];
	sprintf(str_key,"%d",key);
	int id = *((int *)cli_id);
	//printf("form id%d\n", id);
	//printf("form cli fd%d\n",cli_fd[id]);
	switch(cmd_no){
		case 0://printf("connect: %d\n",id);
				//strcpy(command,"connect 10.42.0.47 4000\n");
				strcpy(command,"connect ");
				strcat(command,ip);
				strcat(command," ");
				strcat(command,port);
				strcat(command,"\n");
				intermediate(command,cli_id,total_successful);
				break;
		case 1://printf("create: %d\n",id);
				strcpy(command,"create ");
				strcat(command,str_key);
				strcat(command,msg);
				intermediate(command,cli_id,total_successful);
		break;
		case 2://printf("read: %d\n",id);
				strcpy(command,"read ");
				strcat(command,str_key);
				strcat(command,"\n");
				intermediate(command,cli_id,total_successful);
		break;
		case 3://printf("update: %d\n",id);
				strcpy(command,"update ");
				strcat(command,str_key);
				strcat(command,msg);
				intermediate(command,cli_id,total_successful);
		break;
		case 4://printf("delete: %d\n",id);
				strcpy(command,"delete ");
				strcat(command,str_key);
				strcat(command,"\n");
				intermediate(command,cli_id,total_successful);
		break;
		case 5://printf("disconnect: %d\n",id);
				strcpy(command,"disconnect ");
				strcat(command,str_key);
				strcat(command,"\n");
				intermediate(command,cli_id,total_successful);
		break;
		default:
				// printf("Hi im in default\n");
		break;
	}
}

void * generate_command(void * cli_id)
{	
	printf("Entered generate function: %ld\n",pthread_self());
	unsigned long long int total_requests = 0;
	unsigned long long int total_successful = 0;

	int id = *((int *)cli_id);
	srand(time(NULL));
	int i=0, key, cmd_no;
	int flag = 0;
	while(1){
		pthread_mutex_lock(&lock);
			if(time_up==0){
				printf("I got exit message %ld\n",pthread_self());
				printf("%ld : TR: %u\n", pthread_self(),total_requests);
				printf("%ld : TS: %u\n", pthread_self(),total_successful);
				::total_requests += total_requests;
				::total_successful += total_successful;
				form_command(5,1,cli_fd, &total_successful);
				flag = 1;
			}
		pthread_mutex_unlock(&lock);
		if(flag){
			break;
		}
		
		//printf("%ld : %d\n",pthread_self(), rand()%10);
		//printf("------------------------ i = %d ----------------------------------\n", i);
		i++;
		cmd_no = rand()%6; 
		key = rand()%10000;
		// printf("%d\n",cli_fd[id]);
		if(cmd_no==5 && key<9500){
			cmd_no = 2;
		}
		form_command(cmd_no, key, cli_id, &total_successful);
		total_requests++;
	}
}

int main(int c,char ** argv)
{	
	printf("-----------------------------------------------------\n");
	printf("Threads client : %d\n",N);
			
	pthread_t thread[N];
	int ret_val[N];
	long parent_id = pthread_self();
	int id[N];
	int time = atoi(argv[3]);
	printf("Time : %d\n",time);
	strcpy(ip,argv[1]);
	strcpy(port,argv[2]);
	signal(SIGPIPE,SIG_IGN);
	signal(SIGABRT,SIG_IGN);
	signal(SIGSEGV,SIG_IGN);	
	
	for(int i=0;i<N;i++){
		id[i] = i;
		cli_fd[i] = -1;
		if(ret_val[i]=pthread_create(&thread[i],NULL,generate_command,&id[i]))
			printf("%d:,Thread Creation Failed\n!!",i);		
	}
	if(parent_id == pthread_self()){
		printf("Thread Creation Done!!\n");
		sleep(time);
		pthread_mutex_lock(&lock);
		time_up = 0;
		pthread_mutex_unlock(&lock);
		printf("Time up!!\n");
	}
	// printf("parent: %ld\n",parent_id);
	// printf("exiting: %ld\n",pthread_self());
	for(int i=0;i<N;i++){
		pthread_join(thread[i],NULL);
	}
	printf("Total requests generated: %u\n",(unsigned)total_requests);
	// printf("Total successfull completed requests:\n");
	// printf("Create: %u\n",(unsigned)total_create_successful);
	// printf("Read: %u\n",(unsigned)total_read_successful);
	// printf("Update: %u\n",(unsigned)total_update_successful);
	// printf("Delete: %u\n",(unsigned)total_delete_successful);
	// printf("Connect: %u\n",(unsigned)total_connect_successful);
	// printf("Disconnect generated: %u\n",(unsigned)total_disconnect_successful);
	// unsigned long long int sum = (unsigned)total_create_successful + (unsigned)total_read_successful + (unsigned)total_update_successful + (unsigned)total_delete_successful + (unsigned)total_connect_successful;
	printf("Total succesful: %u\n",(unsigned)total_successful);
	printf("Throughput: %u requests/second \n",total_successful/time);
	printf("-----------------------------------------------------\n");
}
//Throughput = Number of successful requests / Total Run Time
//Response Time  = 
