#include<netinet/in.h> //sockaddr_in
#include <stdio.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <errno.h>
#include<unistd.h>
#include<stdlib.h>
#include <arpa/inet.h>
#include<string.h>
#include <signal.h>
#include <wait.h>
#include <pthread.h>
#include <map> 
#include <iterator> 
#include <iostream>

using namespace std;


#define SLEEP_NANOSEC 100
#define SIZE 5000
#define N 10000	//number of Threads.
#define BS 1024 //Buffer Size OR SHARED MEMORY.

#define MAX_INPUT_SIZE 5000
#define MAX_TOKEN_SIZE 5000
#define MAX_NUM_TOKENS 5000
string ip;
map<int,string> mapOfWords;

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
  	int ret;	
  	int port = *((int *)data);
  	//printf("%ld: %d",pthread_self(),port);
    struct sockaddr_in servaddr;
     //1. Socket File Descriptor
     
     if((serv_fd=socket(AF_INET,SOCK_STREAM,0)) == -1){
     	printf("Server Socket Creation Failed\n");
     	exit(1);
     }
     
     memset(&servaddr, 0, sizeof(servaddr));

     servaddr.sin_family=AF_INET;
     servaddr.sin_port=htons(port);
     servaddr.sin_addr.s_addr=inet_addr(ip.c_str());
     //2. Bind
     if(bind(serv_fd,(struct sockaddr *)&servaddr,sizeof(servaddr))<0){ 
     	printf("Error in binding\n");
     	printf("Oh dear, something went wrong with bind()! %s\n", strerror(errno));
     	exit(1);
     }

     //3. Listen
     listen(serv_fd,5);

     //4. accept 
     //clilen=sizeof(cliaddr);

    
        //add more core here
		pthread_t thread[N];
		int ret_val[N];
		long parent_id = pthread_self();
		//printf("%ld\n",parent_id);
		for(int i=0;i<N;i++){
			//cout<<"created"<<endl;
			int id=i;
			if(ret_val[i]=pthread_create(&thread[i],NULL,consume_numbers,&id))
				printf("%d:,Thread Creation Failed\n!!",i);		
		}
		if(pthread_self()==parent_id)
			generate_numbers();
		for(int i=0;i<N;i++){
			//printf("done:%ld\n",pthread_self());
			pthread_join(thread[i],NULL);
		}
		//sleep(1000);
	      //optional sleep to allow workers to run
	      
}

void * consume_numbers(void * id){
	
	int thread_id = *((int *)id);
	char buffer_msg[SIZE];
    int n;
    int cli_fd;
    std::pair<std::map<int,string>::iterator,bool> ret;
    char  **tokens;
    int flag=0,flag_2=0;
    
	while (1) {
		flag=0;flag_2 = 0;
	    bzero(buffer_msg, SIZE);
	    pthread_mutex_lock(&mutex1);
	    while (get_index == -1) {
	      //printf("Buffer empty. Thread %d sleeping.\n", thread_id);
	      pthread_cond_wait(&cv_empty,&mutex1);
	    }
			//printf("\n Thread Active: %ld",pthread_self());	
			cli_fd=buffer[get_index];
			buffer[get_index]=-1;
			if(get_index==put_index){	
				get_index=put_index=-1;		
			}
			else{
				get_index=(get_index+1)%BS;
			}
			//printf("%ld : %d",pthread_self(),cli_fd);
			 
		pthread_cond_signal(&cv_full);
		pthread_mutex_unlock(&mutex1);
	      
		while(1){
			 
			 memset(&buffer_msg, 0, SIZE);
		     n = read(cli_fd,buffer_msg,SIZE);
		     if(n<0){
		     	printf("Error reading from client socket\n");
		     }
		     else{
		     	//printf("Here is the message: %s\n",buffer_msg);
		     }
		     tokens=tokenize(buffer_msg);
		     //printf("%s hihi %s",tokens[0],tokens[1]);
		     if(!strcmp(tokens[0],"create")){
		     	//printf("Yes create it is\n");
		     	string p;
		     	for(long int i=3 ; tokens[i]!=NULL;i++){
	    			//printf("%s\n",tokens[i]);
	    			p+=tokens[i];
	    			p+=' ';
	    		}
	    	//	cout<<"nil179\n";
		     	sleep(0.5);
	    		int value_size = atoi(tokens[2]);
	    		
	    		for(int i=0;i<strlen(tokens[2]);i++){
	    			if(!isdigit(tokens[2][i])){
	    				memset(&buffer_msg, 0, SIZE);
				    	strcpy(buffer_msg,"Value Size is not numeric.");
						n=write(cli_fd,buffer_msg,strlen(buffer_msg));
						flag_2=1;
						break;
	    			}
	    		}
	    		if(flag_2==0){
	    			char * str = (char *)malloc(strlen(tokens[3])*sizeof(char));
		     	
			     	ret = mapOfWords.insert(pair <int,string> (atol(tokens[1]),p));
			     	if (ret.second==false) {
	    				//cout << " with a value of " << ret.first->second << '\n';
	    				memset(&buffer_msg, 0, SIZE);
					    strcpy(buffer_msg,"Key already exists at the server.");
						n=write(cli_fd,buffer_msg,strlen(buffer_msg));
	  				}
			     	else{
				     	map<int,string> :: iterator it = mapOfWords.begin();
					    memset(&buffer_msg, 0, SIZE);
					    strcpy(buffer_msg,"Key Value created.");
						n=write(cli_fd,buffer_msg,strlen(buffer_msg));
					}/*
				    while(it != mapOfWords.end())
				    {
				        cout<<it->first<<" :: "<<it->second<<endl;
				        it++;
				    }*/
			    }
			 //   cout<<"nil213\n";
			    sleep(0.5);
	    		
		     }
		     else if(!strcmp(tokens[0],"create_check")){
		     	//printf("Yes create it is\n");
		     	string p;
		    // 	cout<<"nil219\n";
		     	sleep(0.5);
	    		
		     	for(long int i=3 ; tokens[i]!=NULL;i++){
	    			//printf("%s\n",tokens[i]);
	    			p+=tokens[i];
	    			p+=' ';
	    		}
	    		int value_size = atoi(tokens[2]);
	    		
	    		for(int i=0;i<strlen(tokens[2]);i++){
	    			if(!isdigit(tokens[2][i])){
	    				memset(&buffer_msg, 0, SIZE);
				    	strcpy(buffer_msg,"Value Size is not numeric.");
						n=write(cli_fd,buffer_msg,strlen(buffer_msg));
						flag_2=1;
						break;
	    			}
	    		}
	    		if(flag_2==0){
	    			//char * str = (char *)malloc(strlen(tokens[3])*sizeof(char));
		     		ret = mapOfWords.insert(pair <int,string> (atol(tokens[1]),p));
			     	if (ret.second==false) {
	    				//cout << " with a value of " << ret.first->second << '\n';
	    				memset(&buffer_msg, 0, SIZE);
					    strcpy(buffer_msg,"Key already exists at the server.");
						n=write(cli_fd,buffer_msg,strlen(buffer_msg));
	  				}
			     	else{
				     	//map<int,string> :: iterator it = mapOfWords.begin();
					    memset(&buffer_msg, 0, SIZE);
					    strcpy(buffer_msg,"Key Value created.");
						n=write(cli_fd,buffer_msg,strlen(buffer_msg));
					}	
			    }
			    map<int, string>::iterator it = mapOfWords.find(atol(tokens[1])); 
				if (it != mapOfWords.end()){
				    mapOfWords.erase(it);
				    memset(&buffer_msg, 0, SIZE);
				    strcpy(buffer_msg,"Key Value pair deleted successfully.");
					n=write(cli_fd,buffer_msg,strlen(buffer_msg));
				}
			//	cout<<"nil260\n";
				sleep(0.5);
	    		
		     }
		     else if(!strcmp(tokens[0],"read")){
		     	//printf("Yes read it is\n");
		     	//cout<<"nil265\n";
	    		sleep(0.5);
		     	map<int, string>::iterator it = mapOfWords.find(atol(tokens[1])); 

				if (it != mapOfWords.end()){
					memset(&buffer_msg, 0, SIZE);
					strcpy(buffer_msg,it->second.c_str());
					n=write(cli_fd,buffer_msg,strlen(buffer_msg));
				    if(n<0){
				    	printf("Error writing to client socket\n");
				    }
				    //printf("msg sent %s\n",it->second.c_str());
				}
				else{
					//printf("\nThe key doesn't exist\n");
					memset(&buffer_msg, 0, SIZE);
					strcpy(buffer_msg,"This key doesn't exist.");
					//printf("This key doesn't exist.\n");
					n=write(cli_fd,buffer_msg,strlen(buffer_msg));
					if(n<0){
						//cout<<"\n Failed sent\n";
					}
					else{
							//printf("\n successfully sent\n:%d bytes",n);
					}
				}
			//	    cout<<"nil289\n";
	    		sleep(0.5);
		     }
		     else if(!strcmp(tokens[0],"update")){
		     	//printf("Yes update it is\n");
		     	//string str = tokens[3];
		    // 	cout<<"nil295\n";
		     	string p;
		    // 	cout<<"nil219\n";
		     	sleep(0.5);
	    		
		     	for(long int i=3 ; tokens[i]!=NULL;i++){
	    			//printf("%s\n",tokens[i]);
	    			p+=tokens[i];
	    			p+=' ';
	    		}
	    		
		     	int value_size = atoi(tokens[2]);
	    		
	    		for(int i=0;i<strlen(tokens[2]);i++){
	    			if(!isdigit(tokens[2][i])){
	    				memset(&buffer_msg, 0, SIZE);
				    	strcpy(buffer_msg,"Value Size is not numeric.");
						n=write(cli_fd,buffer_msg,strlen(buffer_msg));
						if(n<0){
						//cout<<"\n Failed sent\n";
						}
						else{
							//printf("\n successfully sent\n:%d bytes",n);
						}
						flag_2=1;
						break;
	    			}
	    		}
	    		sleep(0.5);
		     	//char * str = (char *)malloc(strlen(tokens[3])*sizeof(char));
		     	map<int, string>::iterator it = mapOfWords.find(atol(tokens[1])); 
				if (it != mapOfWords.end()){
				    it->second = p;
				    memset(&buffer_msg, 0, SIZE);
				    strcpy(buffer_msg,"Key Value updated.");
					n=write(cli_fd,buffer_msg,strlen(buffer_msg));
				}
				else{
					//printf("\nThe key doesn't exist\n");
					memset(&buffer_msg, 0, SIZE);
					strcpy(buffer_msg,"No such key present in Hash Table.");
					n=write(cli_fd,buffer_msg,strlen(buffer_msg));
					if(n<0){
						//cout<<"\n Failed sent\n";
						}
						else{
							//printf("\n successfully sent\n:%d bytes",n);
						}
				}
			//	cout<<"nil311\n";
	    		sleep(0.5);
		     }
		     else if(!strcmp(tokens[0],"delete")){
		     	//printf("Yes delete it is\n");
		    // 	cout<<"nil316\n";
	    		sleep(0.5);
		     	map<int, string>::iterator it = mapOfWords.find(atol(tokens[1])); 
				if (it != mapOfWords.end()){
				    mapOfWords.erase(it);
				    memset(&buffer_msg, 0, SIZE);
				    strcpy(buffer_msg,"Key Value pair deleted successfully.");
					n=write(cli_fd,buffer_msg,strlen(buffer_msg));
						if(n<0){
						//cout<<"\n Failed sent\n";
						}
						else{
							//printf("\n successfully sent del after deleting\n:%d bytes",n);
						}
				}
				else{
					//printf("\nThe key doesn't exist\n");
					memset(&buffer_msg, 0, SIZE);
					strcpy(buffer_msg,"No such key present in Hash Table.");
					n=write(cli_fd,buffer_msg,strlen(buffer_msg));
					if(n<0){
						//cout<<"\n Failed sent\n";
						}
						else{
							//printf("\n successfully sent del\n:%d bytes",n);
						}
				}
			//	cout<<"nil331\n";
	    		sleep(0.5);
		     }
		     else if(!strcmp(tokens[0],"disconnect")){
		     	//printf("Yes delete it is\n");
		     //	cout<<"nil336\n";
	    		sleep(0.5);
		     	memset(&buffer_msg, 0, SIZE);
				strcpy(buffer_msg,"OK");
				n=write(cli_fd,buffer_msg,strlen(buffer_msg));
				close(cli_fd);
				flag=1;
			//	cout<<"nil343\n";
	    		sleep(0.5);
		     }
		     //cout<<"\n-----------------------------------------------------------"<<endl;
			//cout<<"nil347\n";
	    		
		     memset(&buffer_msg, 0, SIZE);
		     
		     //6.Write
		    for(int i=0;tokens[i]!=NULL;i++){
		    	if(tokens[i]!=NULL)
		    		free(tokens[i]);
		    }
		    if(tokens!=NULL){
		    	free(tokens);
		    }
			 if(flag==1){
			 	break;
			 }
			//cout<<"nil362\n";
	    		

		}
	}
	pthread_cond_signal(&cv_empty);
}
//Parent Item Generation Function.
void generate_numbers(){
	int clilen=sizeof(cliaddr);//sizeof(struct sockaddr_in);
	while(1){
			int cli_fd=accept(serv_fd,(struct sockaddr *)&cliaddr,(socklen_t*)&clilen);
			if(cli_fd<0){
     			printf("Error on accept\n");
     			exit(1);
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
    //create master thread
  	pthread_create(&prod_thread, NULL, master, (void *)&port);
  	pthread_join(prod_thread,NULL);	
  	
return(0);
}
