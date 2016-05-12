#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <curl/curl.h>

char* file_name;
int fileNumber = 0;
int fd = -1;
int sLength;

int ip = 1;
int max_no_of_lines = 500;
int no_of_jobs = 2;
size_t bs = BUFSIZ;
char ovrwt = 0;

const char* const help = "Usage:\n"
				"burst [-l LINES] [OPTIONS] ... [FILE]\n"
				"burst [-u] [OPTIONS] ... [URL]\n"
				"Copy and split a file's lines to multiple files.\n\n"
				"Options:\n"
				"-j JOBS\t\tSetting amount of jobs OR threads to run .default ==> 2.\n"
				"-l LINES\tSets the maximum number of lines for each file (default is 500).\n"
				"-x\t\tOverwrite existing files.\n"
				"-u Download file from a url instead.\n"
				"-h\t\tShow this help and exit.\n";

typedef struct{
	int no_of_lines_to_read;
	char** linePosition;
}thread_result;

typedef struct{
	char* data;
	ssize_t size;
}string;

void nextSegmentFile(){
//if the file operation is successful then close the file descriptor
	if(fd != -1) close(fd);
	char* s = malloc(sLength);
	if(s == NULL){
		perror("Error in the malloc function");
		exit(EXIT_FAILURE);
	}
	snprintf(s, sLength, "%s%d", file_name, ++fileNumber);
	fd = open(s, O_WRONLY| O_CREAT | (ovrwt? 0 : O_EXCL), 0755);
	if(fd == -1){
		perror(s);
		exit(EXIT_FAILURE);
	}
	free(s);
}

char *basename(char const *path)
{
	char *s = strrchr(path, '/');
	if(s==NULL) {
		return strdup(path);
	} else {
		return strdup(s + 1);
	}
}

int downloadFromURL(const char *URL, const char * outputFileName)
{  
	CURL *cl;
	FILE *filePointer;
	CURLcode result;
	cl = curl_easy_init();                                                                                                                                                       
	if (cl)
	{
		filePointer = fopen(outputFileName,"wb");
		curl_easy_setopt(cl, CURLOPT_URL, URL);
		curl_easy_setopt(cl, CURLOPT_WRITEFUNCTION, NULL);
		curl_easy_setopt(cl, CURLOPT_WRITEDATA, fp);
		result = curl_easy_perform(cl);
		curl_easy_cleanup(cl);
		fclose(fp);
		return 1;
	}
	return 0;	
}

int no_of_written_lines = 0;
void writingLinesMethod(char* data, ssize_t ds, int new_lines, char** linePosition){
	if(no_of_written_lines == -1){
		nextSegmentFile();
		no_of_written_lines = 0;	
	}
	
	no_of_written_lines += new_lines;
	int overflow = no_of_written_lines - max_no_of_lines;
	if(overflow > 0){
		//split up the max_no_of_lines
		char* final_line = linePosition[new_lines - overflow - 1];
		write(fd, data, final_line-data+1);
		//make a new file
		nextSegmentFile();
		
		//write in a completely new file
		no_of_written_lines = 0;
		writingLinesMethod(final_line+1, data+ds-final_line-1, overflow, linePosition+new_lines-overflow);
	}else if(overflow == 0){
		//write the leftover to the next file
		char* final_line = linePosition[new_lines-1];
		write(fd, data, final_line-data+1);
		
		//this may be the last character, We are just about to make the next file
		if(final_line == data+ds-1){
			no_of_written_lines = -1;
		}else{
			nextSegmentFile();
			write(fd, final_line+1, data+ds-final_line+1);
		}

	}else{
		//write all the data
		write(fd, data, ds);
	}
}

void* thread_process(string* in){
	thread_result* res = malloc(sizeof(thread_result));
	if(res == NULL){
		perror("malloc");
		exit(EXIT_FAILURE);
	}
	res->no_of_lines_to_read = 0;
	res->linePosition = malloc(sizeof(char*)*2);
	if(res->linePosition == NULL){
		perror("malloc");
		exit(EXIT_FAILURE);
	}
	size_t lines_pos_capacity = 2;
	
	char* s = in->data;
	while((s = memchr(s, '\n', (in->data-s) + in->size))){
		res->no_of_lines_to_read++;
		if(res->no_of_lines_to_read > lines_pos_capacity){
			lines_pos_capacity *= 2;
			res->linePosition = realloc(res->linePosition, sizeof(char*)*lines_pos_capacity);
		}
		res->linePosition[res->no_of_lines_to_read-1] = s;
		if(++s == in->data + in->size) break;
	}
	
	return res;
}

int main(int argc, char* argv[]){
	int c;
	while((c = getopt(argc, argv, "-l:j:b:u:xh")) != -1){
		switch(c){
			case 1:
				ip = open(optarg, O_RDONLY);
				if(ip == -1){
					perror(optarg);
					exit(EXIT_FAILURE);
				}
				size_t len = strlen(optarg);
				file_name = malloc(len+1);
				if(file_name == NULL){
					perror("malloc");
					exit(EXIT_FAILURE);
				}
				strcpy(file_name, optarg);
				break;
			case 'l':
				max_no_of_lines = atoi(optarg);
				if(max_no_of_lines < 1) max_no_of_lines = 500;
				break;
			case 'j':
				no_of_jobs = atoi(optarg);
				if(no_of_jobs < 1) no_of_jobs = 2;
				break;
			case 'u':
			   	strcpy(URL,optarg);
				break;
			case 'b':
				bs = atoi(optarg);
				if(bs < 1) bs = BUFSIZ;
				break;
			case 'x':
				ovrwt = 1;
				break;
			case 'h':
				write(0, help, strlen(help));
				exit(EXIT_SUCCESS);
			case '?':
				write(2, "Try 'burst -h' for more information\n", 36);
				exit(EXIT_FAILURE);
		}
	}
	
	if(ip == 1) file_name = "a.out";
	else if (strlen(URL) > 0) {
		file_name = basename(URL);
		downloadFromURL(URL, file_name);
	}
	sLength = strlen(file_name)+9;
	
	nextSegmentFile();
	
	char* buffer = malloc(bs*no_of_jobs);
	if(buffer == NULL){
		perror("malloc");
		exit(EXIT_FAILURE);
	}
	pthread_t* threads = malloc(sizeof(pthread_t)*no_of_jobs);
	if(threads == NULL){
		perror("malloc");
		exit(EXIT_FAILURE);
	}
	ssize_t size;
	while((size = read(ip, buffer, bs*no_of_jobs))){
		if(size == -1) perror(file_name);
		if(size > max_no_of_lines && size > no_of_jobs){
			const ssize_t sj = size/no_of_jobs;
			string* strlist = malloc(sizeof(string)*no_of_jobs);
			if(strlist == NULL){
				perror("malloc");
				exit(EXIT_FAILURE);
			}
			char* split = buffer;
			for(int j = 0; j < no_of_jobs; j++){
				const ssize_t sz = sj + (j == no_of_jobs-1? size%no_of_jobs : 0);
				strlist[j].data = split;
				strlist[j].size = sz;
				int err = pthread_create(threads+j, NULL, &thread_process, strlist+j);
				if(err != 0){
					perror("POSIX thread");
					exit(EXIT_FAILURE);
				}
				split += sz;
			}
			
			for(int j = 0; j < no_of_jobs; j++){
				thread_result* res;
				int err = pthread_join(threads[j], &res);
				if(err != 0){
					perror("POSIX thread");
					exit(EXIT_FAILURE);
				}
				writingLinesMethod(strlist[j].data, strlist[j].size, res->no_of_lines_to_read, res->linePosition);
				free(res->linePosition);
				free(res);
			}
			free(strlist);
		}else{
			//only run 1 thread
			string str = {buffer, size};
			thread_result* res = thread_process(&str);
			writingLinesMethod(buffer, size, res->no_of_lines_to_read, res->linePosition);
			free(res->linePosition);
			free(res);
		}
	}
	
	free(buffer);
	free(threads);
	if(ip != 1) free(file_name);
	close(ip);
	exit(EXIT_SUCCESS);
}
