#include <iostream>			// input output
#include <string>			// for bzero
#include <sys/socket.h> 	// creating socket
#include <netinet/in.h>		// for sockaddr_in
#include <unistd.h>			// for close
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/sendfile.h>
#include <sys/uio.h>
#include <stack>
#include <fstream>
#include <sstream>
#include "INIReader.h"

using namespace std;

bool enabled;
int PORT;
string doc_root;
string mime_types;

int validate_file(string full_path,string url){
	if (access(full_path.c_str(), F_OK) == -1){
		cout<<"not found!"<<endl;
		return -1;
	}else{
		// Check if the file path doesn't escape the document root.
		stack<int> s;
		string parsed;
		stringstream path_stream(url);
		
		while(getline(path_stream,parsed,'/')){
			if(parsed=="")
				continue;
			if(parsed==".."){
				if(s.empty())
					return -1;
				s.pop();
			}
			else{
				s.push(1);
			}	
		}

                // Check if the file has right permissions
		if (access(full_path.c_str(), R_OK) == -1){
			cout<<"access denied!"<<endl;
	                return -1;
		}

		return 1;
	}
}

int get_file_size(const char* filepath)
{
    struct stat finfo;

    if (stat(filepath, &finfo) != 0) {
        //die_system("stat() failed");
    }

    return (int) finfo.st_size;
}

string mime_type(string filepath){
	//stringstream ss(filepath);
	string parsed;
	int i;
	
	//while(getline(ss,parsed,'/'));
			
	for(i=filepath.length()-1;i>0;i--)
		if(filepath[i]=='.')
			break;

	string parsed_type=filepath.substr(i,filepath.size()-i);	

	ifstream in(mime_types,ios::in);
	
	bool flag=false;
	while(getline(in,parsed)){	
		stringstream sss(parsed);		
		getline(sss,parsed,' ');
		if(parsed==parsed_type){
			flag=true;
			getline(sss,parsed);
			break;
		}	
	}	

	in.close(); 	

	if(!flag)
		parsed="application/octet-stream";	

	return parsed;
}

string build_200_ok_headers(string filepath){
	string response;
	response += "HTTP/1.1 200 OK\r\n";
	response += "Content-Length: "+ to_string(get_file_size(filepath.c_str())) +"\r\n";
	//determine mime type 
	string type=mime_type(filepath);
	response += "Content-type: "+type+"\r\n";
	response += "\r\n";

	return response;

}

void handle_request(char* buf, int client_sock){

	//Copy the buffer to parse
	char* buf_copy = (char*)malloc(strlen(buf) + 1); 
	strcpy(buf_copy, buf);

	// Get the filename
	char* first_line = strsep(&buf_copy, "\r\n");
	strsep(&first_line," ");
	char* url = strsep(&first_line," ");

	//doc_root ~
	if(doc_root[0]=='~'){
		char* home=getenv("HOME");
		doc_root.erase(0,1);
		doc_root.insert(0,home);
	}
	
	string surl(url);
	//url=='/'
	if(url[0]=='/'&&surl.size()==1){
		url="/index.html";
	}

	// Prepend document root to get the absolute path
	string full_path;	
	//if(url[0]!='/')
	//	full_path = doc_root+'/'+url;
	//else
	full_path = doc_root+url;
	cout << full_path << endl;

	// Validate the file path requested.
	int is_valid = validate_file(full_path,url);

	string headers;
	if(is_valid==1){
		headers = build_200_ok_headers(full_path);
	}else{
		//headers = build_404_notfound_headers(buf);
	}

	headers="hello";
	//Send headers
	send(client_sock, (void*) headers.c_str(), (ssize_t) headers.size(), 0);

	//Send body
	if(is_valid==1){
		string info;
		ifstream in(full_path,ios::in);
		
		while(in.rdbuf()->in_avail()!= 0){
			in>>info;
			const char* finfo=info.c_str();
			int leng=info.size();

			int h=send(client_sock,finfo,leng,0);
			if(h!=leng)
				cout<<"sendfile error!"<<endl;
		}
		in.close();
/*
		struct stat finfo;
		int fd = open(full_path.c_str(), O_RDONLY);
		fstat(fd, &finfo);
		off_t off = 0;
		int h = sendfile(client_sock, fd, &off, finfo.st_size);
		cout << "sendfile status " << h << endl;*/
	}

}


int main(int argc, char** argv) {

	// Handle the command-line argument
	if (argc != 2) {
		cerr << "Usage: " << argv[0] << " [config_file]" << endl;
		//return EX_USAGE;
		return 0;
	}

	//load config file
	INIReader reader(argv[1]);

	if (reader.ParseError()<0) {
        	cout<<"Can't load config file\n";
        	return 1;
    	}

	enabled=reader.GetBoolean("httpd", "enabled", true);
	PORT=reader.GetInteger("httpd", "port", 8080);
	doc_root=reader.Get("httpd", "doc_root", "UNKNOWN");
	mime_types=reader.Get("httpd", "mime_types", "UNKNOWN");
	
	//mime_types ~
	if(mime_types[0]=='~'){
		char* home=getenv("HOME");
		mime_types.erase(0,1);
		mime_types.insert(0,home);
	}

    	cout<<"Config loaded from config file: enabled="
            <<enabled << ", port="
            <<PORT << ", doc_root="
            <<doc_root << ", mime_types="
            <<mime_types << "\n";

	// 1. socket()
	int sock = socket(AF_INET, SOCK_STREAM, 0);

	// if socket was created successfully, 
	// socket() returns a non-negative number
	if(sock < 0) {
		cout << "ERROR WHILE CREATING SOCKET" << endl;
		return 0;
	}

	// create a sockaddr_in struct
	struct sockaddr_in server_address;
	server_address.sin_family = AF_INET;
	server_address.sin_port = htons(PORT);
	server_address.sin_addr.s_addr = htonl(INADDR_ANY);

	// 2. bind()
	int b = bind(sock, (struct sockaddr*)&server_address,
				 sizeof(server_address));

	// if bind is successful it returns a 0, else 1
	if(b < 0) {
		cout << "ERROR WHILE BINDING SOCKET" << endl;
		close(sock);
		return 0;
	}

	cout << "SERVER IS RUNNING" << endl;

	// 3. listen
	listen(sock, 1);

	struct sockaddr_in client_address;
	socklen_t client_length = sizeof(client_address);

	char buffer[256];
	bzero(buffer, 256);
	// 4. accept and receive
	while(1){
		int new_sock = accept(sock, (struct sockaddr*)&client_address,
						  &client_length);


	// if connection was created successfully, 
	// accept() returns a non-negative number
		if(new_sock < 0) {
			cout << "ERROR WHILE ACCEPTING CONNECTION" << endl;
			close(sock);
			continue;
		}
		int n = read(new_sock, buffer, 256);

		if(n < 0) {
			cout << "ERROR WHILE GETTING MESSAGE" << endl;
		} /*else {
			cout << "Message received: " << buffer << endl;
			cout << "Message length: " << n << endl;
		}*/

	// 6. Handle file request
	// Parse request

		handle_request(buffer, new_sock);

		// 7. close
		close(new_sock);
		sleep(1);
	}
	close(sock);
}
