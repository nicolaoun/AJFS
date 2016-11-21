/*
The MIT License (MIT)

Copyright (c) 2016 Dongjae (David) Kim

		Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
		to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
		copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

		The above copyright notice and this permission notice shall be included in all
		copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
		AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

#include "sm_node.h"
#include <fstream>

//int hdr_mwmr::offset_;

void smNode::DEBUGING(int level, const char *format, ...)
{
    va_list ap;
    char rolestr[3][20]={"SERVER","READER","WRITER"};
    FILE *fp;
    char formattedstr[255];
    char outstr[280];
    
    if(level < debuglvl)
        return;
   
    // handle long messages
    if ( strlen(format) < 255 )
    {
        va_start(ap, format);
        vsprintf(formattedstr, format, ap);
        va_end(ap);
        
        sprintf(outstr, "[INFO]%10s %d: %s\n",
                rolestr[role_],
                nodeID,
                formattedstr);
        
        printf("%s",outstr);
        
        fp=fopen(infofile.c_str(), "a");
        fprintf(fp, "%s", outstr);
        fclose(fp);
    }
    else
    {
        REPORTERROR("Debug msg too long.");
    }
}

void smNode::REPORTERROR(const char *format, ...)
{
    va_list ap;
    char errorstr[255];
    char rolestr[3][20]={"SERVER","READER","WRITER"};
    char errorout[280];
    FILE *fp;
    
    // handle long messages
    if ( strlen(format) < 255 )
    {
        va_start(ap, format);
        vsprintf(errorstr, format, ap);
        va_end(ap);
        
        sprintf(errorout, "[ERROR]%10s %d: %s\n",
                rolestr[role_],
                nodeID,
                errorstr);
        
        fp=fopen(errorfile.c_str(), "a");
        fprintf(fp, "%s", errorout);
        fclose(fp);
    }
    else
    {
        sprintf(errorout, "[ERROR]%10s %d: Error msg too long.",
                rolestr[role_],
                nodeID);
    }
    
    perror((const char*)errorout);
}

smNode::smNode()
{
    
    receive_counter_ = 0;
    mode_ = IDLE;
    
    faulty_ = 0;
    crashed_ = 0;
    
    //initialize counters
    num_writes_=0;
    num_reads_=0;
    num_one_comm_=0;
    num_two_comm_=0;
    num_msgs_=0;
    totTime=0;
}

void smNode::init_logfile(std::string file){
    FILE *fp;
    
    logfile = file;
    
    infofile = logfile + ".info.log";
    errorfile = logfile + ".error.log";
    
    //sprintf(infofile,"%s.info.log", logfile.c_str());
    //sprintf(errorfile,"%s.error.log", logfile.c_str());
    
    DEBUGING(7, "\t%s - DEBUG_LEVEL %d\n************************", get_datetime_str().c_str(), debuglvl);
}

/**********************************************************/
/*                Network Operations                      */
/**********************************************************/

bool smNode::send_file(int sock, char* fpath)
{
    int transmitted_bytes=0 , bytes_sent=0, bytes_read;
    char buffer[MAX_BUFFER_SIZE];
    struct CTRLPacket fs_pkt;
    off_t offset = 0, sbytes;
    int fp;
    
    // get the file size
    fs_pkt.msgType = CTRL_FSIZE;
    fs_pkt.fsize = getFileSize(fpath);
    fs_pkt.counter = req_counter_;
    
    DEBUGING(2, "Size of file %s is %d bytes", fpath, fs_pkt.fsize);
    
    // Send file size
    if ( !send_pkt<CTRLPacket>(sock, &fs_pkt) )
    {
        // error while sending the filesize
        REPORTERROR("Error sending the size of file %s.", fpath);
        return false;
    }
    
    // if the file does not exists - return
    if (fs_pkt.fsize == -1) {
        DEBUGING(2, "No file to be transmitted.");
        return true;
    }
    else //otherwise send the file
    {
        // open the file at the end
        std::ifstream infile( fpath, std::ios::binary );

        int perc=0;
        DEBUGING(4, "Transmitting file...%d\%",perc);  
        
        while( infile.tellg() < fs_pkt.fsize )
        {
            // specify how many bytes to read from the file
            bytes_read = ( fs_pkt.fsize - infile.tellg() >= MAX_BUFFER_SIZE )? MAX_BUFFER_SIZE : fs_pkt.fsize - (int) infile.tellg();
            
            bzero(buffer, MAX_BUFFER_SIZE);                     // reset the buffer
            
            infile.read(buffer, bytes_read);                    // read MAX_BUFFER_SIZE bytes from the file
            
            DEBUGING(1, " ** Read bytes: %d", bytes_read);
            
            try
            {
                bytes_sent = (int) send(sock, buffer, bytes_read, 0);  // send the data
            }
            catch(int err)
            {
                REPORTERROR("Sending Interupted!!Err No: %d", err);
            }
            
            DEBUGING(1, " ** Transmitted file bytes: %d", bytes_sent);
            
            if ( bytes_sent < bytes_read )
            {
                // error while sending the data
                REPORTERROR("Error sending file block. READ:%d bytes, SENT:%d bytes", bytes_read, bytes_sent);
                infile.close();
                exit(EXIT_FAILURE);
            }
            
            transmitted_bytes += bytes_sent;
            
            infile.seekg(transmitted_bytes);                                   // move to the next character to be sent
            
            // print progress
            if (perc < 10) {
                std::cout << "\b\b";
            }
            else
            {
                std::cout << "\b\b\b";
            }
            
            perc = (transmitted_bytes*100)/fs_pkt.fsize;
            std::cout << perc << "%";
        }
        std::cout<<"\n";
        
        //Close input file
        infile.close();
        //close(fp);
        
        if(transmitted_bytes == fs_pkt.fsize)
        {
            DEBUGING(2, "File transmitted correclty.");
        }
        else
        {
            REPORTERROR("Transmitted bytes do not match file size: TX = %d bytes, FS = %d bytes",
                        transmitted_bytes,
                        fs_pkt.fsize);
            return false;
        }
    }
   
    return true;
}

/**
 * Function: rcv_file
 * Params: socket to listen, file path to save rcved contents
 **/
bool smNode::rcv_file(int sock, char* fpath)
{
    int rcv_bytes, transmitted_bytes = 0;
    char buffer[MAX_BUFFER_SIZE];
    struct CTRLPacket fs_pkt;
    
    // rcv the file size
    DEBUGING(1, "Expecting to receive FILESIZE at socket %d...", sock);
    //rcv_bytes = (int) recv(sock, &fs_pkt, sizeof(fs_pkt),0);
    
    if ( !rcv_pkt<CTRLPacket>(sock, &fs_pkt) || fs_pkt.msgType != CTRL_FSIZE) {
        // Error getting size
        REPORTERROR("Receiving file size at socket %d.", sock);
        return false;
    }
    else
    {
        // sender does not have the file
        if ( fs_pkt.fsize == -1 ) {
            DEBUGING(2, "Sender reported file does not exist (Size: %d).", fs_pkt.fsize);
            return true;
        }
        else
        {
            DEBUGING(2, "Waiting to receive a file of size: %d bytes.", fs_pkt.fsize);
            
            // open the output file : flags - truncate previous content, output, append, binary
            std::ofstream outfile(fpath,  std::ios::out | std::ios::binary);

            int perc=0;
            DEBUGING(4, "Receiving file...%d\%",perc);
            while (transmitted_bytes < fs_pkt.fsize) {
                bzero(buffer, MAX_BUFFER_SIZE);   //reset the buffer
                
                //outfile.seekp(transmitted_bytes);                       // move to the last inserted byte
                
                rcv_bytes = (int) recv(sock, buffer, MAX_BUFFER_SIZE, 0);
                
                if ( rcv_bytes < 0) {
                    // Error receiving file
                    REPORTERROR("\n Receiving file %s at socket %d.", fpath, sock);
                    outfile.close();
                    return false;
                }
                
                outfile.write(buffer, rcv_bytes);                       // write the buffer in the file
                
                transmitted_bytes += rcv_bytes;                         // update the latest byte written
            
                // print progress
                if (perc < 10) {
                    std::cout << "\b\b";
                }
                else
                {
                    std::cout << "\b\b\b";
                }
                
                perc = (transmitted_bytes*100)/fs_pkt.fsize;
                std::cout << perc << "%";
            }
            std::cout<<"\n";
            
            // Close the output file
            outfile.close();
            
            if (transmitted_bytes == fs_pkt.fsize) {
                DEBUGING(2, "File %s received successfully.", fpath);
            }
            else{
                REPORTERROR("\nTransmitted bytes do not match file size: TX = %d bytes, FS = %d bytes.",
                            transmitted_bytes,
                            fs_pkt.fsize
                            );
                return false;
            }
        }
    }
    
    return true;
}

void smNode::parse_hosts(const char *file){
    FILE *fp;
  //!!  Server *srv = new Server();
    smNode srv;

    
    DEBUGING(2, "Reading file: %s",
             file);
    
    if(!(fp=fopen(file,"r"))){
        REPORTERROR("Reading Server File");
        exit(1);
    }
    else{
        while (fscanf(fp, "%d %s %d", &srv.nodeID, srv.hostname, &srv.port)!=EOF) {
            
            servers_list_.push_back(srv);
            
            DEBUGING(2, "SID:%d, Host:%s, Port:%d",
                     srv.nodeID,
                     srv.hostname,
                     srv.port);
        }
        
        S_ = (int) servers_list_.size();
        
        fclose(fp);
        
    }
}

void smNode::connect_to_hosts(){
//	int serverlen;
//	struct sockaddr_in _server;
//	struct sockaddr *serverptr;
//	struct hostent *rem;
    std::vector<smNode>::iterator srv_it;
    
	// Clear the servers set
	servers_connected_.clear();
    
	// Initialize the file descriptors
	FD_ZERO(&readfds);
    
	for (srv_it = servers_list_.begin(); srv_it != servers_list_.end(); srv_it++) {
        if( connect_to_server(&(*srv_it)))
        {
            servers_connected_.insert(*srv_it);
            servers_id_connected_.insert((*srv_it).nodeID);
            
			//add the socket in the fd set
			FD_SET((*srv_it).sock, &readfds);
        }
        
	} // end EOF while

    DEBUGING(2, "Connected to %d servers... ", servers_connected_.size());
}

/**
 *  Connect to server and return the socket
 **/
bool smNode::connect_to_server(smNode *s)
{
    int serverlen;
    struct sockaddr_in _server;
    struct sockaddr *serverptr;
    struct hostent *rem;

    if ((s->sock  = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    { /* Create socket */
        REPORTERROR("Socket");
        return false;
    }
    
    rem = gethostbyname(s->hostname);
    
    if (rem == NULL)
    { /* Find Server's address */
        REPORTERROR("Gethostbyname (could not resolve Server's %s IP)", s->hostname);
        return false;
    }
    
    _server.sin_family = AF_INET; /* Internet domain */
    bcopy((char *) rem -> h_addr, (char *) &_server.sin_addr, rem -> h_length);
    _server.sin_port = htons(s->port); /* Server's Internet address and port */
    serverptr = (struct sockaddr *) &_server;
    serverlen = sizeof _server;
    
    if (connect(s->sock, serverptr, serverlen) < 0)
    { /* Request connection */
        REPORTERROR("Could not connect to Server %s (%s)", s->hostname, rem->h_addr_list[0]);
        return false;
    }
    else{
        DEBUGING(6, "Connected to Server %d on host %s, port %d, sock %d",
                 s->nodeID,
                 s->hostname,
                 s->port,
                 s->sock);
    }
    
    return true;
}

/*
bool smNode::connect_to_node(smNode *n)
{
    n->z_sock = &socket;

    // Configure socket to not wait at close time
    int linger = 0;
    n->z_sock->setsockopt (ZMQ_LINGER, &linger, sizeof (linger));

    s_set_id(*n->z_sock);

    std::cout << "Connecting to "<< n->hostname << "(ip:"<< n->ip_addr <<") on port "<< n->port <<"…" << std::endl;
    std::ostringstream oss;
    oss << "tcp://" << n->ip_addr << ":" << n->port;
    n->z_sock->connect (oss.str());

    return true;
}
*/


void smNode::setnonblocking(int sock)
{
	int opts;
    
	opts = fcntl(sock,F_GETFL);
	if (opts < 0) {
		REPORTERROR("fcntl(F_GETFL)");
		exit(EXIT_FAILURE);
	}
	opts = (opts | O_NONBLOCK);
	if (fcntl(sock,F_SETFL,opts) < 0) {
		REPORTERROR("fcntl(F_SETFL)");
		exit(EXIT_FAILURE);
	}
	return;
}
