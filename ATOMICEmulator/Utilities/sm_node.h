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

#ifndef smClient_h
#define smClient_h

#include "sm_common.h"
#include "filesystem.h"
#include "set_operations.hpp"
#include <sys/time.h>
#include <time.h>
#include <string>
//streams
#include <iostream>
#include <fstream>
#include <sstream>
//data structures
#include <set>
#include <vector>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>
//network
#include <sys/types.h>          /* For sockets */
#include <sys/socket.h>         /* For sockets */
#include <sys/uio.h>            /* For sendfile */
#include <netinet/in.h>         /* For Internet sockets */
#include <arpa/inet.h>
#include <netdb.h>              /* For gethostbyname */
#include <fcntl.h>              /* For nonblocking socket */
//threads
#include <cstdlib>              // exit
#include <algorithm>
#include <thread>
#include <mutex>
//network on steroids
#include <zmq.hpp>
//#include "zhelpers.hpp"

// Process Types
enum ProcessType{
    SERVER,
    READER,
    WRITER,
};

// Statuses
enum Status{
    IDLE,
    PHASE1,
    PHASE2,
};

// Outcomes
enum ActionOutcome{
    SUCCESS = true,
    FAILURE = false
};

// Quorum views
enum QuorumViews{
    QVIEW_1,
    QVIEW_2,
    QVIEW_3,
};

// MWMR Protocols
enum MWMRProtocol{
    PROTO_ABD,
    PROTO_MWABD,
    PROTO_CWFR,
    PROTO_SSO,
    PROTO_SSO_APPRX,
};

// Message Types
enum MessageType{
    WRITE,
    READ,
    INFORM,
    WRITEACK,
    READACK,
    INFORMACK,
    TERMINATE,
    CTRL_FSIZE,
    CTRL_PKT
};

// Network Settings
const std::size_t MAX_BUFFER_SIZE = 100;

// Error Types
enum ErrorType{
    COUNTER_ERROR
};

//Minimum timeout
const std::size_t MIN_TIMEOUT = 1;

//Debugging 1-7: 1-detailed messages, 7-protocol messages only
//#define DEBUG_LEVEL_ 1
/*enum DebugLevel{
    //DEBUG_LEVEL_ = 2
    ALL_MSGS = 1,
    NETWORK_ = 2,
    PROTOCOL_INFO = 4
};
 */


//int vid_compare(const void *id1, const void *id2);  //packet comparison used by qsort
//int pkt_vid_compare(const void *id1, const void *id2);  //packet comparison used by qsort
class CTRLPacket : public Serializable{
public:
    int msgType;        // Type of the message
    int fsize;                  // Size of the file
    int counter;                // Used to determine if this is a new packet
    
    CTRLPacket(){};
    
    virtual void serialize(std::ostream& stream)
    {
        // Serialization code
        stream << msgType << " "  << fsize << " " << counter;
    }
    
    virtual void deserialize(std::istream& stream)
    {
        // Deserialization code
        stream >> msgType >> fsize >> counter;
    }
};

/*
 * Specification of a participant in the Register Implementation service
 */
class smClient {
    
public:
    smClient();
    smClient(int R, int W, int proto, char* sfile, char* qfile);
    //void terminate();
    void DEBUGING(int level, const char *format, ...);
    void REPORTERROR(const char *format, ...);
    
protected:
    int nodeID_;                //node identifier
    int debuglvl;
    
    int random_;                //choose random interval yes/no
    int seed_;
    std::string logfile;
    std::string infofile;
    std::string errorfile;
    
    int S_;                     // number of servers in the system
    int R_;                     // number of readers in the system
    int W_;                     // number of writers in the system
    int Q_;                     // number of quorums in the quorum system
    
    int *server_mem;            // @ r/w: Server list associated with the quorums they belong into
    int *q_mem_num;             // @ r/w: list of # of members of each quorum
    int *q_rcv_count;           // counts # of members of each quorum that replied to a r/w req
    
    int protocol_;              // which protocol to run SIMPLE, CWFR, SSO
    
    int role_;                  // Server, reader or writer
    int faulty_;                // whether a node may fail
    int crashed_;               // indicates whethter the node is crashed and dead
    //int randomness_;          // to indicate whether we use random interval for sends
    
    int receive_counter_;       // count # of messages received
    int mode_;                  // specifies whether node is idle or waiting
    
    //helpful counters
    int num_msgs_;
    int num_writes_;
    int num_reads_;
    int num_one_comm_;
    int num_two_comm_;
    int req_counter_;           // request counter
    
    // Timers
    double startTime,endTime,totTime,crashTime;
    double realTstart,realTend,realTtot;
    struct timeval sysTime;
    
    // Network operations
    fd_set readfds,crashfds;    // fd set descriptors
    std::vector<Server> servers_list_;
    std::set<Server> servers_connected_;
    std::set<int> servers_id_connected_;
    void parse_hosts(const char*);
    void connect_to_hosts();
    bool connect_to_server(Server *s);
    void setnonblocking(int sock);
    
    /*
     * Regular socket send
     */
    template <typename PacketClass>
    bool send_pkt(int sock, PacketClass *p)
    {
        int transmitLen=0 , bytes_sent;
        std::stringbuf sb;
        std::ostream ostm(&sb);
        char sendbuf[MAX_BUFFER_SIZE];
        
        try{
            p->serialize(ostm);
            
            DEBUGING(2, "Sending Buffer: %s\n", sb.str().c_str());
            
            bytes_sent = (int) send(sock, sb.str().c_str(), MAX_BUFFER_SIZE,0);
            
            if ( bytes_sent < 0 )
            {
                // error while sending the message
                REPORTERROR("Sending a packet on socket %d", sock);
                return false;
            }
            
            transmitLen += bytes_sent;
            
            DEBUGING(2, "Trasmitted: %d, Pkt Size:%d, Socket: %d\n",
                     transmitLen,
                     strlen(sb.str().c_str()),
                     sock);
        }
        catch(int e)
        {
            REPORTERROR("Failed to send packet. Error No: %d\n",e);
            return false;
        }
        
        return true;
        
    }
    
    /*
     * Regular socket send
     */
    template <typename PacketClass>
    bool rcv_pkt(int sock, PacketClass *p)
    {
        std::stringbuf sb;
        char rcvbuf[MAX_BUFFER_SIZE];
        
        try
        {
            DEBUGING(1, "Expecting to receive packet at socket %d...\n", sock);
            
            int rcv_bytes = recv(sock, rcvbuf, MAX_BUFFER_SIZE,0);
            
            if ( rcv_bytes < 0) { /* Get message */
                REPORTERROR("Receiving packet on socket %d", sock);
                return false;
            }
            
            //initialize the stringbuf
            sb.str(std::string(rcvbuf));
            
            // deserialize the packet received
            std::istream istm(&sb);
            p->deserialize(istm);
            
            DEBUGING(1, "Received Buffer: %s, Size: %d", sb.str().c_str(), rcv_bytes);
        }
        catch(int e)
        {
            REPORTERROR("Failed to receive packet. Error No: %d\n",e);
        }
        
        return true;
    }

    bool send_file(int sock, char* filepath);
    bool rcv_file(int sock, char* filepath);
    
    // Logging
    void init_logfile(std::string dir="");
    
};

#endif /* smClient_h */
