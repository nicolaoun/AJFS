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

#ifndef ABDServer_hpp
#define ABDServer_hpp

#include "abd_protocol.hpp"

#define MAX_CONNECTIONS 5

// details of a single client
class Client {
public:
    int nodeID;             // client id
    int  req_counter;       // used to distinguish stale messages from this client
    int  sock;              // communication socket
    char hostname[100];     // client hostname
    char ip_addr[30];       // client ip address

    // overloaded == operator
    bool operator == (const Client& c1) const
    {
        //Check if c1 == c2
        return (this->nodeID == c1.nodeID && this->sock == c1.sock);
    }
};


class ABDServer : smClient{
public:
    ABDServer(int serverID, int port, int S=0, int W=0, int R=0, int Q=0, int nfq=0, double cInt=5);
    void start();
    void serveClient(Client *c);
    void set_debug_lvl(int lvl){debuglvl = lvl;}
    void terminate();
    
protected:
    std::mutex mtx;
    
    std::vector<std::thread> threads;
    
    int newsock_;                           // new socket connection
    int sock_;                              // local socket
    int port_;                              // local port
    
    Tag tg_;                                // local tag
    std::string value_;                     // local value
    
    std::set<RWObject *> objects_set;         //set of all replicas at the server
    std::vector<Client> serve_clients_;     //set of clients served by the server
    
    RWObject getLocalReplica(RWObject obj);    //return a pointer to the replica
    RWObject insertLocalReplica(RWObject obj);  //return a pointer to the replica
    
    int non_faulty_qid_;                    // id of the non-faulty quorum
    
    // File Locations
    std::string srv_root_dir_;
    std::string rcvd_files_dir_;
    std::string logs_dir_;
    std::string meta_dir_;
    
    //Timers
    double crash_freq_;                     //how often a Server checks for failure
    
    //Network operations
    int serverlen, clientlen;
    struct sockaddr_in server, client;
    struct sockaddr *serverptr, *clientptr;
    struct hostent *rem;
    fd_set childFDs;
    
    void createSocket();
    void initializeServerAddress();
    void bindSocket();
    void listenSocket();
    
    void acceptClientConnection();
    void serve(Packet*, Client *c, RWObject r);
    
    void prepare_pkt(Client *dest, int msgType, RWObject);

};

#endif
