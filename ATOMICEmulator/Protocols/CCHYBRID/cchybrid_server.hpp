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

#ifndef CCHybridServer_hpp
#define CCHybridServer_hpp

#include "cchybrid_protocol.hpp"

#define MAX_CONNECTIONS 5


class CCHybridServer : public smNode{
public:
    CCHybridServer(int serverID, int port);
    void start();
    void serve_client(smNode *c);
    void set_debug_lvl(int lvl){debuglvl = lvl;}
    void terminate();
    
protected:
    std::mutex mtx;
    
    std::vector<std::thread> threads;
    
    int newsock_;                           // new socket connection
    int sock_;                              // local socket
    int port_;                              // local port
    
    std::unordered_map<std::string, CCHybridObject> local_objects_;         //set of all replicas at the server
    std::vector<smNode> serve_clients_;     //set of clients served by the server
    
    CCHybridObject *get_local_replica(CCHybridObject obj);    //return a pointer to the replica

    int non_faulty_qid_;                    // id of the non-faulty quorum
    
    // File Locations
    std::string srv_root_dir_;
    std::string rcvd_files_dir_;
    std::string logs_dir_;
    std::string meta_dir_;
    void setup_dirs(std::string opath);
    
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
    void serve(CCHybridPacket*, smNode *c, CCHybridObject *r);
    
    CCHybridPacket prepare_pkt(smNode *dest, int msgType, CCHybridObject);

};

#endif
