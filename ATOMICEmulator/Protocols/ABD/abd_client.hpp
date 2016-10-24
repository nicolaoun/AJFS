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

#ifndef ABDClient_hpp
#define ABDClient_hpp

#include "ABD_protocol.hpp"
#include <iostream>
#include <stdio.h>          /* For I/O */
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>


#endif /* SFWReader_hpp */


class ABDClient : smClient {
public:
    //SFWReader(int nodeID, int S, int W, int R, int Q, float rInt, int ops, int proto, char* qf);
    ABDClient(int nodeID, int role, std::string opath, std::string sfile="servers.list");
    void invoke_op(std::string objID, std::string path="./", std::string value="");
    //void auto_read(std::string objID, float rInt, int num_ops, std::string path="./", std::string value="");
    void set_debug_lvl(int lvl){debuglvl = lvl;}
    bool has_commited(){return commit_flag_;}
    void stop();
    void terminate();

protected:
        std::mutex mtx;
        bool commit_flag_;
    
		// Local Info
		double read_interval_;              //interval at which we execute read operations
		int total_ops_;                     // number of total operations to be invoked
    
        char objMetaFile[25];               // file that holds latest tag value
        Tag tg_;                            // tag seen by the current node
        std::string value_;                 // latest object value retrieved
        Tag maxTag;                         // maximum tag discovered during a read round
		std::string maxValue;
		int max_server_id;
    
		std::vector<RWObject> objects;
		RWObject   *obj;
    
		// File Locations
		std::string client_root_dir_;
		std::string rcvd_files_dir_;
		std::string logs_dir_;
		std::string meta_dir_;
    
		//structures to group and keep the messages rcved
		std::set<int> servers_replied_;          // ids of the servers that replied
		std::set<Server> servers_sent_;          // servers we sent a msg to
		std::set<Packet> pkts_rcved_;
    
		//int is_quorum_complete();
		void process_replies();
        Tag find_max_tag();
    
		// Coomunication procedures
		bool prepare_pkt(int, Server s, int);
		void send_to_all(int);
        void send_to_server(Server s, int m_type);
		void rcv_from_quorum();
		void close_connections();
};
