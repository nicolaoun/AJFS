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

#ifndef CCHybridClient_hpp
#define CCHybridClient_hpp

#include "cchybrid_protocol.hpp"

class CCHybridClient : public smNode {
public:
    CCHybridClient(int nodeID, int role, std::string opath, std::string sfile="servers.list");
    void invoke_op(std::string objID, object_t objType, std::string value="");

    void set_debug_lvl(int lvl){debuglvl = lvl;}
    void set_failures(int f) {failures_ = f;}

    //bool has_commited(){return commit_flag_;}
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
        std::string maxValue;
		int max_server_id;
        int failures_;                      // number of maximum server failures
    
        std::unordered_map<std::string, RWObject> objects;
		RWObject   *obj;

        // Protocol variables
        Tag max_tag_;                         // maximum tag discovered during a read round
        int max_views_;
        std::unordered_map<int, int> max_servers_;
        std::vector<int> prop_servers_;

		// File Locations
		std::string client_root_dir_;
		std::string rcvd_files_dir_;
		std::string logs_dir_;
		std::string meta_dir_;
        void setup_dirs(std::string root);
    
		//structures to group and keep the messages rcved
        std::unordered_map<int, CCHybridPacket> servers_replies_;       // servers asociated with packets they sent
        std::set<smNode> servers_sent_;                         // servers we sent a msg to


        //Protocol specific procedures
        //int is_quorum_complete();
        void invoke_read();
        void invoke_write(std::string v);
		void process_replies();
        Tag find_max_params();
        bool is_predicate_valid();
    
        // Communication procedures
        Packet prepare_pkt(int, smNode s, int);
		void send_to_all(int);
        bool send_to_server(smNode s, int m_type);
        void rcv_from_quorum(int min);
		void close_connections();
};


#endif /* CCHybridClient_hpp */
