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

//  ABDMWMR_client

#include "abd_client.hpp"

/************************************************
 *              INITIALIZATION
 ************************************************/

// int nodeID, int S, int R, int W, int Q, float rInt, int ops, int proto, char* qfile
ABDClient::ABDClient(int id, int role, std::string opath, std::string sfile) {
    
    nodeID = id;
    role_ = role;
    value_ = "";
    total_ops_ = 1;

    // specify the node's root dir
    if ( opath == "")
    {
        std::stringstream sstm;
        sstm << "./client_" << nodeID;
        client_root_dir_ = sstm.str();
    }
    else
    {
        client_root_dir_ = opath;
    }

    //setup local directories
    setup_dirs(client_root_dir_);
    
    //read the servers
    parse_hosts(sfile.c_str());
    
    DEBUGING(4,"Initialized, Server file: %s",
             sfile.c_str());
}

void ABDClient::setup_dirs(std::string root_dir)
{

    if(!directoryExists(root_dir)) {
        createDirectory(root_dir);
    }

    rcvd_files_dir_ = root_dir + "/rcvd_files" ;
    if(!directoryExists(rcvd_files_dir_)) {
        createDirectory(rcvd_files_dir_);
    }

    logs_dir_ = root_dir + "/logs";
    if(!directoryExists(logs_dir_)) {
        createDirectory(logs_dir_);
    }

    meta_dir_ = root_dir + "/.meta";
    if(!directoryExists(meta_dir_)) {
        createDirectory(meta_dir_);
    }

    char log_fname[10];
    sprintf(log_fname, "/c%d",nodeID);
    logs_dir_ += log_fname;
    init_logfile(logs_dir_);
    DEBUGING(6, "Loaded Directories...");
}

/************************************************
 *         COMMUNICATION METHODS
 ************************************************/

Packet ABDClient::prepare_pkt(int counter, smNode dest, int msgType)
{
    Packet p;
    Tag tg;
        
    //Specify the destination of the packet
    p.src_=nodeID;
    p.dst_=dest.nodeID;
    
    //Specify the fields of the packet accordingly
    p.msgType = msgType;
    p.counter = counter;
    p.obj = *obj;

    return p;
}

void ABDClient::send_to_all(int m_type){
    //int s;
    req_counter_++;
    std::set<smNode>::iterator it;
    std::vector<std::thread> srv_threads;
    struct timeval sysTime;
    double startSend, curTime, timeDiff=0, timeOut=60;
    
    servers_sent_.clear();
    
    //mark the time of the read invocation
    gettimeofday(&sysTime, NULL);
    startSend=sysTime.tv_sec + (sysTime.tv_usec/1000000.0);
    
    for (it=servers_connected_.begin(); it!=servers_connected_.end(); it++)  {
        //srv_threads.push_back(std::thread(&ABDClient::send_to_server, this, *it,m_type));
        send_to_server(*it,m_type);
    } // end of for
    
    // wait until enough servers receive our msg or a timeout occurs
    /*while(timeDiff < timeOut)
    {
        //mark the time of the read invocation
        gettimeofday(&sysTime, NULL);
        curTime=sysTime.tv_sec + (sysTime.tv_usec/1000000.0);
        timeDiff = curTime - startTime;
        if((int)timeDiff%10 == 0)
            std::cout << timeDiff <<",";
    }
     */
     
    //for (auto& th : srv_threads) th.join();
    
    // check if we have not reached the majority => exit
    if((int) servers_sent_.size() <= S_/2)
    {
        REPORTERROR("Too few servers received msg while sending to all.");
        close_connections();
        exit(1);
    }
}

void ABDClient::send_to_server(smNode s, int m_type)
{
    char fpath[100];
    Packet p;

    p = prepare_pkt(req_counter_, s, m_type);

    Tag tg = p.obj.get_tag();
    DEBUGING(2,"Sending packet to PID:%d, Type:%d, Object:%s, Tag:<%d,%d,%d>, Counter:%d",
             p.dst_,
             p.msgType,
             p.obj.get_id().c_str(),
             tg.ts,tg.wid,tg.wc,
             p.counter);

    // try to send meta and file to the server -> if succeed add server in the sent set
    if( send_pkt<Packet>(s.sock, &p) )
    {
        if( m_type == WRITE && p.obj.get_type() == FILE_T)
        {
            //mtx.lock();
            // send the object value
            //sprintf(fpath, "%s/%s_%d_%d",client_root_dir_.c_str(), obj->get_id().c_str(), obj->tg_.ts,obj->tg_.wid);
            sprintf(fpath, "%s/%s",client_root_dir_.c_str(), obj->get_id().c_str());
            if( send_file(s.sock, fpath))
            {
                servers_sent_.insert(s);
            }
            //mtx.unlock();
        }
        else
        {
            //mtx.lock();
            servers_sent_.insert(s);
            //mtx.unlock();
        }
    }
}

void ABDClient::rcv_from_quorum(){
    Packet p;
    struct timeval sel_timeout;
    int ready;
    std::set<smNode>::iterator it;
    std::set<smNode> servers_pending_;
    char fpath[100];
    int total_sent = servers_sent_.size();
    
    //initialize the servers replied and pkts received
    servers_replied_.clear();
    pkts_rcved_.clear();
    
    do {
        sel_timeout.tv_sec=1;
        sel_timeout.tv_usec=0;
        
        // Re-std::set the Fds
        for (it=servers_sent_.begin(); it!=servers_sent_.end(); it++) {
            FD_SET((*it).sock, &readfds);
        }
        
        // wait until something happens
        ready=select(FD_SETSIZE, &readfds, NULL, NULL, &sel_timeout);
        
        DEBUGING(2, "There are %d fds ready to be read....",ready);
        
        if (ready < 0) {
            REPORTERROR("Select");
            exit(EXIT_FAILURE);
        }
        
        if (ready > 0) {
            
            servers_pending_=servers_sent_;
            
            for (it=servers_sent_.begin(); it!=servers_sent_.end(); it++) {
                DEBUGING(2, "Trying sock:%d....",
                         (*it).sock);
                
                if (FD_ISSET((*it).sock, &readfds)) {
                    DEBUGING(1,"Receiving packet at address:0x%x",&p);
                    
                    // receive the packet
                    if ( rcv_pkt<Packet>((*it).sock, &p) )
                    {
                        
                        DEBUGING(3,"Received packet from S:%d, Type:%d, Tag:<%d,%d,%d>, Object: <%s, %s, %s>, Counter:%d",
                                 p.src_,
                                 p.msgType,
                                 p.obj.get_tag().ts,p.obj.get_tag().wid,p.obj.get_tag().wc,
                                 p.obj.get_id().c_str(),
                                 p.obj.get_path().c_str(),
                                 p.obj.get_value().c_str(),
                                 p.counter);

                        if ( (p.msgType == READACK || p.msgType == WRITEACK)  && p.counter==req_counter_ )
                        {
                            // receive the file during the query phase (PHASE 1)
                            if ( mode_ == PHASE1 && p.obj.get_type() == FILE_T)
                            {
                                sprintf(fpath, "%s/sid%d.[%d,%d].%s.temp",
                                        rcvd_files_dir_.c_str(),
                                        p.src_,
                                        p.obj.get_tag().ts, p.obj.get_tag().wid,
                                        p.obj.get_id().c_str()
                                        );

                                if ( rcv_file((*it).sock, fpath) )
                                {
                                    pkts_rcved_.insert(p);
                                    servers_replied_.insert((*it).nodeID);
                                }
                                else
                                {
                                    REPORTERROR("Failed receiving file form SID: %d on socket SID: %d",
                                                (*it).nodeID,
                                                (*it).sock);
                                }

                                //servers_alive_.erase(*it);
                                //servers_sent_.erase(*it);
                            }
                            else
                            {
                                pkts_rcved_.insert(p);
                                servers_replied_.insert((*it).nodeID);
                                //servers_sent_.erase(*it);
                            }
                        }
                        else{ //unknown msgType or the counter did not match
                            switch (p.msgType) {
                                case READACK:
                                case WRITEACK:
                                case COUNTER_ERROR:
                                    DEBUGING(4, "Counter-Error with Server %d (%s) on socket %d: LC=%d, SC=%d ...",
                                             (*it).nodeID,
                                             (*it).hostname,
                                             (*it).sock,
                                             req_counter_,
                                             p.counter);
                                    break;
                                    
                                default:
                                    DEBUGING(4, "Unknown-Error with Server %d (%s) on socket %d: Packet details: type %d, counter %d, tag <%d,%d,%d>...",
                                             (*it).nodeID,
                                             (*it).hostname,
                                             (*it).sock,
                                             p.msgType, p.counter,
                                             p.obj.get_tag().ts,p.obj.get_tag().wid,p.obj.get_tag().wc);
                                    
                                    break;
                            }
                            
                            //servers_alive_.erase(*it);
                            //servers_sent_.erase(*it);
                            //FD_CLR(socks_[*it], &readfds);
                        }
                    }
                    else
                    {
                        REPORTERROR("Failed receiving packet form SID: %d on socket SID: %d",
                                    (*it).nodeID,
                                    (*it).sock);
                        //servers_alive_.erase(*it);
                        //servers_sent_.erase(*it);;
                    }
                    
                    servers_pending_.erase(*it);
                }
            }
        }
        servers_sent_ = servers_pending_;
        
    }while( servers_pending_.size() > 0 );//servers_replied_.size() <= total_sent-servers_alive_.size());
    
    // if only a minority replied -> exit
    if (servers_replied_.size() <= S_ / 2) {
        DEBUGING(4, "Exiting due to many Server errors...");
        exit(1);
    }
    
    // if there are still alive that did not reply -> interrupt them
    /*if( servers_alive_.size() > 0 )
     {
     // Re-std::set the Fds
     for (it=servers_alive_.begin(); it!=servers_alive_.end(); it++) {
     FD_SET((*it).sock, &readfds);
     }
     }
     */
    
    /*
     DEBUGING(4, "Replied Majority: %s",
     SetOperation<int>::print_std::set(servers_replied_).c_str()
     );
     */
}


/************************************************
 *         PROTOCOL SPECIFIC METHODS
 ************************************************/

void ABDClient::invoke_op(std::string objID, object_t objType, std::string value){
    struct timeval sysTime;
    std::string rounds="ONE";

    // connect to hosts
    connect_to_hosts();

    //Initialize counter
    req_counter_=0;

    // if not being able to connect to a majority -> through exception
    if ( (int) servers_connected_.size() <= S_/2 ) {
        REPORTERROR("Connection could not be established with the majority of the servers in the given list.");
        return;
    }

    // initialize object details
    RWObject temp_obj(objID, objType, meta_dir_);
    temp_obj.set_path(client_root_dir_);
    temp_obj.set_value(value);
    temp_obj.set_type(objType);

    if(temp_obj.get_type() == FILE_T)
        DEBUGING(1, "Temp Object type: FILE");

    if(temp_obj.get_type() == VALUE_T)
        DEBUGING(1, "Temp Object type: VALUE");

    std::vector<RWObject>::iterator oit = std::find(objects.begin(), objects.end(), temp_obj);

    if ( oit != objects.end() ) {
        DEBUGING(3,"Object %d already in the std::set.");//, oit->get_id());
    }
    else
    {
        DEBUGING(3,"Inserting object %d in the std::set.");//, oit->get_id());
        oit = objects.insert(oit, temp_obj);
    }

    //std::set the object to work with
    obj = &(*oit);

    //mark the time of the read invocation
    gettimeofday(&sysTime, NULL);
    startTime=sysTime.tv_sec + (sysTime.tv_usec/1000000.0);

    mode_ = PHASE1;

    if ( role_ == READER ) {
        num_reads_++;
        DEBUGING(6, "***************************************");
        DEBUGING(6,"Invoking read %d on object %s at %s ",
                 num_reads_,
                 obj->get_id().c_str(),
                 get_datetime_str().c_str()
                 );
        DEBUGING(6, "***************************************");
    }
    else
    {
        num_writes_++;
        DEBUGING(6, "***************************************");
        DEBUGING(6,"Invoking write %d on object %s at %s ",
                 num_writes_,
                 obj->get_id().c_str(),
                 get_datetime_str().c_str());
        DEBUGING(6, "***************************************");
    }

    num_msgs_ = 0;

    // PHASE1
    send_to_all(READ);      // send READ to all the servers
    rcv_from_quorum();      // receive from majority
    process_replies();      // discover the maximum tag among the replies
    // END PHASE1

    //PHASE2
    DEBUGING(2, "Performing phase2...");
    rounds="TWO";

    num_two_comm_++;

    send_to_all(WRITE); // send the latest tag to all the servers
    rcv_from_quorum();  // wait for a majority to reply
    // END PHASE2

    // Get the real time after the computation
    gettimeofday(&sysTime, NULL);
    endTime = sysTime.tv_sec+(sysTime.tv_usec/1000000.0);

    totTime+=endTime-startTime;

    DEBUGING(6,"\n\n**************************************************");
    DEBUGING(6, "Read#:%d, Duration:%f, Rounds:%s, Tag:<%d,%d,%d>, Object: %s!!",
             num_reads_,
             endTime-startTime,
             rounds.c_str(),
             obj->tg_.ts,obj->tg_.wid,obj->tg_.wc,
             obj->get_id().c_str());
    DEBUGING(6,"******************************************************");

    //disconnect from the hosts
    DEBUGING(6, "Closing connections...");
    close_connections();

}

void ABDClient::process_replies()
{
   std::stringstream sstm;
    
    maxTag = find_max_tag();
    
    //set the target journal file
    sstm.str("");
    sstm.clear();
    sstm << client_root_dir_.c_str() << "/" << obj->get_id().c_str();// << "_" << maxTag.ts << "_" << maxTag.wid;
    
    std::string dest_file = sstm.str();     // local file
   
    
    DEBUGING(3,"ABD: WRITE Comparing - Local Tag:<%d,%d> vs Max Tag <%d,%d>",
             obj->tg_.ts,
             obj->tg_.wid,
             maxTag.ts,
             maxTag.wid
             );
    
    // ABD with updates
    // Check if the max tag is equal to our local tag -> we know the latest version thus write the new version
    if (role_ == WRITER && maxTag == obj->tg_) {
        // increment the max timestamp
        maxTag.ts = maxTag.ts + 1;
        maxTag.wid  = nodeID;
        
        obj->set_latest_tag(maxTag);
        commit_flag_ = true;
        
        DEBUGING(6, "\n\n***************************************************");
        DEBUGING(6,"ABD: WRITE SUCCEEDED -> Writing New Tag:<%d,%d,%d> ",
                 maxTag.ts,
                 maxTag.wid,
                 maxTag.wc
                 );
        DEBUGING(6, "***************************************************");
        
    }
    else
    {   // either a newer write discovered or a read is invoked
        commit_flag_ = false;
        
        // if the tag is higher than the local tag or file does not exist -> keep the file
        if ( maxTag > obj->tg_ || !fileExists(dest_file) )
        {
            obj->set_latest_tag(maxTag);
            
            // copy the appropriate file to the path
            sstm.str("");
            sstm.clear();
            sstm << rcvd_files_dir_.c_str() << "/sid" << max_server_id << ".[" << obj->get_tag().ts << "," << obj->get_tag().wid << "]." << obj->get_id().c_str() << ".temp";
            
            std::string src_file = sstm.str();  // received file
            
            // copy the file to the local directory
            if (directoryExists(src_file)){
                copyFile(src_file, dest_file);
                DEBUGING(2, "Copied file '%s' successfully", dest_file.c_str());
            }
            else
            {
                REPORTERROR("Directory does not exist: %s",src_file.c_str());
            }
        }
        
       DEBUGING(6, "\n\n***************************************");
        DEBUGING(6,"ABD: Propagating Tag:<%d,%d,%d> ",
                 maxTag.ts,
                 maxTag.wid,
                 maxTag.wc
                 //src.rdbuf()
                 //maxValue.c_str()
                 );
       DEBUGING(6, "***************************************");
    }
    
    mode_ = PHASE2;
}

/************************************************
 *         HELPER METHODS
 ************************************************/

Tag ABDClient::find_max_tag()
{
    std::set<Packet>::iterator pit;
    
    Tag max_tag = obj->tg_;
    
    //
    // Find the maximum timestamp among the received msgs
    // and collect the pkts coming from the witnessed quorum
    //
    for(pit=pkts_rcved_.begin(); pit!=pkts_rcved_.end(); pit++){
        
        DEBUGING(3,"Checking Msg Sid:%d Tag:<%d,%d,%d>",
                 pit->src_,
                 pit->obj.tg_.ts,
                 pit->obj.tg_.wid,
                 pit->obj.tg_.wc);
        
        if(pit->obj.tg_ > max_tag){
            max_tag = pit->obj.tg_;
            max_server_id = pit->src_;
        }
    }
    
    return max_tag;
}

void ABDClient::stop(){
    close_connections();
    //terminate();
    switch(role_){
        case WRITER:
            //send_timer_.force_cancel();
            DEBUGING(7,"(writes: %d, ONECOMM: %d, TWOCOMM: %d, AVETIME: %.4lf) -- TERMINATED",
                     num_writes_,
                     num_one_comm_,
                     num_two_comm_,
                     totTime/num_writes_);
            break;
        case READER:
            //send_timer_.force_cancel();
            DEBUGING(7,"(reads: %d, ONECOMM: %d, TWOCOMM: %d, AVETIME: %.4lf) -- TERMINATED",
                     num_reads_,
                     num_one_comm_,
                     num_two_comm_,
                     totTime/num_reads_);
            break;
    }

}

void ABDClient::close_connections(){
    
    //Packet p;
    std::set<smNode>::iterator  it;
    
    req_counter_++;
    
    for (it=servers_connected_.begin(); it!=servers_connected_.end(); it++) {
        prepare_pkt(req_counter_, *it, TERMINATE);
        
        close((*it).sock); /* Close socket */
    }
    
}

void ABDClient::terminate(){
    //Print the outcome
    switch(role_){
        case WRITER:
            //send_timer_.force_cancel();
            DEBUGING(7,"(writes: %d, ONECOMM: %d, TWOCOMM: %d, AVETIME: %.4lf) -- TERMINATED",
                     num_writes_,
                     num_one_comm_,
                     num_two_comm_,
                     totTime/num_writes_);
            break;
        case READER:
            //send_timer_.force_cancel();
            DEBUGING(7,"(reads: %d, ONECOMM: %d, TWOCOMM: %d, AVETIME: %.4lf) -- TERMINATED",
                     num_reads_,
                     num_one_comm_,
                     num_two_comm_,
                     totTime/num_reads_);
            break;
        /*
        case SERVER:
            if(crashed_)
                DEBUGING(7,"crashed at %f -- TERMINATED", crashTime);
            else
                DEBUGING(7,"terminates -- TERMINATED");
            break;
            */
    }
}



