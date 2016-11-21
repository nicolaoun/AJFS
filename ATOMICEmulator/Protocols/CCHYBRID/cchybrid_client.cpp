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

//  cchybridMWMR_client

#include "cchybrid_client.hpp"

/************************************************
 *              INITIALIZATION
 ************************************************/

CCHybridClient::CCHybridClient(int id, int role, std::string opath, std::string sfile) {
    
    nodeID = id;
    role_ = role;
    total_ops_ = 1;
    
    //setup local directories
    // specify the node's paths
    if ( opath == "" )
    {
        std::stringstream sstm;
        sstm << "./client_" << nodeID;
        client_root_dir_ = sstm.str();
    }
    else
    {
        client_root_dir_ = opath;
    }

    setup_dirs(client_root_dir_);
    
    //read the servers
    parse_hosts(sfile.c_str());

    failures_ = S_/2;       //set failures by default to minority
    
    DEBUGING(4,"Initialized, Server file: %s, failures: %d",
             sfile.c_str(),
             failures_);
    
}

void CCHybridClient::setup_dirs(std::string root_dir)
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

CCHybridPacket CCHybridClient::prepare_pkt(int counter, smNode dest, int msgType)
{
    CCHybridPacket p;
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

void CCHybridClient::send_to_all(int m_type){
    //int s;
    std::set<smNode>::iterator it;
    std::vector<std::thread> srv_threads;
    struct timeval sysTime;
    double startSend, curTime, timeDiff=0, timeOut=60;
    
    // increment the send counter and empty the receive set
    req_counter_ ++;
    servers_sent_.clear();
      
    for (it=servers_connected_.begin(); it!=servers_connected_.end(); it++)  {
        //srv_threads.push_back(std::thread(&CCHybridClient::send_to_server, this, *it,m_type));
        if( send_to_server(*it,m_type) )
        {
            servers_sent_.insert(*it);
        }
        else
        {
            REPORTERROR("Failed to communicate with server %d", (*it).nodeID);
        }
    } // end of for
     
    //for (auto& th : srv_threads) th.join();
    
    // check if we have not reached the majority => exit
    if((int) servers_sent_.size() <= S_/2)
    {
        REPORTERROR("Too few servers received msg while sending to all.");
        close_connections();
        exit(1);
    }
}

bool CCHybridClient::send_to_server(smNode s, int m_type)
{
    char fpath[100];
    CCHybridPacket p;

    p = prepare_pkt(req_counter_, s, m_type);

    Tag tg = p.obj.get_tag();
    DEBUGING(2,"Sending packet to PID:%d, Type:%d, Object:%s, Tag:<%d,%d,%d>, Counter:%d",
             p.dst_,
             p.msgType,
             p.obj.get_id().c_str(),
             tg.ts,tg.wid,tg.wc,
             p.counter);

    // try to send meta and file to the server -> if succeed add server in the sent set

    if( send_pkt<CCHybridPacket>(s.sock, &p) )
    {
            //mtx.lock();
            return true;
            //mtx.unlock();
    }
    else
    {
        return false;           //failed to send packet
    }
}

void CCHybridClient::rcv_from_quorum(int min_replies){
    CCHybridPacket p;
    struct timeval sel_timeout;
    int ready;
    std::set<smNode>::iterator it;
    std::set<smNode> servers_pending_;
    char fpath[100];
    int total_sent = servers_sent_.size();
    
    //initialize the servers replied and pkts received
    servers_replies_.clear();

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
            REPORTERROR("Error selecting a server.");
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
                    if ( rcv_pkt<CCHybridPacket>((*it).sock, &p) )
                    {
                        
                        DEBUGING(3,"Received packet from S:%d, Type:%d, Tag:<%d,%d,%d>, Object: <%s, %s, %s>, Counter:%d, Views:%d, Propagated:%d",
                                 p.src_,
                                 p.msgType,
                                 p.obj.get_tag().ts,p.obj.get_tag().wid,p.obj.get_tag().wc,
                                 p.obj.get_id().c_str(),
                                 p.obj.get_path().c_str(),
                                 p.obj.get_value().c_str(),
                                 p.counter, p.obj.views_, p.obj.propagated_tg_);
                        
                        if ( (p.msgType == READACK || p.msgType == WRITEACK || p.msgType == INFORMACK)  && p.counter==req_counter_ )
                        {
                            servers_replies_[(*it).nodeID]=p;
                        }
                        else{ //unknown msgType or the counter did not match
                            switch (p.msgType) {
                                case READACK:
                                case WRITEACK:
                                case COUNTER_ERROR:
                                    REPORTERROR("Counter-Error with Server %d (%s) on socket %d: LC=%d, SC=%d ...",
                                             (*it).nodeID,
                                             (*it).hostname,
                                             (*it).sock,
                                             req_counter_,
                                             p.counter);
                                    break;
                                    
                                default:
                                    REPORTERROR("Unknown-Error with Server %d (%s) on socket %d: Packet details: type %d, counter %d, tag <%d,%d,%d>...",
                                             (*it).nodeID,
                                             (*it).hostname,
                                             (*it).sock,
                                             p.msgType, p.counter,
                                             p.obj.get_tag().ts,p.obj.get_tag().wid,p.obj.get_tag().wc);
                                    
                                    break;
                            }
                        }
                    }
                    else
                    {
                        REPORTERROR("Failed receiving packet form SID: %d on socket SID: %d",
                                    (*it).nodeID,
                                    (*it).sock);
                    }
                    
                    servers_pending_.erase(*it);
                }
            }
        }
        servers_sent_ = servers_pending_;
        
    }while( servers_pending_.size() > 0 );//servers_replied_.size() <= total_sent-servers_alive_.size());
    
    // if only a minority replied -> exit
    if (servers_replies_.size() <= min_replies) {
        DEBUGING(4, "Exiting due to many Server errors...");
        exit(1);
    }
    else
    {
        DEBUGING(4, "Received replies from %d servers...", servers_replies_.size());
    }
}


/************************************************
 *         PROTOCOL SPECIFIC METHODS
 ************************************************/

void CCHybridClient::invoke_op(std::string objID, object_t objType, std::string value){
    std::string op_type="READ";
    int num_ops = 0, num_exch = 0;

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
    if( objects.find(objID) == objects.end() )
    {
        CCHybridObject temp_obj(objID, VALUE_T, meta_dir_);
        objects[objID] = temp_obj;
    }

    obj = &objects[objID];

    //Start the clock
    startTime = get_datetime_sec();

    switch (role_)
    {
    case READER:
        num_exch = invoke_read();
        op_type = "READ";
        num_ops = num_reads_;
        break;
    case WRITER:
        invoke_write(value);
        op_type = "WRITE";
        num_ops = num_writes_;
        break;
    }

    // Stop the clock
    endTime = get_datetime_sec();

    // Calculate operation duration
    totTime+=endTime-startTime;

    DEBUGING(6, "\n**************************************************");
    DEBUGING(6, "%s#:%d, Object: %s, Duration:%f, Tag:<%d,%d,%d>, Values:[%s, %s] @ %d EXCH",
             op_type.c_str(),
             num_ops,
             obj->get_id().c_str(),
             endTime-startTime,
             obj->tg_.ts,obj->tg_.wid,obj->tg_.wc,
             obj->get_value().c_str(), obj->get_pvalue().c_str(),
             num_exch);

    DEBUGING(6, "******************************************************\n");

    //disconnect from the hosts
    DEBUGING(6, "Closing connections...");
    close_connections();

    //save the objects metadata
    obj->save_metadata();
}

int CCHybridClient::invoke_read()
{
    mode_ = PHASE1;

    num_reads_++;

    DEBUGING(6, "***************************************");
    DEBUGING(6,"Invoking read %d on object %s at %s ",
             num_reads_,
             obj->get_id().c_str(),
             get_datetime_str().c_str()
             );
    DEBUGING(6,"***************************************");

    num_msgs_ = 0;

    // PHASE1
    send_to_all(READ);      // send READ to all the servers
    rcv_from_quorum(S_- failures_);      // receive from majority
    process_replies();      // discover the maximum tag among the replies
    // END PHASE1

    //check if we need a second phase
    if (mode_ == PHASE2)
    {
        //PHASE2
        DEBUGING(2, "Performing phase2...");

        num_two_comm_++;

        send_to_all(INFORM); // send the latest tag to all the servers
        rcv_from_quorum(S_- failures_);  // wait for a majority to reply
        // END PHASE2

        return 2;
    }
    else
    {
        num_one_comm_++;

        return 1;
    }
}

void CCHybridClient::invoke_write(std::string v)
{
    mode_ = PHASE1;

    num_writes_++;

    DEBUGING(6,"***************************************");
    DEBUGING(6,"Invoking write %d on object %s at %s ",
             num_writes_,
             obj->get_id().c_str(),
             get_datetime_str().c_str());
   DEBUGING(6,"***************************************");

    num_msgs_ = 0;

    // PHASE1
    // generate new tag
    obj->tg_.ts = obj->tg_.ts + 1;
    obj->tg_.wid = this->nodeID;
    obj->set_pvalue( obj->get_value() );
    obj->set_value( v );

    // send write request to all the servers
    send_to_all(WRITE);

    // receive from majority and terminate
    rcv_from_quorum(S_- failures_);

    num_one_comm_ ++;
    // END PHASE1
}

void CCHybridClient::process_replies()
{
    std::stringstream sstm;

    // Find max_Tag, max_views, and servers that replied with those
    find_max_params();


    // if too many processes viewed this ts => go to a second phase
    if (max_views_ > ((S_ / failures_) - 2) )
    {
        DEBUGING(2, "Checking the PROPAGATION SET, maxViews: %d, bound: %d", max_views_, ((S_ / failures_) - 2));

        // if propagated to less than f+1 servers - go to second phase
        if( prop_servers_.size() < failures_ +1)
        {
            //Phase 2
            mode_ = PHASE2;

            DEBUGING(6, "** Too few servers in propagation set (%d) => Going to PHASE2 **", prop_servers_.size());
        }
        else
        {
            DEBUGING(6, "** Enough servers in propagation set (%d) => RETURNING Value: %s **", prop_servers_.size(), obj->get_value().c_str());

            mode_ = IDLE;
        }

        //reset maxviews
        max_views_ = 0;
    }
    else
    {
        DEBUGING(2, "Checking the PREDICATE, maxViews: %d, bound: %d", max_views_, ((S_ / failures_) - 2));

        //check the predicate to return in one round
        if ( is_predicate_valid () )
        {
            DEBUGING(6, "** Predicate VALID => RETURNING Value: %s **", obj->get_value().c_str());
        }
        else
        {
            DEBUGING(6, "** Predicate INVALID => RETURNING PValue: %s **", obj->get_pvalue().c_str());
        }

        mode_ = IDLE;
    }
}

bool
CCHybridClient::is_predicate_valid()
{
    std::vector<uint16_t> buckets;
    int a;
    std::stringstream sstm;

    buckets.resize((int) (( S_ / failures_) - 1));

    // construct the buckets
    for( auto& x : servers_replies_)
    {
        buckets[x.second.obj.views_]++;
    }

    for(a = ((S_/failures_) - 2); a > 0; a--)
    {
        DEBUGING(2, "PREDICATE LOOP: a= %d, b[a]= %d, bound= %d", a, buckets[a], (S_ - a*failures_));

        if (buckets[a] >= (S_ - a*failures_))
        {
            return true;
        }
        else
        {
            buckets[a-1] += buckets[a];
        }
    }

    return false;
}

/************************************************
 *         HELPER METHODS
 ************************************************/

Tag CCHybridClient::find_max_params()
{
    std::set<CCHybridPacket>::iterator pit;
    
    Tag pkt_tag;
    int pkt_views;

    for(auto& x : servers_replies_)
    {
        pkt_tag = x.second.obj.tg_;
        pkt_views = x.second.obj.views_;

        DEBUGING(3, "Checking tag: <%d, %d>",
                    pkt_tag.ts,
                    pkt_tag.wid
                 );

        //if new max ts discovered - update the local <ts, value, pvalue>
        if( pkt_tag > obj->tg_)
        {
            obj->tg_ = pkt_tag;
            obj->set_value( x.second.obj.get_value() );
            obj->set_pvalue(x.second.obj.get_pvalue());

            DEBUGING(3, "Updated local <tag,value, pvalue> pair to: [<%d, %d>, %s, %s]",
                        obj->tg_.ts,
                        obj->tg_.wid,
                        obj->get_value().c_str(),
                        obj->get_pvalue().c_str()
                     );

            //reset the maxAck set and maxViews variables
            max_servers_.clear();
            prop_servers_.clear();
            max_views_ = 0;
        }

        // enclosed ts == maxTs - include the msg in the maxAck set and update maxViews variable
        if ( pkt_tag == obj->tg_ )
        {
            // add the sender and the views
            max_servers_[x.first] = x.second.obj.views_;

            //max views
            max_views_ = (max_views_ < pkt_views) ? pkt_views : max_views_;

            //check if the ts was propagated by a reader
            if(x.second.obj.propagated_tg_)
            {
                prop_servers_.push_back(x.first);
            }

        }

    }
    
    return obj->tg_;
}

void CCHybridClient::stop(){
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

void CCHybridClient::close_connections(){
    
    //Packet p;
    std::set<smNode>::iterator  it;
    
    req_counter_++;
    
    for (it=servers_connected_.begin(); it!=servers_connected_.end(); it++) {
        prepare_pkt(req_counter_, *it, TERMINATE);
        
        close((*it).sock); /* Close socket */
    }
    
}

void CCHybridClient::terminate(){
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



