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

#include "cchybrid_server.hpp"

CCHybridServer::CCHybridServer(int serverID, int port) {
    
    nodeID = serverID;
    crash_freq_ = 0.0;
    port_ = port;
    role_ = SERVER;
    
    // specify the Server's paths
    std::stringstream sstm;
    sstm << "./server_" << nodeID;
    srv_root_dir_ = sstm.str();
    
    setup_dirs(srv_root_dir_);
    
    DEBUGING(4,"Initialized on Port:%d",
             port_);
    
    serve_clients_.clear();   
}

void CCHybridServer::setup_dirs(std::string root_dir)
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

void CCHybridServer::start(){
    int fd[2],cid;                                    //pipe variables
    struct timeval timeout;
    int ready;
    smNode *tmp_client;
    
    DEBUGING(4, "Starting server %d", nodeID);
    
    timeout.tv_sec=1;
    timeout.tv_usec=0;
    
    /* Create socket */
    createSocket();
    
    /* Initialize server structs */
    initializeServerAddress();
    
    /* Bind socket to address */
    bindSocket();
    
    /* Listen for connections */
    listenSocket();
    
    DEBUGING(4, "Listening for connections to port %d...", port_);
    
    while(1) {
        
        acceptClientConnection();
        
    } /* end of while(1) */
}

//Method to create a socket
void CCHybridServer::createSocket()
{
    if ((sock_ = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
        REPORTERROR("Socket Creation");
        exit(1);
    }
    
    //setnonblocking(sock_);
}

//Method to initialize the server structs
void CCHybridServer::initializeServerAddress()
{
    server.sin_family = PF_INET;                    /* Internet domain */
    server.sin_addr.s_addr = htonl(INADDR_ANY);     /* My Internet address */
    server.sin_port = htons(port_);                 /* The given port */
    serverptr = (struct sockaddr *) &server;
    serverlen = sizeof (server);
}

//Method to bind the socket with ip and port
void CCHybridServer::bindSocket()
{
    if (bind(sock_, serverptr, serverlen) < 0) {
        REPORTERROR("Binding Socket");
        exit(1);
    }
}

//Method to call socket listening
void CCHybridServer::listenSocket()
{
    if (listen(sock_, MAX_CONNECTIONS ) < 0) { /* 5 max. requests in queue */
        REPORTERROR("Listen");
        exit(1);
    }
}

//Method to accept a new connection request from a client
void CCHybridServer::acceptClientConnection()
{
    smNode *tmp_client;
    
    clientptr = (struct sockaddr *) &client;
    clientlen = sizeof(client);
    
    DEBUGING(4, "Accepting connections on port %d", port_);
    
    /* Accept connection */
    if ((newsock_ = accept(sock_, clientptr, (socklen_t *) &clientlen)) < 0) {
        REPORTERROR("Accepting new client");
        exit(1);
    }
    
    // Make the socket non-blocking
    //setnonblocking(newsock_);
    
    // Initialize new client
    tmp_client = new smNode();
    tmp_client->req_counter_ = 0;
    tmp_client->sock = newsock_;
    
    // add client in the list of clients
    serve_clients_.push_back(*tmp_client);
    
    /* Find client's address */
    if ((rem = gethostbyaddr((char *) &client.sin_addr.s_addr,
                             sizeof (client.sin_addr.s_addr), client.sin_family)) == NULL) {
        char IPstr[INET_ADDRSTRLEN];
        inet_ntop(AF_INET,&client.sin_addr, IPstr,(socklen_t) clientlen);
        REPORTERROR("Gethostbyaddr for client %s",IPstr);
        
        //exit(1);
    }
    else{
        strcpy(tmp_client->hostname, rem->h_name);
        strcpy(tmp_client->ip_addr, rem->h_addr);
        
        DEBUGING(4, "Accepted connection from %s (IP:%s)",
                 tmp_client->hostname,
                 tmp_client->ip_addr);
    }
    
    //create a new thread to handle the client
    try {
        threads.push_back(std::thread(&CCHybridServer::serve_client, this, tmp_client));
    } catch (int err) {
        REPORTERROR("An exception occured while creating the thread. Err No:%d",  err);
    }
}


void CCHybridServer::serve_client(smNode *tmp_client)
{
    CCHybridPacket p;
    std::string type_str;
    CCHybridObject *local_replica;
    std::vector<smNode>::iterator it;
    int msg_type = READ;
    
    while (msg_type != TERMINATE) {
        
        try {
            if( !rcv_pkt<CCHybridPacket>(tmp_client->sock, &p) )         // receive a packet from the client
            {
                REPORTERROR("Failed receiving packet!");
                return;
            }
            
            tmp_client->nodeID = p.src_;
            
            DEBUGING(2, "Received msg: ClientID = %d, Socket = %d...", tmp_client->nodeID, tmp_client->sock);
            
            msg_type = p.msgType;
            
            if(msg_type == READ || msg_type == WRITE || msg_type == INFORM)
            {
                type_str = (p.msgType == READ)? "READ" : ( (p.msgType == WRITE) ? "WRITE" : "INFORM" );

                //find the requested object in the local set of replicas
                local_replica = get_local_replica(p.obj);

                DEBUGING(6,"\n********************************************************");
                DEBUGING(6,"%s msg from %d, Object ID: %s, Tag Rcvd: <%d,%d>, Local Tag: <%d, %d>",
                         type_str.c_str(),
                         p.src_,
                         p.obj.get_id().c_str(),
                         p.obj.get_tag().ts, p.obj.get_tag().wid,
                         local_replica->tg_.ts, local_replica->tg_.wid
                         );
                DEBUGING(6,"---------------------------------------------------------");


                // if it is a write msg -> receive the file
                /*
                    if ( msg_type == WRITE)
                    {
                        char fpath[100];
                        sprintf(fpath, "%s/rcvd_files/src%d.[%d,%d].%s.temp",
                                srv_root_dir_.c_str(),
                                p.src_,
                                p.obj.get_tag().ts, p.obj.get_tag().wid,
                                p.obj.get_id().c_str()
                                );
                        if( !rcv_file(tmp_client->sock, fpath) )
                            return;    // receive the object's file from the client
                    }
                    */

                mtx.lock();
                serve(&p, tmp_client, local_replica);
                mtx.unlock();
            }
            else
            {
                DEBUGING(2,"Closing connection with %d...",
                         p.src_);

                close(tmp_client->sock);                        // Close socket
                it = std::find(serve_clients_.begin(), serve_clients_.end(), *tmp_client);
                if (it != serve_clients_.end())
                {
                    serve_clients_.erase(it); // Erase from the list of clients
                }
            }
        } catch (int err)
        {
            REPORTERROR("Error while serving the client. Err No: %d", err);
            close(tmp_client->sock);
        }

    }
}

//Method to search for an object in the set of replicas and return that object if found
CCHybridObject* CCHybridServer::get_local_replica(CCHybridObject obj)
{   
    // if the object is not found -> initialize the object with default values
    if( local_objects_.find(obj.objID_) == local_objects_.end() )
    {
        // Create an object of the same type
        CCHybridObject temp_obj(obj.get_id(), obj.get_type(), meta_dir_);
        
        //oit = objects_set.insert(oit, temp_obj);
        local_objects_[obj.objID_] = temp_obj;
        //ret_obj = insertLocalReplica(temp_obj);
    }
    
    return &local_objects_[obj.objID_];
}

void CCHybridServer::serve(CCHybridPacket *pkt, smNode *c, CCHybridObject *replica)
{
    int pid = pkt->src_;
    CCHybridPacket h_pkt;
    std::vector<smNode>::iterator cit;
    
    //if(!crashed_){
    c->nodeID = pid;
    
    // file paths
    /*
    std::stringstream sstm;
    sstm << rcvd_files_dir_.c_str() << "/src" << pkt->src_ << ".[" << pkt->obj.get_tag().ts << "," << pkt->obj.get_tag().wid << "]." << pkt->obj.get_id().c_str() << ".temp";
    
    std::string from = sstm.str();       // temp storage
    
    sstm.str("");
    sstm.clear();
    sstm << srv_root_dir_.c_str() << "/" << pkt->obj.get_id().c_str(); //<< ".journal";
    
    std::string to = sstm.str();         // permanent filename
    
    // copy file if it does not exists
    if(fileExists(from) && !fileExists(to))
    {
        copyFile(from, to);
    }
    */
    
    
    if(pkt->counter >= c->req_counter_)
    {
        c->req_counter_ = pkt->counter;

        if ( replica->tg_ < pkt->obj.tg_ )
        {
            // Update the local replica
            replica->tg_ = pkt->obj.tg_;
            replica->set_value(pkt->obj.get_value());
            replica->set_pvalue(pkt->obj.get_pvalue());

            // reset the seen set
            replica->seen_.clear();

            //reset propagation flag
            replica->propagated_tg_ = 0;
        }

        // insert node in the seen set
        replica->seen_.insert(*c);
        replica->views_ = replica->seen_.size();

        DEBUGING(2, "Seen set size: %d", replica->seen_.size());

        // changed the propagation flag if coming from reader
        if ( pkt->obj.tg_ == replica->tg_ && pkt->msgType == INFORM)
        {
            replica->propagated_tg_ = 1;
        }

        std::string ackstr;
        // Send the appropriate reply
        switch(pkt->msgType){
            case READ:
                
                h_pkt = prepare_pkt(c, READACK, *replica);                   // send a READACK to client c
                
                // if we failed to send the file -> exit the thread
                if ( !send_pkt<CCHybridPacket>(c->sock, &h_pkt) ) exit(EXIT_FAILURE);

                // if we failed to send the file -> exit the thread
                //if ( !send_file(c->sock, (char*) to.c_str()) ) exit(EXIT_FAILURE);
                
                ackstr = "READACK";

                break;
            case WRITE:

                h_pkt = prepare_pkt(c, WRITEACK, *replica);                  // send a WRITEACK to client c

                // if we failed to send the file -> exit the thread
                if ( !send_pkt<CCHybridPacket>(c->sock, &h_pkt) ) exit(EXIT_FAILURE);
                
                ackstr = "WRITEACK";
                break;
        case INFORM:

            h_pkt = prepare_pkt(c, INFORMACK, *replica);                  // send a WRITEACK to client c

            // if we failed to send the file -> exit the thread
            if ( !send_pkt<CCHybridPacket>(c->sock, &h_pkt) ) exit(EXIT_FAILURE);

            ackstr = "INFORMACK";
            break;
        }

        DEBUGING(4,"%s to %d, rcvTag=<%d,%d>, sendTag=<%d,%d>",
                 ackstr.c_str(),
                 pkt->src_,
                 pkt->obj.get_tag().ts, pkt->obj.get_tag().wid,
                 replica->tg_.ts, replica->tg_.wid);
        DEBUGING(6,"********************************************************");

        // update replica's local metadata
        replica->save_metadata();
        
    }
    else{
        DEBUGING(2,"Discarding pkt from %d, pktCounter:%d, srvCounter:%d",
                 pkt->src_,
                 pkt->counter,
                 c->req_counter_);
        prepare_pkt(c, COUNTER_ERROR, *replica);//,0);
    }
}

CCHybridPacket CCHybridServer::prepare_pkt(smNode *dest, int msgType, CCHybridObject obj){
    
    CCHybridPacket p;
    std::set<Tag>::iterator it;
    
    //Specify the destination of the packet
    p.src_=nodeID;
    p.dst_=dest->nodeID;
    
    //Specify the fields of the packet accordingly
    p.msgType = msgType;
    p.counter = dest->req_counter_;
    p.obj = obj;

    DEBUGING(2,"Prepared packet to PID:%d, Type:%d, Tag:<%d,%d,%d>, Counter:%d Object:%s Views:%d Propagated:%d",
             p.dst_,
             p.msgType,
             p.obj.get_tag().ts,p.obj.get_tag().wid,p.obj.get_tag().wc,
             p.counter,
             p.obj.get_id().c_str(),
             p.obj.views_, p.obj.propagated_tg_);

    return p;
    
}

