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

#include "abd_server.hpp"

ABDServer::ABDServer(int serverID, int port, int S, int W, int R, int Q, int nfq, double cInt) {
    
    nodeID_ = serverID;
    S_ = S;     //# of servers
    W_ = W;     //# of writers
    R_ = R;     //# of readers
    Q_ = Q;     //# of quorums
    non_faulty_qid_ = nfq;
    crash_freq_ = 0.0;
    port_ = port;
    role_ = SERVER;
    
    // specify the Server's paths
    std::stringstream sstm;
    sstm << "./server_" << nodeID_;
    srv_root_dir_ = sstm.str();
    
    if( !directoryExists(srv_root_dir_) ){
        createDirectory(srv_root_dir_);
    }
    
    rcvd_files_dir_ = srv_root_dir_ + "/rcvd_files";
    if (!directoryExists(rcvd_files_dir_)){
        createDirectory(rcvd_files_dir_);
    }
    
    meta_dir_ = srv_root_dir_ + "/.meta";
    if(!directoryExists(meta_dir_)) {
        createDirectory(meta_dir_);
    }
    
    logs_dir_ = srv_root_dir_ + "/logs" ;
    if(!directoryExists(logs_dir_)){
        createDirectory(logs_dir_);
    }
    
    char log_fname[10];
    sprintf(log_fname, "/s%d",nodeID_);
    logs_dir_ += log_fname;
    init_logfile(logs_dir_);
    
    DEBUGING(4,"Initialized on Port:%d\n",
             port_);
    
    //Initialize the node's local tag
    tg_.ts=0;
    tg_.wid=0;
    tg_.wc=0;
    
    serve_clients_.clear();
    /*
     serve_counter=(int *)malloc((R_+W_)*sizeof(int));
     for(int i=0; i<=R_+W_; i++){  //array initialization
     serve_counter[i]=0;
     }
     */
    
}

void ABDServer::start(){
    Packet p;
    int fd[2],cid;                                    //pipe variables
    //vector<int> child_list;                         //list to maintain the connected nodes
    //int child_num=0;
    struct timeval timeout;
    int ready;
    Client *tmp_client;
    
    DEBUGING(4, "Starting server %d\n", nodeID_);
    
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
    
    DEBUGING(4, "Listening for connections to port %d...\n", port_);
    
    while(1) {
        
        acceptClientConnection();
        
    } /* end of while(1) */
}

//Method to create a socket
void ABDServer::createSocket()
{
    if ((sock_ = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
        REPORTERROR("Socket Creation");
        exit(1);
    }
    
    //setnonblocking(sock_);
}

//Method to initialize the server structs
void ABDServer::initializeServerAddress()
{
    server.sin_family = PF_INET;                    /* Internet domain */
    server.sin_addr.s_addr = htonl(INADDR_ANY);     /* My Internet address */
    server.sin_port = htons(port_);                 /* The given port */
    serverptr = (struct sockaddr *) &server;
    serverlen = sizeof (server);
}

//Method to bind the socket with ip and port
void ABDServer::bindSocket()
{
    if (bind(sock_, serverptr, serverlen) < 0) {
        REPORTERROR("Binding Socket");
        exit(1);
    }
}

//Method to call socket listening
void ABDServer::listenSocket()
{
    if (listen(sock_, MAX_CONNECTIONS ) < 0) { /* 5 max. requests in queue */
        REPORTERROR("Listen");
        exit(1);
    }
}

//Method to accept a new connection request from a client
void ABDServer::acceptClientConnection()
{
    Client *tmp_client;
    
    clientptr = (struct sockaddr *) &client;
    clientlen = sizeof(client);
    
    DEBUGING(4, "Accepting connections on port %d\n", port_);
    
    /* Accept connection */
    if ((newsock_ = accept(sock_, clientptr, (socklen_t *) &clientlen)) < 0) {
        REPORTERROR("Accepting new client");
        exit(1);
    }
    
    // Make the socket non-blocking
    //setnonblocking(newsock_);
    
    // Initialize new client
    tmp_client = new Client();
    tmp_client->req_counter = 0;
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
        
        DEBUGING(4, "Accepted connection from %s (IP:%s)\n",
                 tmp_client->hostname,
                 tmp_client->ip_addr);
    }
    
    //create a new thread to handle the client
    try {
        threads.push_back(std::thread(&ABDServer::serveClient, this, tmp_client));
    } catch (int err) {
        REPORTERROR("An exception occured while creating the thread. Err No:%d\n",  err);
    }
}


void ABDServer::serveClient(Client *tmp_client)
{
    Packet p;
    std::string type_str;
    RWObject local_replica;
    std::vector<Client>::iterator it;
    int msg_type = READ;
    
    while (msg_type != TERMINATE) {
        
        try {
            if( !rcv_pkt<Packet>(tmp_client->sock, &p) )         // receive a packet from the client
            {
                REPORTERROR("Failed receiving packet!\n");
                return;
            }
            
            tmp_client->nodeID = p.src_;
            
            DEBUGING(2, "Received msg: ClientID = %d, Socket = %d...\n", tmp_client->nodeID, tmp_client->sock);
            
            msg_type = p.msgType;
            
            switch (msg_type) {
                case READ:
                case WRITE:
                    type_str = (p.msgType == READ)? "READ" : "WRITE";
                    
                    //find the requested object in the local set of replicas
                    local_replica = getLocalReplica(p.obj);
                    
                    std::cout << "\n********************************************************\n";
                    DEBUGING(6,"%s msg from %d, Object ID: %s, Tag Rcvd: <%d,%d>, Local Tag: <%d, %d>\n",
                             type_str.c_str(),
                             p.src_,
                             p.obj.get_id().c_str(),
                             p.obj.get_tag().ts, p.obj.get_tag().wid,
                             local_replica.tg_.ts, local_replica.tg_.wid
                             );
                    std::cout << "---------------------------------------------------------\n";
                    
                    
                    // if it is a write msg -> receive the file
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
                    
                    mtx.lock();
                    serve(&p, tmp_client, local_replica);
                    mtx.unlock();
                    break;
                case TERMINATE:
                default:
                    DEBUGING(2,"Closing connection with %d...\n",
                             p.src_);
                    
                    close(tmp_client->sock);                        // Close socket
                    it = std::find(serve_clients_.begin(), serve_clients_.end(), *tmp_client);
                    if (it != serve_clients_.end())
                    {
                        //serve_clients_.erase(serve_clients_.begin()+i); // Erase from the list of clients
                        serve_clients_.erase(it); // Erase from the list of clients
                    }
                    //FD_CLR(tmp_client->sock, &childFDs);            // Remove from FD
                    //child_list[i]=0;
                    //FD_CLR(child_list[i], &childFDs); // Remove from FD
                    //exit(0);
                    break;
            }
        } catch (int err) {
            REPORTERROR("Error while serving the client. Err No: %d", err);
            close(tmp_client->sock);
        }

    }
}

//Method to search for an object in the set of replicas and return that object if found
RWObject ABDServer::getLocalReplica(RWObject obj)
{
    std::set<RWObject*>::iterator oit;
    RWObject ret_obj;
    
    //find the object of interest
    oit = objects_set.find(&obj);
    
    // if the object is not found -> initialize the object with default values
    if( oit == objects_set.end() )
    {
        RWObject temp_obj(obj.get_id(), meta_dir_);
        
        //oit = objects_set.insert(oit, temp_obj);
        ret_obj = insertLocalReplica(temp_obj);
    }
    else
    {
        ret_obj = *((RWObject*) *oit);
    }
    
    return ret_obj;
}

//Method to insert a replica in the replica set, returns a pointer to the element inserted
RWObject ABDServer::insertLocalReplica(RWObject obj)
{
    std::pair<std::set<RWObject*>::iterator,bool> ret;
    std::set<RWObject*>::iterator oit;
    RWObject *ret_obj;
    
    //remove the outdated object if exists
    oit = objects_set.find(&obj);
    if( oit == objects_set.end() )
    {
        //objects_set.erase(oit);
        //}
    
        //insert the received one
        ret = objects_set.insert(&obj);
        ret_obj = (RWObject*) *ret.first;
    }
    else
    {
        ret_obj = *oit;
    }
    
    return *ret_obj;
    
}

void ABDServer::serve(Packet *pkt, Client *c, RWObject replica)
{
    //int pid = pkt->src_ - S_;   // position of requesting process in the counter array
    int pid = pkt->src_;
    std::vector<Client>::iterator cit;
    
    //if(!crashed_){
    c->nodeID = pid;
    
    // file paths
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
    
    
    if(pkt->counter >= c->req_counter)
    {
        c->req_counter = pkt->counter;
        
        // Send the appropriate reply
        switch(pkt->msgType){
            case READ:
                
                prepare_pkt(c, READACK, replica);                   // send a READACK to client c
                
                // if we failed to send the file -> exit the thread
                if ( !send_file(c->sock, (char*) to.c_str()) ) exit(EXIT_FAILURE);
                
                DEBUGING(4,"READACK to %d, rcvTag=<%d,%d>, sendTag=<%d,%d>\n",
                         pkt->src_,
                         pkt->obj.get_tag().ts, pkt->obj.get_tag().wid,
                         replica.tg_.ts, replica.tg_.wid);
                std::cout << "********************************************************\n";
                break;
            case WRITE:
                if(pkt->obj.get_tag() > replica.tg_)
                {
                    //replace with the new object
                    //replica = insertLocalReplica(pkt->obj);
                    if( !replica.set_latest_tag(pkt->obj.tg_) )
                    {
                        REPORTERROR("Failed changing replica's tag to <%d,%d>", pkt->obj.tg_.ts, pkt->obj.tg_.wid);
                        exit(EXIT_FAILURE);
                    }
                    
                    // copy the file to the permanent storage
                    if(fileExists(from))
                    {
                        copyFile(from, to);
                    
                        DEBUGING(4,"New maxTag=<%d,%d> From:%d\n --> Copied journal from %s to %s\n",
                             replica.tg_.ts, replica.tg_.wid,
                             pkt->src_,
                             from.c_str(),
                             to.c_str()
                             );
                    }
                    else
                    {
                        DEBUGING(4,"New maxTag=<%d,%d> From:%d\n --> Failed Copied journal %s to %s\n",
                                 replica.tg_.ts, replica.tg_.wid,
                                 pkt->src_,
                                 from.c_str(),
                                 to.c_str()
                                 );
                    }
                }
                
                prepare_pkt(c, WRITEACK, replica);                  // send a WRITEACK to client c
                
                DEBUGING(4,"WRITEACK from %d, rcvTag=<%d,%d>, sendTag=<%d,%d>\n",
                         pkt->src_,
                         pkt->obj.get_tag().ts, pkt->obj.get_tag().wid,
                         replica.tg_.ts, replica.tg_.wid);
                std::cout << "********************************************************\n";
                break;
        }
        
    }
    else{
        DEBUGING(2,"Discarding pkt from %d, pktCounter:%d, srvCounter:%d\n",
                 pkt->src_,
                 pkt->counter,
                 c->req_counter);
        prepare_pkt(c, COUNTER_ERROR, pkt->obj);//,0);
    }
    //}
    /*else
     DEBUGING(2,"Server %d crashed.\n",nodeID_);
     */
}

void ABDServer::prepare_pkt(Client *dest, int msgType, RWObject obj){
    
    Packet p;
    std::set<Tag>::iterator it;
    
    //Specify the destination of the packet
    p.src_=nodeID_;
    p.dst_=dest->nodeID;
    
    //Specify the fields of the packet accordingly
    p.msgType = msgType;
    p.counter = dest->req_counter;
    p.obj = obj;
    //p.tg = tg_;
    //p.value = value_;
    
    send_pkt(dest->sock, &p);
    
    
    DEBUGING(2,"Sent packet to PID:%d, Type:%d, Tag:<%d,%d,%d>, Counter:%d Object:%s\n",
             p.dst_,
             p.msgType,
             p.obj.get_tag().ts,p.obj.get_tag().wid,p.obj.get_tag().wc,
             p.counter,
             p.obj.get_id().c_str());
}

