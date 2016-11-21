//
//  main.cpp
//  SharedMemory_Simulator
//
//  Created by nicolas on 12/5/15.
//  Copyright © 2015 Imdea Networks. All rights reserved.
//

#include <iostream>
//#include "Proto-ABD/ABDclient.hpp"
//#include "Proto-ABD/ABDReader.hpp"
#include "ABD/abd_client.hpp"
#include "ABD/abd_server.hpp"
#include "CCHYBRID/cchybrid_client.hpp"
#include "CCHYBRID/cchybrid_server.hpp"
#include "arguments.h"
#include <string>

using namespace std;

#define MIN_TIMEOUT 1

template <class ProtoType>
void handle_client(ProtoType* client, Arguments);

template <class ProtoType>
void handle_server(ProtoType* s, Arguments);

void in_error(std::string e)
{
    std::cout << "Input Error: " << e << "\n\n";
    exit(1);
}

int main(int argc, char** argv)
{
    // Wrap everything in a try block.  Do this every time,
    // because exceptions will be thrown for problems.
    try {
        Arguments args;
        args.parseArguments(argc, argv);

        std::cout << "\n*********************************************************\n";
        std::cout << "Initializing "<< args.type <<" process ID: "<< args.nodeid << "\n";
        std::cout << "*********************************************************\n\n";

        if ( args.type == "read" || args.type == "write")
        {
            if (args.objid == "" ) {
                in_error("Object identifier not defined or is empty. Use -o option to define the object to read.");
            }

            int client_type = (args.type == "read")? READER : WRITER;

            ABDClient *abd_client;
            CCHybridClient *cchybrid_client;

            switch((Protocol) args.protocol)
            {
            case PROTO_ABD:
                std::cout << "*********************************************************\n";
                std::cout << "                 Protocol running: ABD\n";
                std::cout << "*********************************************************\n\n";
                abd_client = new ABDClient(args.nodeid, client_type, args.path);
                handle_client<ABDClient>(abd_client, args);
                break;
            case PROTO_CCHYBRID:
                std::cout << "*********************************************************\n";
                std::cout << "                 Protocol running: CCHYBRID\n";
                std::cout << "*********************************************************\n\n";
                std::cout << "Root Dir:" << args.path << "\n";
                cchybrid_client = new CCHybridClient(args.nodeid, client_type, args.path);

                //set the number of failures
                if(args.crashes > 0)
                {
                    cchybrid_client->set_failures(args.crashes);
                }

                handle_client<CCHybridClient>(cchybrid_client, args);
                break;
            default:
                std::cout << "\n *** [ERROR] This protocol is not implemented yet! ***\n\n";
                exit(EXIT_FAILURE);
            }
        }


        if ( args.type == "serve" )
        {
            if (args.port == 0 ) {
                in_error("Expected port number for the Server. Use -p option.");
            }

            ABDServer *abd_server;
            CCHybridServer *cchybrid_server;

            //!!ABDServer *s = new ABDServer (args.nodeid, args.port);
            switch( (Protocol) args.protocol)
            {
            case PROTO_ABD:
                std::cout << "*********************************************************\n";
                std::cout << "                 Protocol running: ABD\n";
                std::cout << "*********************************************************\n\n";
                abd_server = new ABDServer(args.nodeid, args.port);
                handle_server<ABDServer>(abd_server, args);
                break;
            case PROTO_CCHYBRID:
                std::cout << "*********************************************************\n";
                std::cout << "                 Protocol running: CCHYBRID\n";
                std::cout << "*********************************************************\n\n";
                cchybrid_server = new CCHybridServer(args.nodeid, args.port);
                handle_server<CCHybridServer>(cchybrid_server, args);
                break;
            default:
                std::cout << "\n *** [ERROR] This protocol is not implemented yet! ***\n\n";
                exit(EXIT_FAILURE);
            }
        }

    } catch (const std::exception &e)
    {
        std::cerr << "ASM : " << e.what() << std::endl;
    }
}

template <class ProtoType>
void handle_client(ProtoType* client, Arguments args)
{
    object_t o_type;
    double frequency;
     int val = 0;
    int ops = 10;

    int client_type = (args.type == "read")? READER : WRITER;

    //check the type of the object
    if( args.value == "" )
    {
        o_type = FILE_T;
        std::cout << "oType: FILE";
    }
    else
    {
        o_type = VALUE_T;
        std::cout << "oType: VALUE";
    }


    client->set_debug_lvl(args.debuglvl);

    //show operation menu - "yes" or "y":show menu
    if (args.menu == "yes" || args.menu == "Yes" || args.menu == "y" || args.menu == "Y")
    {
        int choice = 0;

        while (choice!=3) {

            std::cout << "\n\n***** Menu *****\n";
            std::cout << "1. "<< args.type <<"\n";
            std::cout << "2. Auto "<< args.type <<"\n";
            std::cout << "3. Exit\n";
            std::cout << "Choice:";
            std::cin >> choice;


            switch (choice) {
            case 1:
                std::cout << "\nInvoking "<< args.type <<" operation....\n";

                client->invoke_op(args.objid, o_type, args.value);
                break;

            // perform multiple operations in a specified frequency
            case 2:
                std::cout << "\nEnter "<< args.type <<" frequency (in seconds): ";
                std::cin >> frequency;
                while(ops > 0)
                {
                    client->invoke_op(args.objid, o_type, std::to_string(val));
                    int timeout = (rand()%((int) frequency));

                    std::cout << "\n\n****************\nNext operation in " << timeout << " sec...\n****************\n\n";

                    if(timeout < MIN_TIMEOUT)
                        sleep(MIN_TIMEOUT);
                    else
                        sleep(timeout);

                    ops --;
                    val++;
                }
                break;

            default:
                std::cout << "Exiting....\n\n";
                break;
            }

        }

        client->terminate();
    }
    else //just invoke a single operation
    {
        if ( args.menu == "auto" )
        {
            // set the frequency between 3-10
            frequency = (rand()%7)+3;

            while(ops > 0)
            {
                // timeout between 2 - frequency secs
                int timeout = (rand()%((int) frequency))+2;

                client->invoke_op(args.objid, o_type, std::to_string(val));

                std::cout << "\n\n****************\nNext operation in " << timeout << " sec...\n****************\n\n";

                if(timeout < MIN_TIMEOUT)
                    sleep(MIN_TIMEOUT);
                else
                    sleep(timeout);

                ops --;
                val++;
            }

            client->terminate();
        }
        else
        {
            client->invoke_op(args.objid, o_type, args.value);
            exit(EXIT_SUCCESS);
        }
    }
}

template <class ProtoType>
void handle_server(ProtoType* s, Arguments args)
{
    s->set_debug_lvl(args.debuglvl);
    s->start();
}

