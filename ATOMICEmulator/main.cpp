//
//  main.cpp
//  SharedMemory_Simulator
//
//  Created by nicolas on 12/5/15.
//  Copyright © 2015 Imdea Networks. All rights reserved.
//

#include <iostream>
//#include "Proto-ABD/ABDClient.hpp"
//#include "Proto-ABD/ABDReader.hpp"
#include "ABD/abd_client.hpp"
#include "ABD/abd_server.hpp"
#include "arguments.h"
#include <string>

using namespace std;

#define MIN_TIMEOUT 1

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
        double frequency;
        int ops = 10;
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

            //ABDClient *r = new ABDClient (args.nodeid, READER, 10, 10);
            ABDClient client(args.nodeid, client_type, args.path);
            client.set_debug_lvl(args.debuglvl);

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

                        client.invoke_op(args.objid, ".");
                        break;

                    // perform multiple opearions in a specified frequency
                    case 2:
                        std::cout << "\nEnter "<< args.type <<" frequency (in seconds): ";
                        std::cin >> frequency;
                        while(ops > 0)
                        {
                            client.invoke_op(args.objid, ".");
                            int timeout = (rand()%((int) frequency));

                            std::cout << "\n\n****************\nNext operation in " << timeout << " sec...\n****************\n\n";

                            if(timeout < MIN_TIMEOUT)
                                sleep(MIN_TIMEOUT);
                            else
                                sleep(timeout);

                            ops --;
                        }
                        break;

                    default:
                        std::cout << "Exiting....\n\n";
                        break;
                    }

                }

                client.terminate();
            }
            else //just invoke a single operation
            {
                client.invoke_op(args.objid, ".");

                if ( client_type == READER || (client_type == WRITER && client.has_commited()) )
                {
                    exit(EXIT_SUCCESS);
                }
                else
                {
                    exit(EXIT_FAILURE);
                }
            }
        }


        if ( args.type == "serve" )
        {
            if (args.port == 0 ) {
                in_error("Expected port number for the Server. Use -p option.");
            }

            //!!ABDServer *s = new ABDServer (args.nodeid, args.port);
            ABDServer s(args.nodeid, args.port);
            s.set_debug_lvl(args.debuglvl);
            s.start();
        }

    } catch (const std::exception &e) {
        std::cerr << "ASM : " << e.what() << std::endl;
    }
}

