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

#ifndef sm_common_h
#define sm_common_h

#include <set>
#include <string>  
#include <ostream>
#include <ctime>
//network on steroids
#include "zhelpers.hpp"

class Serializable
{
public:
    Serializable(){}
    virtual ~Serializable(){}
    
    virtual void serialize(std::ostream& stream) = 0;
    virtual void deserialize(std::istream& stream) = 0;
   // virtual void serialize(std::ostringstream& stream) = 0;
   //virtual void deserialize(std::istringstream& stream) = 0;
};

// Node structure
/*
class Node{

public:
    int nodeID;
    char hostname[100];     // node hostname
    char ip_addr[30];       // node ip address
    int port;               // node incoming port
    int sock;               // node communication socket
    zmq::socket_t *z_sock;  // node zmq socket

    Node(){
        nodeID = 0;
        port = 10000;
        sock = 0;
        z_sock = NULL;
    }
        
    // overloaded < operator
    bool operator < (const Server& n) const
    {
        return (this->nodeID < n.nodeID);
    }

    // overloaded == operator
    bool operator == (const Server& n) const
    {
        //Check if n1 == n2
        return (this->nodeID == n.nodeID);
    }
};
*/

std::string get_datetime_str();
std::string get_date_str();

double get_datetime_sec();



#endif /* sm_common_h */
