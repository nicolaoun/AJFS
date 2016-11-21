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

#include "arguments.h"
#include <unistd.h>
#include <sstream>
#include <stdexcept>

template<typename T>
void unstringify( T& x, const std::string& s ){
	std::istringstream iss(s);
	if( !(iss >> x) || !(iss >> std::ws).eof() ){
		throw std::runtime_error( "can't interpret: " + s );
	}
}

//!! Requires a help to show how to run the thing
void Arguments::parseArguments(int argc, char** argv){
	optind = 1;
	int c;
    const char optionString[] = "f:v:o:p:t:i:m:d:a:c:h:";
	while( (c = getopt(argc, argv, optionString)) != -1 ) {
		switch (c) {
			case 'f':
				unstringify(path, optarg);
				break;
			case 'v':
				unstringify(value, optarg);
				break;
			case 'o':
				unstringify(objid, optarg);
				break;
			case 'p':
				unstringify(port, optarg);
				break;
			case 't':
				unstringify(type, optarg);
                if(type!="read" && type!="write" && type!="serve")
                {
                    throw std::runtime_error("illegal type: "+ type +". Try using one of [read,write,serve].");
                }
				break;
			case 'i':
				unstringify(nodeid, optarg);
				break;
            case 'm':
                unstringify(menu, optarg);
                break;
            case 'd':
                unstringify(debuglvl, optarg);
                break;
            case 'a':
                unstringify(protocol, optarg);
                break;
            case 'c':
                unstringify(crashes, optarg);
                break;
            case 'h':
            default:
                printHelp();
		}
	}

}

void Arguments::printHelp()
{
    std::cout << "Usage: "<<std::setw(5)<<"./asm [-option1] [value1] [-option2] [value2]....\n";
    std::cout << "Options:\n";
    std::cout << std::setw(10) << "-t" <<std::string(5,' ')<<"Operation type. Legal values: {read,write,server}.\n";
    std::cout << std::setw(10) << "-o" <<std::string(5,' ')<<"Object identifier. In case the object is a file this is the object's filename.\n";
    std::cout << std::setw(10) << "-i" <<std::string(5,' ')<<"Node identifier.\n";
    std::cout << std::setw(10) << "-c" <<std::string(5,' ')<<"Maximum number of crash failures.\n";
    std::cout << std::setw(10) << "-p" <<std::string(5,' ')<<"Port number to listen. Used only with the {serve} type.\n";
    std::cout << std::setw(10) << "-a" <<std::string(5,' ')<<"Algorithm to run [1: ABD, 6:CCHybrid]\n";
    std::cout << std::setw(10) << "-f" <<std::string(5,' ')<<"Object path if object is a file.\n";
    std::cout << std::setw(10) << "-v" <<std::string(5,' ')<<"Object value (if not a file).\n";
    std::cout <<"\n";
    std::cout << std::setw(10) << "-d" <<std::string(5,' ')<<"Debug level [1-7]. 1 prints everything while 7 most important info.\n";
    std::cout << std::setw(10) << "-m" <<std::string(5,' ')<<"Presents a menu or not. You have to type [y,yes,Y,Yes].\n";
    exit(0);
}


Arguments::Arguments() :
		path(""),
		value(""),
		objid(""),
		port(4000),
		type("serve"),
		nodeid(0),
        menu("no"),
        debuglvl(4),
        protocol(1),
        crashes(-1)
{}
