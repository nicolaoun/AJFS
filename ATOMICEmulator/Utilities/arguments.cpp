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
    const char optionString[] = "f:v:o:p:t:i:m:d:a:";
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
		}
	}

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
        protocol(1)
{}
