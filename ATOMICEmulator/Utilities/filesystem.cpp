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

#include <sys/stat.h>
#include "filesystem.h"

bool fileExists(const std::string &s){
	bool exists = false;
	struct stat st;
	if(stat(s.c_str(), &st) == 0 ) {
		exists = true;
	}
	return exists;
}

void createDirectory(const std::string &s){
	if(!directoryExists(s)) {
		mkdir(s.c_str(), 0777);
	}
}

bool directoryExists(const std::string &s){
    return fileExists(s);
}

/**
  * Method to copy a file
  * Input: source, destination
 */
void copyFile(const std::string from, const std::string to)
{
    if(fileExists(from)){
        std::ifstream  src(from.c_str(), std::ios::binary);
        std::ofstream  dst(to.c_str(),   std::ios::binary);
        
        dst << src.rdbuf();
        
        src.close();
        dst.close();
    }
}

/**
 *  Return the size of a file
 */
int getFileSize(const std::string f)
{
    struct stat st;
    
    if (stat(f.c_str(), &st) < 0)
    {
        return -1;
    }
    
    // Sending file size //
    return st.st_size;
}


/**
 *  Return file checksum
 */
/*
char *md5Checksum(const std::string filename)
{
    GChecksum   *cs;
    guchar       data[MAX_SIZE];
    gsize        size = 0;
    FILE        *input;
    
    const char *sum;
    
    cs = g_checksum_new( G_CHECKSUM_MD5 );
    input = fopen( filename.c_str(), "rb" );
    do
    {
        size = fread( (void *)data, sizeof( guchar ), MAX_SIZE, input );
        g_checksum_update( cs, data, size );
    }
    while( size == MAX_SIZE );
    fclose( input );
    
    sum = g_checksum_get_string( cs );
   
    return (char*)sum;
    
}
 */

