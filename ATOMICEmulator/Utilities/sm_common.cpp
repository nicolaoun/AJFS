//
//  sm_common.cpp
//  Atomic_Shared_Memory
//
//  Created by Nicolas Nicolaou on 13/04/16.
//
//

#include "sm_common.h"


std::string get_datetime_str()
{
    time_t t = time(0);   // get time now
    //struct tm * now = localtime( & t );
    char date[100];
    
    std::strftime(date, sizeof(date), "%F %T", localtime( & t ));

    std::string res(date);
    return res;
}

std::string get_date_str()
{
    time_t t = time(0);   // get time now
    //struct tm * now = localtime( & t );
    char date[100];
    
    std::strftime(date, sizeof(date), "%F", localtime( & t ));
    
    std::string res(date);
    return res;
}

double get_datetime_sec()
{
    struct timeval sysTime;

    gettimeofday(&sysTime, NULL);

    return sysTime.tv_sec + (sysTime.tv_usec/1000000.0);
}
