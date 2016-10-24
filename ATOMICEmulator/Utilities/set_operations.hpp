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

#ifndef SetOperation_hpp
#define SetOperation_hpp

#include <stdio.h>
#include <set>
#include <string>
#include <sstream>

template < class TargetClass >
class SetOperation
{
    SetOperation();
    
public:
    // Goal: Return union between two std::sets
    // Output: Set union
    //
    static std::set<TargetClass> unite(const std::set<TargetClass> &set1,
                                       const std::set<TargetClass> &set2);
    
    // Goal: Return intersection between two std::sets
    // Output: Set intersection
    //
    static std::set<TargetClass> intersect(const std::set<TargetClass> &set1,
                                           const std::set<TargetClass> &set2);
    
    // Goal: Find the complement of a std::set
    // Input: The universe of elements and the
    // std::set of which we want to ge the complement
    // Ourput: the complement
    static std::set<TargetClass> setcomplement(const std::set<TargetClass> &U,
                                               const std::set<TargetClass> &S);
    
    // Goal: Check if the std::set given as first parameter is
    // a substd::set of the second std::set.
    // Output: 1 if first std::set is a substd::set of a second, 0 otherwise
    //
    static int subset(const std::set<TargetClass> &set1,
                      const std::set<TargetClass> &set2);
    
    //Goal: print the elements in a std::set
    //Input: (a) The std::set we want to print
    //       (b) name of the std::set
    //       (c) prefix of each element of the std::set
    static std::string print_set(const std::set<TargetClass> &S,
                                 const std::string &name,
                                 const std::string &prefix);
    /*
     static std::set<TargetClass> intersect(std::set<TargetClass> std::set1, std::set<TargetClass> std::set2);
    static std::set<TargetClass> std::setcomplement(std::set<TargetClass> U, std::set<TargetClass> S);
    static int substd::set(std::set<TargetClass> std::set1, std::set<TargetClass> std::set2);
    static std::string print_std::set(std::set<TargetClass> S, std::string name="", std::string prefix="");
     */
};

#endif /* SetOperation_hpp */