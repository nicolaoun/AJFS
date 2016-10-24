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
#include "set_operations.hpp"

template < class TargetClass >
std::set<TargetClass> SetOperation<TargetClass>::unite(const std::set<TargetClass> &set1,
                                          const std::set<TargetClass> &set2){
	std::set<TargetClass> outSet;
	typename std::set<TargetClass>::iterator it;

	for(it=set1.begin(); it!=set1.end(); it++){
		//SMNodeAgent::DEBUGING(1, "Adding element %d in union\n", *it);
		outSet.insert(*it);
	}

	for(it=set2.begin(); it!=set2.end(); it++){
		//SMNodeAgent::DEBUGING(1, "Adding element %d in union\n", *it);
		outSet.insert(*it);
	}

	return outSet;
}

// Goal: Return intersection between two std::sets
// Output: Set intersection
//
template < class TargetClass >
std::set<TargetClass> SetOperation<TargetClass>::intersect(const std::set<TargetClass> &set1,
                                              const std::set<TargetClass> &set2){
	std::set<TargetClass> outSet;
	typename std::set<TargetClass>::iterator it;

	for(it=set1.begin(); it!=set1.end(); it++){
		//DEBUGING(1, "Checking element: %d\n", *it);

		if(set2.find(*it) != set2.end()){
			//DEBUGING(1, "Element %d exists in std::set2!\n", *it);

			outSet.insert(*it);
		}
	}

	return outSet;
}

// Goal: Find the complement of a std::set
// Input: The universe of elements and the
// std::set of which we want to ge the complement
// Output: the complement
template < class TargetClass >
std::set<TargetClass> SetOperation<TargetClass>::setcomplement(const std::set<TargetClass> &U,
                                                  const std::set<TargetClass> &S){
	typename std::set<TargetClass>::iterator it;
	std::set<TargetClass> tmp=U;

	//remove all the elements of S from U
	for (it=S.begin(); it!=S.end(); it++) {
		//SMNodeAgent::DEBUGING(2, "Removing Elm=%d\n", *it);

		tmp.erase(*it);
	}

	return tmp;
}

// Goal: Check if the std::set given as first parameter is
// a substd::set of the second std::set.
// Output: 1 if first std::set is a substd::set of a second, 0 otherwise
//
template < class TargetClass >
int SetOperation<TargetClass>::subset(const std::set<TargetClass> &set1,
                         const std::set<TargetClass> &set2){
	typename std::set<TargetClass>::iterator it;

	// check if all the elements of std::set1 belong in std::set2
	for(it=set1.begin(); it!=set1.end(); it++){
		//DEBUGING(1, "Checking element: %d\n", *it);

		if(set2.find(*it) == set2.end()){
			//DEBUGING(1, "Element %d not found!\n", *it);
			return 0;
		}
	}

	return 1;
}

//Goal: print the elements in a std::set
//Input: (a) The std::set we want to print
//       (b) name of the std::set
//       (c) prefix of each element of the std::set
template < class TargetClass >
std::string SetOperation<TargetClass>::print_set(const std::set<TargetClass> &S,
                                    const std::string &name,
                                    const std::string &prefix){
	std::set<int>::iterator it;
	std::stringstream ss;
	std::string outstr;

	outstr= name+"={";
	for(it=S.begin(); it!=S.end(); it++){
		ss << prefix << *it << " ";
	}
	outstr=outstr+ss.str()+"}\n";

	return outstr;

}
/*
		 static std::set<TargetClass> intersect(std::set<TargetClass> std::set1, std::set<TargetClass> std::set2);
		static std::set<TargetClass> std::setcomplement(std::set<TargetClass> U, std::set<TargetClass> S);
		static int substd::set(std::set<TargetClass> std::set1, std::set<TargetClass> std::set2);
		static std::string print_std::set(std::set<TargetClass> S, std::string name="", std::string prefix="");
		 */
