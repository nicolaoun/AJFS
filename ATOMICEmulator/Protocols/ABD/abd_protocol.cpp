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

#include "ABD_protocol.hpp"

bool Tag::operator > (const Tag& tg1) const
{
	//Check if tg1 > tg2
	return ((this->ts > tg1.ts) ||
	   (this->ts == tg1.ts && this->wid > tg1.wid));
}

// overloaded == operator
bool Tag::operator == (const Tag& tg1) const
{
	//Check if tg1 == tg2
	return (this->ts == tg1.ts && this->wid == tg1.wid);
}

// overloaded >= operator
bool Tag::operator >= (const Tag& tg1) const
{
	//Check if tg1 == tg2
	return (*this == tg1 || *this > tg1);
}

/*
bool Tag::operator < (const Tag& tg1)
{
	return (!(*this > tg1) && !(*this == tg1));
}
*/

RWObject::RWObject() :
    meta_dir_("./meta"),
    objID_("")
{}

RWObject::RWObject(int oID, const std::string &meta)
{
	std::stringstream sstm;
	sstm << oID;

	objID_=sstm.str();
    
    meta_dir_ = meta;
    
    
    if(!directoryExists(meta_dir_)){
        createDirectory(meta_dir_);
    }

	//load latest known tag
	load_latest_tag();

	objType_ = "file";
	file_path_ ="./";
	value_ ="";
}

RWObject::RWObject(const std::string &oID, const std::string &meta)
{
	objID_ = oID;
    meta_dir_ = meta;
    
    if(!directoryExists(meta_dir_)){
        createDirectory(meta_dir_);
    }

	//load latest known tag
	load_latest_tag();

	objType_ = "file";
	file_path_ ="./";
	value_ ="";
}

std::string RWObject::get_id(){ return objID_; }

std::string RWObject::get_path(){ return file_path_; }

void RWObject::set_path(const std::string &path){ file_path_ = path; }

std::string RWObject::get_value(){ return value_; }
void RWObject::set_value(const std::string &val){ value_ = val; }

Tag RWObject::get_tag(){ return tg_; }

Tag RWObject::load_latest_tag()
{
	std::ifstream instrm;
	std::ofstream outstrm;
	char objmfile[100];

	sprintf(objmfile, "%s/%s.meta", meta_dir_.c_str(), objID_.c_str());

    if (fileExists(objmfile))
    {
        // open objects meta data to read the objects tag
        instrm.open(objmfile, std::ios::in | std::ios::binary);

        if ( instrm.good() )
        {
            instrm >> tg_.ts;
            instrm >> tg_.wid;
        }
        instrm.close();
    }
	else
	{
		outstrm.open(objmfile, std::ios::out);

		//Initialize the node's local tag
		tg_.ts=0;
		tg_.wid=0;
		tg_.wc=0;

		outstrm << tg_.ts << " " << tg_.wid;
		outstrm.close();
	}

	return tg_;
}

bool RWObject::set_latest_tag(const Tag &tg)
{
	std::ofstream outstrm;
	char objmfile[100];

	tg_ = tg;

	sprintf(objmfile, "%s/%s.meta", meta_dir_.c_str(), objID_.c_str());

	// open objects meta data to write the objects tag
	outstrm.open(objmfile, std::ios::out);

	outstrm << tg_.ts << " " << tg_.wid;

	outstrm.close();

	return true;
}

bool RWObject::operator == (const RWObject& obj1) const
{
	return (this->objID_ == obj1.objID_);
}

bool RWObject::operator < (const RWObject& obj1) const
{
	return (this->objID_ < obj1.objID_);
}

bool RWObject::operator < (const RWObject* obj1) const
{
    return (this->objID_ < obj1->objID_);
}
