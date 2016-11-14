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

/*
 * Defining the packet header for the semifast implementation.
 */

#ifndef protocol_h
#define protocol_h
#include "sm_node.h"
#include <vector>
#include "rapidjson/document.h"

enum object_t
{
    FILE_T,
    VALUE_T
};

// timestamp structure
class Tag {
  public:
    int ts;
    int wid;
    int wc;

    Tag(int writerID=0){ts=0; wid=writerID; wc=0;};

    // overloaded > operator
    bool operator > (const Tag& tg1) const;

    // overloaded == operator
    bool operator == (const Tag& tg1) const;

    // overloaded >= operator
    bool operator >= (const Tag& tg1) const;

    // overloaded < operator
    bool operator < (const Tag& tg1);
};

// shared object structure
class RWObject{
  public:
    std::string objID_;
    Tag tg_;                    // tag associated with object

    RWObject();
    RWObject(int oID, object_t, const std::string &meta="./.meta");
    RWObject(const std::string &oID, object_t, const std::string &meta="./.meta");

    std::string get_id();

    std::string get_path();
    void set_path(const std::string &path);

    std::string get_value();
    void set_value(const std::string &val);
    std::string get_pvalue();
    void set_pvalue(const std::string &val);

    Tag get_tag();
    void set_tag(Tag t);
    Tag load_latest_tag();
    bool set_latest_tag(const Tag &tg);

    bool load_metadata();
    bool save_metadata();

    object_t get_type(){return objType_;}
    void set_type(object_t t){objType_ = t;}

    bool operator == (const RWObject& obj1) const;
    bool operator < (const RWObject& obj1) const;
    bool operator < (const RWObject* obj1) const;

  protected:
    object_t objType_;        // this can be file or value
    std::string file_path_;
    std::string meta_dir_;
    std::string value_;         // value associated with tag
    std::string p_value_;       // value associated with previous tag
};


class Packet : public Serializable{
  public:
    int src_;
    int dst_;

    int msgType;        //Type of the message
    int counter;        //Used to determine if this is a new packet

    RWObject obj;       // Object id with value and/or filepath

    Packet(){};

    bool operator < (const Packet& pkt1) const
    {
      return ( this->src_ < pkt1.src_);
    }
    
    virtual void serialize(std::ostream& stream)
    {
        Tag tag = obj.get_tag();

        // Serialization code
        stream << src_ <<" "<< dst_ << " " << msgType << " "  << counter << " ";
        stream  << obj.objID_ << " " << obj.get_type() << " " << tag.ts << " " << tag.wid << " " << obj.get_value();
    }
    
    virtual void deserialize(std::istream& stream)
    {
        int oType;
        std::string val;
        Tag tag;

        // Deserialization code
        stream >> src_ >> dst_ >> msgType >> counter >> obj.objID_ >> oType >> tag.ts >> tag.wid >> val;

        obj.set_tag(tag);
        obj.set_value(val);
        obj.set_type((object_t) oType);
    }
};

#endif /* protocol_h */
