#ifndef CCHYBRID_PROTOCOL_HPP
#define CCHYBRID_PROTOCOL_HPP

#include "sm_protocol.hpp"
#include "set_operations.hpp"

class CCHybridObject : public RWObject {
public:
    std::set<smNode> seen_;
    int views_;
    int propagated_tg_;

    CCHybridObject() : RWObject (), views_(0), propagated_tg_(0) {}
    CCHybridObject(int oID, object_t T, const std::string &meta="./.meta") : RWObject (oID, T, meta), views_(0), propagated_tg_(0){}
    CCHybridObject(const std::string &oID, object_t T, const std::string &meta="./.meta") : RWObject (oID, T, meta), views_(0), propagated_tg_(0){}
};

class CCHybridPacket : public Packet{
  public:
    CCHybridObject obj;

    CCHybridPacket(){}

    virtual void serialize(std::ostream& stream)
    {
        // Serialization code
        Packet::serializeHead(stream);

        //stream << obj.get_pvalue() << " " << views_ << " " << tg_propagated_;
        stream  << ", \"oID\" : \"" << obj.objID_ << "\"";
        stream << ", \"oType\" : " << obj.get_type();
        stream << ", \"Ts\" : " << obj.tg_.ts << ", \"Wid\" : " << obj.tg_.wid;
        stream << ", \"Value\" : \"" << obj.get_value() <<"\"";
        stream << ", \"pValue\" : \"" << obj.get_pvalue() << "\"";
        stream << ", \"Views\" : " << obj.views_;
        stream << ", \"propFlag\" : " << obj.propagated_tg_ << "}";
    }

    virtual void deserialize(std::istream& stream)
    {
        std::string pval;

        // Deserialization code
        Packet::deserialize(stream);
        stream >> pval >> obj.views_ >> obj.propagated_tg_;

        obj.set_pvalue(pval);
    }

    virtual void serialize(std::ostringstream& stream)
    {
        // Serialization code
        Packet::serialize(stream);
        //stream <<  obj.get_pvalue() << " " << views_ << " " << tg_propagated_;

        stream << ", \"Views\" : " << obj.views_;
        stream << ", \"propFlag\" : " << obj.propagated_tg_;
    }

    virtual void deserialize(std::istringstream& stream)
    {
        std::string pval;

        // Deserialization code
        Packet::deserialize(stream);
        stream >> pval >> obj.views_ >> obj.propagated_tg_;

        obj.set_pvalue(pval);
    }

    virtual void deserialize(std::string jsonstr)
    {
        rapidjson::Document jsonDoc;

        // call parent's code
        Packet::deserializeHead(jsonstr);

        //parse JSON
        jsonDoc.Parse(jsonstr.c_str());

        obj.objID_ = jsonDoc["oID"].GetString();
        obj.set_type( (object_t) jsonDoc["oType"].GetInt());
        obj.tg_.ts = jsonDoc["Ts"].GetInt();
        obj.tg_.wid = jsonDoc["Wid"].GetInt();
        obj.set_value(jsonDoc["Value"].GetString());
        obj.set_pvalue(jsonDoc["pValue"].GetString());
        obj.views_ = jsonDoc["Views"].GetInt();
        obj.propagated_tg_ = jsonDoc["propFlag"].GetInt();
    }
};

#endif // CCHYBRID_PROTOCOL_HPP
