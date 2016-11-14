#ifndef CCHYBRID_PROTOCOL_HPP
#define CCHYBRID_PROTOCOL_HPP

#include "sm_protocol.hpp"

class CCHybridPacket : public Packet{
  public:
    int views_;
    int tg_propagated_;

    CCHybridPacket(){};

    virtual void serialize(std::ostream& stream)
    {
        // Serialization code
        Packet::serialize(stream);
        stream << " " << views_ << " " << tg_propagated_;
    }

    virtual void deserialize(std::istream& stream)
    {
        // Deserialization code
        Packet::deserialize(stream);
        stream >> views_ >> tg_propagated_;
    }

    virtual void serialize(std::ostringstream& stream)
    {
        // Serialization code
        Packet::serialize(stream);
        stream << " " << views_ << " " << tg_propagated_;
    }

    virtual void deserialize(std::istringstream& stream)
    {
        // Deserialization code
        Packet::deserialize(stream);
        stream >> views_ >> tg_propagated_;
    }
};

class CCHybridReplica : public RWObject {
public:
    std::set<smNode> seen_;
    int propagated_tg_;

    CCHybridReplica() : RWObject () {}
    CCHybridReplica(int oID, object_t T, const std::string &meta="./.meta") : RWObject (oID, T, meta){}
    CCHybridReplica(const std::string &oID, object_t T, const std::string &meta="./.meta") : RWObject (oID, T, meta){}
};

#endif // CCHYBRID_PROTOCOL_HPP
