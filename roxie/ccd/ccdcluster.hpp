/* 
 * File:   ccdcluster.h
 * Author: Chin-Jung Hsu
 *
 * Created on January 25, 2016, 5:45 PM
 */

#ifndef _CCDCLUSTER_INCL
#define _CCDCLUSTER_INCL

#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "jsocket.hpp"
//#include "roxiehelper.hpp"
//#include "ccd.hpp"
//#include "ccdcontext.hpp"

class IRoxieNode
{
public:
	virtual void setNodeIndex(unsigned nodeIndex) = 0;
    virtual unsigned getNodeIndex() = 0;
	virtual std::string getAddress() = 0;
	virtual IpAddress &getIpAddress(IpAddress &ipAddress) = 0;
};

class RoxieNode : public IRoxieNode
{
private:
	unsigned nodeIndex = -1;
	std::string address;
	unsigned short port;

public:

	RoxieNode(unsigned nodeIndex, const char* address)
	{
		RoxieNode(nodeIndex, address, 9876);
	}

	RoxieNode(unsigned nodeIndex, const char* addressStr, unsigned short port)
		: nodeIndex(nodeIndex)
		, address(addressStr)
		, port(port)
	{
	}

	virtual void setNodeIndex(unsigned nodeIndex)
	{
		this->nodeIndex = nodeIndex;
	}

	virtual unsigned getNodeIndex()
	{
		return this->nodeIndex;
	}

	virtual std::string getAddress()
	{
		return this->address;
	}

	virtual IpAddress &getIpAddress(IpAddress &ipAddress)
	{
		ipAddress.ipset(this->address.c_str());
		return ipAddress;
	}

	bool operator==(const RoxieNode &other) const
	{
		return this->address == other.address;
	}
	bool operator!=(const RoxieNode &other) const
	{
		return this->address != other.address;
	}
	bool operator<(const RoxieNode &other) const
	{
		return this->address < other.address;
	}

};

class IRoxieChannel
{
public:
	virtual unsigned getChannelIndex() = 0;
	virtual unsigned getChannelLevel() = 0;
	virtual void suspend() = 0;
	virtual bool isSuspended() = 0;
	virtual bool isLocal() = 0;
	virtual bool isPrimChannel() = 0; // the rignt name?
	virtual void addNode(RoxieNode node) = 0;
	virtual void removeNode(RoxieNode node) = 0;
	virtual bool containsNode(RoxieNode node) = 0;
	virtual unsigned getNumOfParticipantNodes() = 0;
	virtual const RoxieNodeSet &getParticipantNodes() = 0;
};

class RoxieChannel : public IRoxieChannel
{
private:
	unsigned index = -1; // maybe a problem for negative?
	unsigned level = -1;
	bool suspended;
	RoxieNodeSet nodes;
public:
	RoxieChannel(unsigned _index, unsigned _level = 0)
		: index(_index)
		, level(_level)
		, suspended(false)
	{
	}
	unsigned getChannelIndex()
	{
		return this->index;
	}
	unsigned getChannelLevel()
	{
		return this->level;
	}
	virtual void suspend()
	{
		this->suspended = true;
	}
	virtual bool isSuspended()
	{
		return this->suspended;
	}
	bool isLocal()
	{
		return this->index == 0;
	}
	bool isPrimChannel()
	{
		return this->getNumOfParticipantNodes() == 1;
	}
	virtual void addNode(RoxieNode node)
	{
		this->nodes.insert(node);
	}
	virtual void removeNode(RoxieNode node)
	{
		this->nodes.erase(node);
	}
	virtual unsigned getNumOfParticipantNodes()
	{
		return this->nodes.size();
	}
	virtual const RoxieNodeSet &getParticipantNodes()
	{
		return this->nodes;
	}
};

typedef std::vector<RoxieNode> RoxieNodeList;
typedef std::set<RoxieNode> RoxieNodeSet;
typedef std::map<unsigned, RoxieNode> RoxieNodeMap;

typedef std::vector<RoxieChannel> RoxieChannelList;
typedef std::set<RoxieChannel> RoxieChannelSet;
typedef std::map<unsigned, RoxieChannel> RoxieChannelMap;

// the services provided for slave nodes
class IRoxieMasterService
{
public:
	virtual void joinRoxieCluster() = 0; // blocking call
	virtual RoxieNodeList retrieveNodeList() = 0;
	virtual RoxieChannelList retrieveChannelList() = 0;
};

// the Roxie mater, mainly for internal usage
class IRoxieMaster
{
public:
	virtual void enableMasterService() = 0;
	virtual void disableMasterService() = 0;
	virtual unsigned generateUniqueNodeId() = 0;
	virtual void joinCluster(const char *host) = 0; // need to make sure the id is unique across all nodes
	virtual void leaveCluster(const char *host) = 0;
	virtual RoxieNodeSet getNodes() = 0;
	virtual RoxieChannelSet getChannels() = 0;

};

// the comman API for both slave and the master
class IRoxieCluster
{
public:
	// 1) node related
	// add the node to the node table
	virtual const RoxieNode &addNode(const char *address) = 0;
    // remove the node from the node table
	// TOOD do we need to remove a node or maintain an active node list?
	virtual void removeNode(RoxieNode node) = 0;
	virtual void removeNode(unsigned index) = 0;
    // get the list of all nodes
	virtual RoxieNodeSet &getNodes() = 0;
	// get node by node index
	virtual const RoxieNode &getNode(unsigned nodeIndex) = 0;
	// lookup node by host
	virtual const RoxieNode &lookupNode(const char *host) = 0;
	virtual bool containNode(const char *host) = 0;
	// the pointer to the master node
	virtual RoxieNode &getMaster() = 0;
	// the pointer to the self node
	virtual RoxieNode &getSelf() = 0;
    // check itself a master node
	virtual bool isMasterNode() = 0;
	// the cluster size
	// TODO should include only active nodes
	virtual unsigned getClusterSize() = 0;

	// channel related
	virtual RoxieChannel &addChannel(unsigned channelIndex) = 0;
	virtual void updateChannel(unsigned channelIndex, RoxieNodeSet nodes) = 0;
	virtual RoxieChannel &getChannel(unsigned channelIndex) = 0;
	virtual RoxieChannel &getLocalChannel() = 0;
	virtual RoxieChannel &getSnifferChannel() = 0;
	virtual RoxieChannelSet getChannels() = 0;
	virtual unsigned getNumOfChannels() = 0;
};

class IRoxieClusterManager : public virtual IRoxieCluster, public virtual IRoxieMaster
{
public:
	using IRoxieCluster::getNodes;
	using IRoxieCluster::getChannels;

	// master service related
	virtual void configueMasterService() = 0;
	virtual void electMaster() = 0;

	// channel related
	virtual void syncChannels() = 0;

    // thread control
	virtual void init() = 0;
	virtual void start() = 0;
	virtual void stop() = 0;
};

class IRoxieMasterProxy
{
public:
	virtual void handleRequest(IPropertyTree *request, ISocket *client) = 0;
};

MyStringBuffer &convertToHTTPRequest(IPropertyTree *jsonTree, MyStringBuffer &ret);
IPropertyTree *createJSONTree(const IRoxieChannelList &channelList);
IPropertyTree *createJSONTree(StringBuffer &jsonStr);
StringBuffer &convertToJSON(IPropertyTree *jsonTree, StringBuffer &ret);
IRoxieChannelList &parseFromJSON(IPropertyTree *jsonTree, IRoxieChannelList &channelList);

IRoxieClusterManager *createRoxieClusterManager(const char *masterHost, bool localSlave=false);
extern IRoxieClusterManager *roxieClusterManager;

IRoxieMasterProxy *createRoxieMasterProxy(IRoxieClusterManager *roxieClusterManager);
extern IRoxieMasterProxy *roxieMasterProxy;

#endif