/* 
 * File:   ccdcluster.h
 * Author: Chin-Jung Hsu
 *
 * Created on January 25, 2016, 5:45 PM
 */

#ifndef _CCDCLUSTER_INCL
#define _CCDCLUSTER_INCL

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
	bool equals(IRoxieNode &other)
	{
		return strcmp(this->getAddress(), other.getAddress()) == 0;
	}

	virtual void setNodeIndex(unsigned nodeIndex) = 0;
    virtual unsigned getNodeIndex() = 0;
    //virtual StringBuffer &getAddress(StringBuffer &sb) = 0;
	virtual const char *getAddress() = 0;
	virtual const IpAddress &getIpAddress() = 0;
};

class IRoxieChannel
{
public:
	virtual unsigned getChannelIndex() = 0;
	virtual unsigned getChannelLevel() = 0;
};

#define MAX_ROXIE_ARRAY_SIZE 1204
typedef std::string MyString;
typedef std::ostringstream MyStringBuffer;
typedef IRoxieNode *IRoxieNodePtr; //required to be here before IRoxieClusterManager
typedef IRoxieChannel *IRoxieChannelPtr;
typedef MapStringTo<IRoxieNodePtr> IRoxieNodeMap; //map host to the node object
//typedef ArrayOf<IRoxieNodePtr> IRoxieNodeList;
//typedef ArrayOf<IRoxieChannelPtr> IRoxieChannelList;
typedef std::vector<IRoxieNodePtr> IRoxieNodeList;
typedef std::vector<IRoxieChannelPtr> IRoxieChannelList;
typedef IRoxieNodeList *IRoxieNodeListPtr;
typedef IRoxieChannelList *IRoxieChannelListPtr;
typedef MapBetween<unsigned, unsigned, IRoxieChannelPtr, IRoxieChannelPtr> IRoxieChannelMap; // channel index to channel gourp
typedef MapBetween<unsigned, unsigned, IRoxieNodeListPtr, IRoxieNodeListPtr> ChannelToNodeListMap; //map channel id to node list

// the services provided for slave nodes
class IRoxieMasterService
{
public:
	virtual void joinRoxieCluster() = 0; // blocking call
	virtual IRoxieNodeList &retrieveNodeList(IRoxieNodeList &ret) = 0;
	virtual IRoxieChannelList &retrieveChannelList(IRoxieChannelList &ret) = 0;
};

// the Roxie mater, mainly for internal usage
class IRoxieMaster
{
public:
	virtual void enableMasterService() = 0;
	virtual void disableMasterService() = 0;
	virtual unsigned generateUniqueNodeId() = 0;
	virtual IRoxieNode *joinCluster(const char *host) = 0; // need to make sure the id is unique across all nodes
	virtual IRoxieNode *leaveCluster(const char *host) = 0;
	virtual const IRoxieNodeList &getNodeList() = 0;
	virtual const IRoxieChannelList &getChannelList() = 0;

};

// the comman API for both slave and the master
class IRoxieCluster
{
public:
	// 1) node related
	// central place to initialize a node object to avoid memory leak
    virtual IRoxieNode *createNode(unsigned nodeIndex, const char *nodeAddress) = 0;
	// add the node to the node table
	virtual void addNode(IRoxieNode *node) = 0;
    // remove the node from the node table
	// TOOD do we need to remove a node or maintain an active node list?
	virtual void removeNode(IRoxieNode *node) = 0;
    // get the list of all nodes
	virtual const IRoxieNodeList &getNodeList() = 0;
	// get node by node index
	virtual IRoxieNode *getNode(unsigned nodeIndex) = 0;
	// lookup node by host
	virtual IRoxieNode *lookupNode(const char *host) = 0;
	// the pointer to the master node
	virtual IRoxieNode *getMaster() = 0;
	// the pointer to the self node
	virtual IRoxieNode *getSelf() = 0;
    // check itself a master node
	virtual bool isMasterNode() = 0;
	// the cluster size
	// TODO should include only active nodes
	virtual unsigned getClusterSize() = 0;

	// channel related
	virtual IRoxieChannel *createChannel(unsigned channelIndex) = 0;
	virtual IRoxieChannel *addChannel(IRoxieChannel *channel) = 0;
	virtual IRoxieChannel *lookupChannel(unsigned channelIndex) = 0;
	virtual const IRoxieChannelList &getChannelList() = 0;
	virtual unsigned getNumOfChannels() = 0;
	virtual void updateChannels(IRoxieChannelList &channelList) = 0;
	virtual void setJoinMulticastGroupFunc(void(*func)() = 0) = 0;
	virtual void setLeaveMulticastGroupFunc(void(*func)() = 0) = 0;
};

class IRoxieClusterManager : public virtual IRoxieCluster, public virtual IRoxieMaster
{
public:
	using IRoxieCluster::getNodeList;
	using IRoxieCluster::getChannelList;

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

IRoxieClusterManager *createRoxieClusterManager(const char *masterHost);
extern IRoxieClusterManager *roxieClusterManager;

IRoxieMasterProxy *createRoxieMasterProxy(IRoxieClusterManager *roxieClusterManager);
extern IRoxieMasterProxy *roxieMasterProxy;

#endif