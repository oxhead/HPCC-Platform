/* 
 * File:   ccdcluster.h
 * Author: Chin-Jung Hsu
 *
 * Created on January 25, 2016, 5:45 PM
 */

#ifndef _CCDCLUSTER_INCL
#define _CCDCLUSTER_INCL


#include "jsocket.hpp"
#include "roxiehelper.hpp"
//#include "ccd.hpp"
//#include "ccdcontext.hpp"

class IRoxieNode
{
public:
    virtual unsigned getNodeIndex() = 0;
    virtual const char *getAddress() = 0;
	virtual const IpAddress &getIpAddress() = 0;
};

class IChannel
{
public:
	virtual unsigned getChannelIndex() = 0;
	virtual unsigned getChannelLevel() = 0;
};

typedef IRoxieNode *IRoxieNodePtr; //required to be here before IRoxieClusterManager
typedef IChannel *IChannelPtr;
typedef MapStringTo<IRoxieNodePtr> IRoxieNodeMap; //map host to the node object
typedef ArrayOf<IRoxieNodePtr> IRoxieNodeList;
typedef ArrayOf<IChannelPtr> IChannelList;
typedef IRoxieNodeList *IRoxieNodeListPtr;
typedef IChannelList *IChannelListPtr;
//typedef MapBetween<unsigned, unsigned, IChannelPtr, IChannelPtr> ChannelMap; // channel index to channel gourp
typedef MapBetween<unsigned, unsigned, IRoxieNodeListPtr, IRoxieNodeListPtr> ChannelToNodeListMap; //map channel id to node list

class IRoxieClusterManager
{
    
public:
    
    // methods for the master node
	virtual void enableMasterService() = 0;
	virtual void disableMasterService() = 0;
	virtual bool isMasterServiceReady() = 0;
	virtual bool isMasterNode() = 0;
	virtual IRoxieNode &addNode(const char *nodeAddress) = 0;
	virtual void removeNode(const char *nodeAddress) = 0;

    // methods for slave nodes
    // the master node is also a slave node
	virtual void setMaster(const char *host) = 0;
	virtual IRoxieNode *getMaster() = 0;
	virtual bool hasMaster() = 0;
	virtual void joinRoxieCluster() = 0; // blocking call
	virtual IRoxieNode *getSelf() = 0;
	virtual IRoxieNode *getNode(unsigned nodeIndex) = 0;
	virtual const IRoxieNodeMap &getNodes() = 0;
	virtual const IChannelList &getChannelList() = 0;
	virtual unsigned getNumOfChannels() = 0;
	virtual unsigned getClusterSize() = 0;
	virtual unsigned getNumOfNodes() = 0;
	virtual void printTopology() = 0;

    // configuration related
    //virtual void onConfigChange() = 0;

    // thread control
	virtual void start() = 0;
	virtual void stop() = 0;
};

class IRoxieMasterProxy
{
public:
	virtual void handleRequest(IPropertyTree *request, SafeSocket *client) = 0;
};


IRoxieClusterManager *createRoxieClusterManager();
extern IRoxieClusterManager *roxieClusterManager;

IRoxieMasterProxy *createRoxieMasterProxy();
extern IRoxieMasterProxy *roxieMasterProxy;

#endif