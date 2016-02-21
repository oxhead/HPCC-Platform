/* 
 * File:   ccdcluster.h
 * Author: Chin-Jung Hsu
 *
 * Created on January 25, 2016, 5:45 PM
 */

#ifndef _CCDCLUSTER_INCL
#define _CCDCLUSTER_INCL

#include "ccd.hpp"
#include "ccdcontext.hpp"

class IRoxieNode
{
public:
    virtual unsigned getNodeIndex() const = 0;
    virtual const char *getAddress() = 0;
};

class IChannelGroup
{
public:
	virtual unsigned getChannelIndex() = 0;
	virtual unsigned getChannelLevel() = 0;
};

typedef IRoxieNode *IRoxieNodePtr; //required to be here before IRoxieClusterManager
typedef IChannelGroup *IChannelGroupPtr;
typedef MapStringTo<IRoxieNodePtr> IRoxieNodeMap; //map host to the node object
typedef ArrayOf<IRoxieNodePtr> IRoxieNodeList;
typedef ArrayOf<IChannelGroupPtr> IChannelGroupList;
typedef IRoxieNodeList *IRoxieNodeListPtr;
typedef IChannelGroupList *IChannelGroupListPtr;
typedef MapBetween<unsigned, unsigned, IChannelGroupPtr, IChannelGroupPtr> ChannelMap;
typedef MapBetween<unsigned, unsigned, IRoxieNodeListPtr, IRoxieNodeListPtr> ChannelToNodeListMap; //map channel id to node list

class IRoxieClusterManager
{
    
public:
	virtual unsigned getNumNodes() = 0;
	virtual void printTopology() = 0;
    
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
	virtual IRoxieNode &getSelf() = 0;
	virtual IRoxieNodeMap getNodes() = 0;
	virtual IChannelGroupList getChannelGroups() = 0;
	virtual unsigned getClusterSize() = 0;

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