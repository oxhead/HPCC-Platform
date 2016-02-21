/* 
 * File:   ccdcluster.cpp
 * Author: Chin-Jung Hsu
 *
 * Created on January 25, 2016, 5:53 PM
 */

#include "jlog.hpp"
#include "jptree.hpp"

#include "ccd.hpp"
#include "ccdcontext.hpp"
#include "ccdcluster.hpp"

class ChannelGroup : public IChannelGroup
{
private:
	unsigned index = -1; // maybe a problem for negative?
	unsigned level = -1;
public:
	ChannelGroup(unsigned index, unsigned level = 0) : index(index), level(level)
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
};
class RoxieNode: public IRoxieNode
{
    
private:
    unsigned nodeIndex = -1;
    const char* address;
    unsigned short port;

public:

    RoxieNode(unsigned nodeIndex, const char* address)
    {
        RoxieNode(nodeIndex, address, 9876);
    }
    RoxieNode(unsigned nodeIndex, const char* address, unsigned short port)
    {
        this->nodeIndex = nodeIndex;
        this->address = address;
        this->port = port;
    }

    virtual unsigned getNodeIndex() const
    {
        return this->nodeIndex;
    }

    virtual const char *getAddress()
    {
        return this->address;
    }

};

class RoxieClusterManager : public IRoxieClusterManager, public Thread
{
private:
    bool masterServiceEnabled = false;

    IRoxieNode *selfNode;
    IRoxieNode *masterNode;
	IRoxieNodeMap clusterNodes;

    // channel related variables
    //unsigned numSlaves[MAX_CLUSTER_SIZE];
    //unsigned replicationLevel[MAX_CLUSTER_SIZE];
    IPropertyTree* ccdChannels;
	ChannelMap channelMap;
	ChannelToNodeListMap channelToNodeListMap;

    // thread related variables
    bool running;

public:
    
    RoxieClusterManager() : Thread("RoxieClusterManager")
    {
        // the self node
        IpAddress ip = queryHostIP(); // call jsocket API
        StringBuffer ipString;
        ip.getIpText(ipString);
        this->selfNode = new RoxieNode(1, ipString.str(), 9876); // temparary solution

        // cluster related

        // thread related
        this->running = false;
    }
    
    virtual IRoxieNode &addNode(const char *nodeAddress)
    {
        DBGLOG("[Roxie][Cluster] add node -> host=%s", nodeAddress);
        unsigned clusterSize = this->clusterNodes.ordinality();
        DBGLOG("[Roxie][Cluster] current cluster size = %u", clusterSize);

        if (!this->clusterNodes.getValue(nodeAddress))
        {
            unsigned newNodeId = this->clusterNodes.ordinality() + 1;
            IRoxieNodePtr roxieNodePtr = new RoxieNode(newNodeId, nodeAddress, 9876); // fixed port for now
            this->clusterNodes.setValue(nodeAddress, roxieNodePtr);
        }
        return **(this->clusterNodes.getValue(nodeAddress));
    }

	virtual  void removeNode(const char *nodeAddress)
    {
    }

	virtual  unsigned getNumNodes()
    {
        return this->clusterNodes.ordinality();
    }

	virtual  void printTopology()
    {
    }
    
    
    void _load_testing_data()
    {
        const char *hosts[] = {
            "1.1.1.1",
            "2.2.2.2",
            "3.3.3.3",
            "4.4.4.4",
        };

        for (int i=0; i<4; i++)
        {
            this->addNode(hosts[i]);
        }
    }

	virtual void enableMasterService()
    {
        DBGLOG("[Roxie][API] enableMasterService");

		// initialize the channel mapping table
		for (unsigned i = 1; i <= 5; i++)
		{
			IChannelGroupPtr channelGroupPtr = new ChannelGroup {i}; // requires using new?
			DBGLOG("\tchannelGroupPtr -> p=%p", channelGroupPtr);
			//DBGLOG("[Roxie][enableMasterService] channelGroup -> index=%u, level=%u", channelGroupPtr->getChannelIndex(), channelGroupPtr->getChannelLevel());
			IRoxieNodeListPtr roxieNodeListPtr = new IRoxieNodeList();
			this->channelMap.setValue(channelGroupPtr->getChannelIndex(), channelGroupPtr);
			this->channelToNodeListMap.setValue(channelGroupPtr->getChannelIndex(), roxieNodeListPtr);
        }

		// add this master node itself
        IpAddress ip = queryHostIP();
        StringBuffer ipString;
        ip.getIpText(ipString);
        this->addNode(ipString.str());
        this->masterServiceEnabled = true;
        this->_load_testing_data();
    }
    
	virtual  void disableMasterService()
    {
        DBGLOG("[Roxie][Cluster] disable master");
        this->masterServiceEnabled = false;
    }
    
	virtual  bool isMasterServiceReady()
    {
        return this->masterServiceEnabled;
    }
    
	virtual bool isMasterNode()
    {
		IpAddress ip(this->getMaster()->getAddress());
		return ip.isLocal();
    }

	virtual void setMaster(const char *host)
    {
        DBGLOG("[Roxie][Cluster] set master -> %s", host);
        this->masterNode = new RoxieNode(1, host, 9876);
    }
    
	virtual IRoxieNode *getMaster()
    {
        return this->masterNode;
    }
    
	virtual bool hasMaster()
    {
        return this->masterNode != NULL;
    }

	virtual IRoxieNode &getSelf()
    {
        return *(this->selfNode);
    }

	virtual IRoxieNodeMap getNodes()
    {
        return this->clusterNodes;
    }

	virtual unsigned getClusterSize()
	{
		return this->clusterNodes.count();
	}

	virtual IChannelGroupList getChannelGroups()
	{
		// only works for the master node now
		// need to get this information for the master node

		DBGLOG("[Roxie][API] getChannelGroups");
		HashIterator iter(this->channelMap);
		DBGLOG("\t[pre-1]");
		IChannelGroupList channelGroupList;
		DBGLOG("\t[pre-2]");
		ForEach(iter)
		{
			//IChannelGroupPtr *channelGroupPtr = this->channelMap.mapToValue(&iter.query());
			DBGLOG("\t[1]");
			IChannelGroupPtr channelGroupPtr = *(this->channelMap.mapToValue(&iter.query()));
			DBGLOG("\t[2]");
			channelGroupList.append(channelGroupPtr);
			DBGLOG("\t[3]");
			DBGLOG("[4] k=%p, %p", channelGroupPtr, &(*channelGroupPtr));
			//DBGLOG("[Roxie][Cluster] channel -> index=%u, level=%u", k->getChannelIndex(), k->getChannelLevel());
			//DBGLOG("[Roxie][Cluster] channelGroupPtr=%p (%p)", k, &k);
		}
		return channelGroupList;
    }

	virtual void joinRoxieCluster()
    {
        // do http request to the master server
        while (true)
            sleep(1000); //dangerous
    }

	virtual void start()
    {
        DBGLOG("[Roxie][Cluster] service started");
        this->running = true;
        this->run();
    }
    
	virtual void stop()
    {
        DBGLOG("[Roxie][Cluster] service stopped");
        this->running = false;
    }

	virtual int run()
    {
        while (running)
        {
            this->running = false; // for test purpose
        }
        return 0;
    }

private:
    void addSlaveChannel(unsigned channel, unsigned level)
    {
        DBGLOG("[Roxie] addSlaveChannel: channel=%u, level=%u", channel, level);
    }

    void addChannel(unsigned nodeNumber, unsigned channel, unsigned level)
    {
        DBGLOG("[Roxie] addChannel: nodeNumber=%u, channel=%u, level=%u", nodeNumber, channel, level);
    }

    void addEndpoint(unsigned channel, const IpAddress &slaveIp, unsigned port)
    {
    }

    bool isSlaveEndpoint(unsigned channel, const IpAddress &slaveIp)
    {
        return false;
    }
};

class RoxieMasterProxy : public IRoxieMasterProxy
{
private:
    IRoxieClusterManager *clusterManager;

public:
    RoxieMasterProxy(IRoxieClusterManager *clusterManager)
    {
        this->clusterManager = clusterManager;
    }

	virtual void handleRequest(IPropertyTree *request, SafeSocket *client)
    {
        const char *action = request->queryProp("./action");
        DBGLOG("[Roxie][Worker] action=%s", action);

        MapStringTo<StringBuffer> *params = new MapStringTo<StringBuffer>();
        Owned<IPropertyTreeIterator> paramsIter = request->getElements("./params/*");
        ForEach(*paramsIter)
        {
            StringBuffer paramKey;
            StringBuffer paramValue;
            paramKey.append("./params/").append(paramsIter->query().queryName());
            paramValue.append(request->queryProp(paramKey.str()));
            params->setValue(paramsIter->query().queryName(), paramValue);
            DBGLOG("[Roxie][Worker] xpath=%s", paramKey.str());
            DBGLOG("[Roxie][Worker] param -> name=%s, value=%s", paramsIter->query().queryName(), params->getValue(paramsIter->query().queryName())->str());
        }

        // dispatch command
        if (strnicmp(action, "echo", 4) == 0)
        {
            this->echo(client);
        }
        else if (strnicmp(action, "join", 4) == 0)
        {
            const char *keyHost = "host";
            const char *host = params->getValue(keyHost)->str();
            this->join(client, host);
        }
        else if (strnicmp(action, "leave", 5) == 0)
        {
            this->leave(client);
        }
        else if (strnicmp(action, "list", 4) == 0)
        {
            this->list(client);
        }
        else
        {
            client->write("UNIMPLEMENTED", 13);
        }
        delete params;
    }

private:
    void echo(SafeSocket *client)
    {
        client->write("ECHOECHO", 8); // only echo will not get flushed
    }

    void join(SafeSocket *client, const char *host)
    {
        this->clusterManager->addNode(host);
        client->write("JOINED", 6);
    }

    void leave(SafeSocket *client)
    {
        client->write("LEAVE", 5);
    }

    void list(SafeSocket *client)
    {
        const MapStringTo<IRoxieNodePtr> nodes = this->clusterManager->getNodes();
        HashIterator nodeIterator(nodes);
        ForEach(nodeIterator)
        {
            IRoxieNode *node = *(nodes.mapToValue(&nodeIterator.query()));
            const char *nodeAddress = node->getAddress();
            client->write("LEAVE", strlen(nodeAddress)); // is this correct
        }
    }
};


IRoxieClusterManager *roxieClusterManager;

extern IRoxieClusterManager *createRoxieClusterManager()
{
    return new RoxieClusterManager();
};

IRoxieMasterProxy *roxieMasterProxy;

extern IRoxieMasterProxy *createRoxieMasterProxy()
{
    return new RoxieMasterProxy(roxieClusterManager);
};