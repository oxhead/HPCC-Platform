/* 
 * File:   ccdcluster.cpp
 * Author: Chin-Jung Hsu
 *
 * Created on January 25, 2016, 5:53 PM
 */

#include "jlog.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"

#include "ccd.hpp"
#include "ccdcontext.hpp"
#include "ccdcluster.hpp"

class RoxieChannel : public IRoxieChannel
{
private:
	unsigned index = -1; // maybe a problem for negative?
	unsigned level = -1;
public:
	RoxieChannel(unsigned index, unsigned level = 0) : index(index), level(level)
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
    char* address;
    unsigned short port;
	IpAddress ipAddress;

public:

    RoxieNode(unsigned nodeIndex, const char* address)
    {
        RoxieNode(nodeIndex, address, 9876);
    }
    RoxieNode(unsigned nodeIndex, const char* address, unsigned short port)
    {
        this->nodeIndex = nodeIndex;
		this->address = new char[256]; //big enough?
        memcpy(this->address, address, strlen(address) + 1);
		this->ipAddress.ipset(this->address);
        this->port = port;
    }

	virtual void setNodeIndex(unsigned nodeIndex)
	{
		this->nodeIndex = nodeIndex;
	}

    virtual unsigned getNodeIndex()
    {
        return this->nodeIndex;
    }

    virtual const char *getAddress()
    {
		return this->address;
    }

	virtual const IpAddress &getIpAddress()
	{
		return this->ipAddress;
	}

};

class LocalMasterServiceProxy : public IRoxieMasterService
{
private:
	IRoxieMaster *master;
	IRoxieNode *selfNode;

public:
	LocalMasterServiceProxy(IRoxieMaster *master, IRoxieNode *selfNode) : master(master), selfNode(selfNode)
	{
	}

	virtual void joinRoxieCluster()
	{
		DBGLOG("[Roxie][LocalMasterServiceProxy] joinRoxieClsuter");
		IRoxieNode *registeredNode = this->master->joinCluster(this->selfNode->getAddress());
	}
	virtual IRoxieNodeList &retrieveNodeList(IRoxieNodeList &ret)
	{
		DBGLOG("[Roxie][LocalMasterServiceProxy] retrieveNodeList");
		const IRoxieNodeList &nodeList = this->master->getNodeList();
		ret = nodeList;
		return ret;
	}
	virtual IRoxieChannelList &retrieveChannelList(IRoxieChannelList &ret)
	{
		DBGLOG("[Roxie][LocalMasterServiceProxy] retrieveChannelList");
		const IRoxieChannelList &channelList = this->master->getChannelList();
		ret = channelList;
		return ret;
	}

};

class RemoteMasterServiceProxy : public IRoxieMasterService
{
private:
	IRoxieCluster *cluster;
public:
	RemoteMasterServiceProxy(IRoxieCluster *cluster) : cluster(cluster)
	{
	}

	virtual void joinRoxieCluster()
	{
		DBGLOG("[Roxie][RemoteMasterServiceProxy] joinRoxieClsuter");
		IPropertyTree *jsonTree = createPTree();
		IPropertyTree *paramTree = createPTree();
		DBGLOG("\t@ host=%s", this->cluster->getSelf()->getAddress());
		paramTree->setProp("./host", this->cluster->getSelf()->getAddress());
		jsonTree->setProp("./action", "join");
		jsonTree->setPropTree("./params", paramTree);

		DBGLOG("finished creating json tree");
		StringBuffer content;
		toJSON(jsonTree, content, 0, JSON_SortTags);
		DBGLOG("content=%s", content.str());

		MyStringBuffer cmdRequest;
		convertToHTTPRequest(jsonTree, cmdRequest);

		DBGLOG("\t@ master=%s", this->cluster->getMaster()->getAddress());
		SocketEndpoint ep(this->cluster->getMaster()->getAddress(), 9876);

		Owned<ISocket> socket;
		socket.setown(ISocket::connect_timeout(ep, 60000));		
		DBGLOG("socket connection is ready");
		MyString requestStr = cmdRequest.str();
		socket->write(requestStr.data(), requestStr.size());

		char buff[8192];
		unsigned len;
		unsigned bytesRead;
		socket->read(buff, sizeof(len), sizeof(len), bytesRead, maxBlockSize);
		DBGLOG("response=%s", buff);
		unsigned nodeIndex = atoi(buff);
		DBGLOG("\tregisted node id -> %u", nodeIndex);
		IRoxieNode *registeredNode = this->cluster->createNode(nodeIndex, this->cluster->getSelf()->getAddress());
		this->cluster->addNode(registeredNode);

		socket->shutdown();
		socket->close();
		socket.clear();
	}

	virtual IRoxieNodeList &retrieveNodeList(IRoxieNodeList &ret)
	{
		DBGLOG("[Roxie][RemoteMasterServiceProxy] retrieveNodeList");
		throw MakeStringException(ROXIE_INTERNAL_ERROR, "Retrieve node list operation is not yet implemented");
	}
	virtual IRoxieChannelList &retrieveChannelList(IRoxieChannelList &ret)
	{
		DBGLOG("[Roxie][RemoteMasterServiceProxy] retrieveCList");

		IPropertyTree *jsonTree = createPTree();
		IPropertyTree *paramTree = createPTree();
		DBGLOG("\t@ host=%s", this->cluster->getSelf()->getAddress());
		paramTree->setProp("./host", this->cluster->getSelf()->getAddress());
		jsonTree->setProp("./action", "retrieveChannelList");
		jsonTree->setPropTree("./params", paramTree);

		MyStringBuffer cmdRequest;
		convertToHTTPRequest(jsonTree, cmdRequest);

		DBGLOG("\t@ master=%s", this->cluster->getMaster()->getAddress());
		SocketEndpoint ep(this->cluster->getMaster()->getAddress(), 9876);

		Owned<ISocket> socket;
		socket.setown(ISocket::connect_timeout(ep, 60000));
		DBGLOG("socket connection is ready");
		MyString requestStr = cmdRequest.str();
		socket->write(requestStr.data(), requestStr.size());

		char buff[8192];
		unsigned len;
		unsigned bytesRead;
		socket->read(buff, 4, 8192, bytesRead, maxBlockSize);
		DBGLOG("\tbytesRead=%u", bytesRead);
		DBGLOG("\tresponse=%s", buff);

		IPropertyTree *responseJson = createPTreeFromJSONString(buff, ipt_caseInsensitive, (PTreeReaderOptions)(defaultXmlReadFlags | ptr_ignoreNameSpaces));
		parseFromJSON(responseJson, ret);

		socket->shutdown();
		socket->close();
		socket.clear();
		return ret;
	}
};

class RoxieClusterManager : public IRoxieClusterManager, public Thread
{
private:
	IRoxieMasterService *masterService = nullptr;

	// node related
	IRoxieNode *selfNode;
	IRoxieNode *masterNode;
	IRoxieNodeList nodeList;
	IRoxieNodeMap nodeMap; // for host to IRoxieNode lookup, and each node maintins the same copy

	// the variables related to the channels for multicast groups
	IRoxieChannelList channelList;
	IRoxieChannelMap channelMap;
	ChannelToNodeListMap channelToNodeListMap;
	
    // thread related variables
    bool running;

public:
    RoxieClusterManager(const char *masterHost) : Thread("RoxieClusterManager"), running(false)
    {
		DBGLOG("[Roxie][ClusterManager] initializing...");
		//this->nodeList.ensure(1024); // initialize the list?
		//this->channelList.ensure(128);

        // the self node
		IpAddress selfIpAddress = queryHostIP(); // call jsocket API
		StringBuffer ipString;
		selfIpAddress.getIpText(ipString);
		this->selfNode = this->createNode(0, ipString.str());

		// this master node
		this->masterNode = this->createNode(0, masterHost);
    }

	// implement IRoxieCluster
	virtual IRoxieNode *createNode(unsigned nodeIndex, const char *nodeAddress)
	{
		DBGLOG("[Roxie][Cluster] createNode");
		DBGLOG("\tnodeIndex=%u, host=%s", nodeIndex, nodeAddress);
		IRoxieNode *node = this->lookupNode(nodeAddress);
		if (!node)
		{
			DBGLOG("\tinitiate a Roxie node -> index=%u, host=%s", nodeIndex, nodeAddress);
			node = new RoxieNode(nodeIndex, nodeAddress, 9876); // fixed port for now
			this->nodeMap.setValue(node->getAddress(), node);
		}
		return node;
	}

	virtual void addNode(IRoxieNode *node)
	{
		DBGLOG("[Roxie][Cluster] addNode");
		DBGLOG("\tbefore) nodeList=%d", this->nodeList.size());
		this->nodeList.push_back(node);
		DBGLOG("\tafter) nodeList=%d", this->nodeList.size());
	}

	virtual void removeNode(IRoxieNode *node)
	{
		DBGLOG("[Roxie][Cluster] removeNode");
		// TODO is this safe?
		// this->nodeList.element(node->getNodeIndex()) = NULL;
	}

	virtual const IRoxieNodeList &getNodeList()
	{
		DBGLOG("[Roxie][Cluster] getNodeList");
		return this->nodeList;
	}

	virtual IRoxieNode *getNode(unsigned nodeId)
	{
		DBGLOG("[Roxie][Cluster] getNode");
		// need to check the index range
		DBGLOG("\tnodeIndex=%u, total=%u", nodeId, this->nodeList.size());
		return this->nodeList[nodeId];
	}

	virtual IRoxieNode *lookupNode(const char *nodeAddress)
	{
		DBGLOG("[Roxie][Cluster] lookupNode");
		DBGLOG("\thost=%s", nodeAddress);
		IRoxieNode **node = this->nodeMap.getValue(nodeAddress);
		if (node)
		{
			return *node;
		}
		else
		{
			return NULL;
		}
	}

	virtual IRoxieNode *getMaster()
	{
		DBGLOG("[Roxie][Cluster] getMaster");
		return this->masterNode;
	}

	virtual IRoxieNode *getSelf()
	{
		DBGLOG("[Roxie][Cluster] getSelf");
		return this->selfNode;
	}

	virtual bool isMasterNode()
	{
		DBGLOG("[Roxie][Cluster] isMasterNode");
		if (this->getMaster())
		{
			return this->getSelf()->equals(*this->getMaster());
		}
		else
		{
			return false;
		}
	}

	virtual unsigned getClusterSize()
	{
		DBGLOG("[Roxie][Cluster] getClusterSize");
		// TODO needs to count only active nodes in the future
		return this->nodeList.size();
	}

	virtual IRoxieChannel *createChannel(unsigned channelIndex)
	{
		DBGLOG("[Roxie][Cluster] createChannel");
		DBGLOG("\tchannelIndex=%u", channelIndex);
		IRoxieChannel *channel = lookupChannel(channelIndex);
		if (!channel)
		{
			channel = new RoxieChannel(channelIndex);
			this->channelMap.setValue(channelIndex, channel);
		}
		return channel;
	}

	virtual IRoxieChannel *addChannel(IRoxieChannel *channel)
	{
		DBGLOG("[Roxie][Cluster] addChannel");
		DBGLOG("\tchannelIndex=%u", channel->getChannelIndex());
		this->channelList.push_back(channel);

		for (int i = 0; i < this->channelList.size(); i++)
		{
			IRoxieChannel *channel = this->channelList[i];
			DBGLOG("\t# channelPtr=%p", channel);
			DBGLOG("\t(1) channel=%u", channel->getChannelIndex());
		}
		joinMulticastChannel(channel->getChannelIndex());

		return channel;
	}

	virtual IRoxieChannel *lookupChannel(unsigned channelIndex)
	{
		DBGLOG("[Roxie][Cluster] lookupChannel");
		DBGLOG("\tchannelIndex=%u", channelIndex);
		IRoxieChannel **channel = this->channelMap.getValue(channelIndex);
		if (channel)
		{
			return *channel;
		}
		else
		{
			return NULL;
		}
	}

	virtual const IRoxieChannelList &getChannelList()
	{
		DBGLOG("[Roxie][Cluster] getChannelList");
		DBGLOG("\t* channel size=%u", this->channelList.size());
		for (int i = 0; i < this->channelList.size(); i++)
		{
			IRoxieChannel *channel = this->channelList[i];
			DBGLOG("\t# channelPtr=%p", channel);
			DBGLOG("\t(2) channel=%u", channel->getChannelIndex());
		}
		// only works for the master node now
		// need to get this information for the master node

		return this->channelList;
		/*
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
		*/
	}

	virtual unsigned getNumOfChannels()
	{
		DBGLOG("[Roxie][Cluster] getNumOfChannels");
		return this->channelList.size() - 1; // the channel 0 is for local used only?
	}

	virtual void updateChannels(IRoxieChannelList &updateChannelList)
	{
		DBGLOG("[Roxie][Cluster] updateChannels");
		// TODO need a lock here
		this->nodeList.clear();
		for (IRoxieChannelPtr channelPtr : updateChannelList)
		{
			this->addChannel(channelPtr);
		}
	}

	virtual void setJoinMulticastGroupFunc(void(*func)() = 0)
	{
	}

	virtual void setLeaveMulticastGroupFunc(void(*func)() = 0)
	{
	}

	// implement IRoxieMaster
	virtual void enableMasterService()
	{
		DBGLOG("[Roxie][Master] enableMasterService");
		this->masterNode = this->selfNode;

		// the following code works only for the first initialization
		// initialize the channel mapping table
		for (unsigned i = 0; i <= 2; i++)
		{
			IRoxieChannel *channel = this->createChannel(i);
			this->addChannel(channel);
			// safe for memory leak?
			IRoxieNodeList *roxieNodeList = new IRoxieNodeList{};
			roxieNodeList->push_back(this->getSelf());
			this->channelToNodeListMap.setValue(channel->getChannelIndex(), roxieNodeList);
		}
	}

	virtual  void disableMasterService()
	{
		DBGLOG("[Roxie][Master] disableMasterService");
		// TODO is this safe? need to do master election?
		this->masterNode = NULL;
		this->configueMasterService();
	}

	virtual IRoxieNode *joinCluster(const char *host)
	{
		DBGLOG("[Roxie][Master] joinCluster");
		DBGLOG("\thost=%s", host);
		IRoxieNode *node = this->lookupNode(host);
		if (!node)
		{
			unsigned newNodeId = this->generateUniqueNodeId();
			node = this->createNode(newNodeId, host);
		}
		this->addNode(node);
		return node;
	}

	virtual IRoxieNode *leaveCluster(const char *host)
	{
		DBGLOG("[Roxie][Master] leaveCluster");
		DBGLOG("\thost=%s", host);
		throw MakeStringException(ROXIE_INTERNAL_ERROR, "Leave operation is not yet implemented");
	}

	virtual unsigned generateUniqueNodeId()
	{
		DBGLOG("[Roxie][Master] generateUniqueNodeId");
		return this->nodeMap.count();
	}

	// implement IRoxieClusterManager
	virtual void configueMasterService()
	{
		DBGLOG("[Roxie][ClusterManager] configueMasterService");
		if (!this->getMaster())
		{	this->electMaster();
		}

		if (this->isMasterNode())
		{
			DBGLOG("\tusing LocalMasterServiceProxy");
			this->masterService = new LocalMasterServiceProxy(this, this->getSelf());
		}
		else
		{
			DBGLOG("\tusing RemoteMasterServiceProxy");
			this->masterService = new RemoteMasterServiceProxy(this);
		}
	}

	virtual void electMaster()
	{
		DBGLOG("[Roxie][ClusterManager] electMaster");
		throw MakeStringException(ROXIE_INTERNAL_ERROR, "Master election is not yet implemented");
	}

	virtual void syncChannels()
	{
		DBGLOG("[Roxie][ClusterManager] syncChannels");
		IRoxieChannelList updatedChannelList;;
		this->masterService->retrieveChannelList(updatedChannelList);
	    this->updateChannels(updatedChannelList);
	}

	virtual void init()
	{
		DBGLOG("[Roxie][ClusterManager] init");
		this->configueMasterService();
		if (this->isMasterNode())
		{
			this->enableMasterService();
		}
		this->masterService->joinRoxieCluster();
		if (!this->isMasterNode())
		{
			this->syncChannels();
		}
	}

	virtual void start()
    {
        DBGLOG("[Roxie][ClusterManager] start");
        this->running = true;
        this->run();
    }
    
	virtual void stop()
    {
        DBGLOG("[Roxie][ClusterManager] stop");
        this->running = false;
    }

	virtual int run()
    {
		DBGLOG("[Roxie][ClusterManager] run");
        while (running)
        {
            this->running = false; // for test purpose
        }
        return 0;
    }
};


class RoxieMasterProxy : public IRoxieMasterProxy
{
private:
    IRoxieMaster *roxieMaster;

public:
    RoxieMasterProxy(IRoxieMaster *roxieMaster)
    {
        this->roxieMaster = roxieMaster;
    }

	virtual void handleRequest(IPropertyTree *request, ISocket *client)
    {
        DBGLOG("[Roxie][MasterProxy] handleRequest");
        const char *action = request->queryProp("./action");
        DBGLOG("\taction=%s", action);

        MapStringTo<StringBuffer> *params = new MapStringTo<StringBuffer>();
        Owned<IPropertyTreeIterator> paramsIter = request->getElements("./params/*");
        ForEach(*paramsIter)
        {
            StringBuffer paramKey;
            StringBuffer paramValue;
            paramKey.append("./params/").append(paramsIter->query().queryName());
            paramValue.append(request->queryProp(paramKey.str()));
            params->setValue(paramsIter->query().queryName(), paramValue);
            DBGLOG("\txpath=%s", paramKey.str());
            DBGLOG("\tparam -> name=%s, value=%s", paramsIter->query().queryName(), params->getValue(paramsIter->query().queryName())->str());
        }

        // dispatch command
        if (strnicmp(action, "echo", 4) == 0)
        {
            this->echo(client, request);
        }
        else if (strnicmp(action, "join", 4) == 0)
        {
            const char *keyHost = "host";
            const char *host = params->getValue(keyHost)->str();
            this->join(client, host);
        }
        else if (strnicmp(action, "leave", 5) == 0)
        {
			const char *keyHost = "host";
			const char *host = params->getValue(keyHost)->str();
            this->leave(client, host);
        }
        else if (strnicmp(action, "list", 4) == 0)
        {
            this->list(client);
        }
		else if (strnicmp(action, "retrieveChannelList", 19) == 0)
		{
			const char *keyHost = "host";
			const char *host = params->getValue(keyHost)->str();
			this->retrieveChannelList(client, host);
		}
        else
        {
            client->write("UNIMPLEMENTED", 13);
        }
        delete params;
    }

private:
    void echo(ISocket *client, IPropertyTree *request)
    {
		DBGLOG("[Roxie][MasterProxy] echo");
        //client->write("ECHOECHO", 8); // only echo will not get flushed
		StringBuffer sb;
		toXML(request, sb);
		sb = sb.trim();
		DBGLOG("[Roxie][Proxy][echo] length=%u", sb.length());
		DBGLOG("[Roxie][Proxy][echo] content=%s", sb.str());
		client->write(sb.str(), strlen(sb.str()) + 1);
    }

    void join(ISocket *client, const char *host)
    {
		DBGLOG("[Roxie][MasterProxy] join");
		IRoxieNode *node = this->roxieMaster->joinCluster(host);
		DBGLOG("\t@host=%s -> index=%u", host, node->getNodeIndex());
		StringBuffer sb;
		sb.appendf("%u___", node->getNodeIndex()); // appending str because str is not flushed.
		DBGLOG("\tresponse=%s, len=%u", sb.str(), sb.length());
        client->write(sb.str(), sb.length());
		//client->write("UNIMPLEMENTED", 13);
    }

    void leave(ISocket *client, const char *host)
    {
		DBGLOG("[Roxie][MasterProxy] leave");
		// TODO needs to polish
		IRoxieNode *node = this->roxieMaster->leaveCluster(host);
        client->write("LEAVE", 5);
    }

	// TODO two remote calls will fail here
    void list(ISocket *client)
    {
		DBGLOG("[Roxie][MasterProxy] list");
		const IRoxieNodeList &nodeList = this->roxieMaster->getNodeList();
		for (IRoxieNode *node : nodeList)
		{
			const char*nodeAddress = node->getAddress();
			// TODO is this correct?
			client->write(nodeAddress, strlen(nodeAddress) + 1);
		}
    }

	void retrieveChannelList(ISocket *client, const char *host)
	{
		DBGLOG("[Roxie][MasterProxy] retrieveChannelList");
		// TODO return channel list per host?
		const IRoxieChannelList &channelList = this->roxieMaster->getChannelList();
		for (int i = 0; i < channelList.size(); i++)
		{
			IRoxieChannel *channel = channelList[i];
			DBGLOG("\t@channel=%d", channel->getChannelIndex());
		}
		IPropertyTree *jsonTree = createJSONTree(channelList);
		StringBuffer response;
		convertToJSON(jsonTree, response);
		const char *responseStr = response.str();
		DBGLOG("\tresponse=%s", responseStr);
		DBGLOG("\tresponse length=%d", strlen(responseStr) + 1);
		unsigned bytesWrite = client->write(responseStr, strlen(responseStr) + 1);
		DBGLOG("\tbytesWrite=%u", bytesWrite);
	}
};

MyStringBuffer &convertToHTTPRequest(IPropertyTree *jsonTree, MyStringBuffer &ret)
{
	DBGLOG("[Roxie][util] convertToHTTPRequest");
	StringBuffer content;
	toJSON(jsonTree, content, 0, JSON_SortTags);
	DBGLOG("content=%s", content.str());

	ret << "CMD /proxy HTTP/1.1\n";
	ret << "Content-Length: ";
	ret << content.length();
	ret << "\r\n\r\n";
	ret << content.str();
	DBGLOG("req=%s", ret.str().data());

	return ret;
}


IPropertyTree  *createJSONTree(const IRoxieChannelList &channelList)
{
	DBGLOG("[Roxie][util] createJSONTree -> IRoxieChannelList");
	IPropertyTree *jsonTree = createPTree();
	jsonTree->setProp("./class", "IRoxieChannelList");

	for (int idx = 0; idx < channelList.size(); idx++)
	{
		IRoxieChannel *channel = channelList[idx];
		DBGLOG("\t@channel=%u", channel->getChannelIndex());

		//jsonTree->addPropInt("./channel", channel->getChannelIndex());
		IPropertyTree *objTree = createPTree("channel");
		objTree->setPropInt("@channel", channel->getChannelIndex());
		jsonTree->addPropTree("object", objTree);
	}

	return jsonTree;
}

IPropertyTree *createJSONTree(StringBuffer &jsonStr)
{
	DBGLOG("[Roxie][util] createJSONTree -> StringBuffer");
	IPropertyTree *responseJson = createPTreeFromJSONString(jsonStr.str(), ipt_caseInsensitive, (PTreeReaderOptions)(defaultXmlReadFlags | ptr_ignoreNameSpaces));
	return responseJson;
}

StringBuffer &convertToJSON(IPropertyTree *jsonTree, StringBuffer &content)
{
	DBGLOG("[Roxie][util] convertToJSON");
	toJSON(jsonTree, content, 0, JSON_SortTags);
	return content;
}

IRoxieChannelList &parseFromJSON(IPropertyTree *jsonTree, IRoxieChannelList &channelList)
{
	DBGLOG("[Roxie][util] parseFromJSON");
	Owned<IPropertyTreeIterator> paramsIter = jsonTree->getElements("./object");
	DBGLOG("\tcount=%u", jsonTree->getCount("./object"));
	ForEach(*paramsIter)
	{
		IPropertyTree &object = paramsIter->query();
		unsigned channelIndex = object.getPropInt("@channel");
		DBGLOG("\t@ object=%u", channelIndex);
		IRoxieChannel *channel = roxieClusterManager->createChannel(channelIndex);
		channelList.push_back(channel);
		DBGLOG("\t@ channel=%u", channel->getChannelIndex());
	}
	return channelList;
}


IRoxieClusterManager *roxieClusterManager;

extern IRoxieClusterManager *createRoxieClusterManager(const char *masterHost)
{
	return new RoxieClusterManager(masterHost);
};

IRoxieMasterProxy *roxieMasterProxy;

extern IRoxieMasterProxy *createRoxieMasterProxy(IRoxieClusterManager *roxieClusterManager)
{
    return new RoxieMasterProxy(roxieClusterManager);
};