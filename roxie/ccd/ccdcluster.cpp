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
	virtual RoxieNodeSet retrieveNodes()
	{
		DBGLOG("[Roxie][LocalMasterServiceProxy] retrieveNodeList");
		return RoxieNodeSet(this->master->getNodes());
	}
	virtual RoxieChannelSet retrieveChannels()
	{
		DBGLOG("[Roxie][LocalMasterServiceProxy] retrieveChannelList");
		return RoxieChannelSet(this->master->getChannels());
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

	virtual RoxieNodeSet retrieveNodes()
	{
		DBGLOG("[Roxie][RemoteMasterServiceProxy] retrieveNodeList");
		throw MakeStringException(ROXIE_INTERNAL_ERROR, "Retrieve node list operation is not yet implemented");
	}
	virtual RoxieChannelSet retrieveChannels()
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
		RoxieChannelSet channels;
		parseFromJSON(responseJson, channels);

		socket->shutdown();
		socket->close();
		socket.clear();
		return channels;
	}
};

class RoxieClusterManager : public IRoxieClusterManager, public Thread
{
public:
	static const unsigned CHANNEL_LOCAL = 0;
	static const unsigned CHANNEL_SNIFFER = 32; // is this bigger enough?
private:
	IRoxieMasterService *masterService = nullptr;

	// node related
	bool localSlave; // from the original Roxie design
	RoxieNode selfNode;
	RoxieNode masterNode;
	RoxieNodeMap nodeMap; // for host to IRoxieNode lookup, and each node maintins the same copy

	// the variables related to the channels for multicast groups
	RoxieChannelMap channelMap;
	
    // thread related variables
    bool running;

public:
    RoxieClusterManager(const char *masterHost, bool _localSlave)
		: Thread("RoxieClusterManager")
		, running(false)
		, localSlave(_localSlave)
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

	virtual const RoxieNode &addNode(const char *address)
	{
		DBGLOG("[Roxie][Cluster] addNode");
		if (this->containNode(address))
		{
			return this->lookupNode(address);
		}
		else
		{
			unsigned newNodeId = this->nodeMap.size();
			RoxieNode newNode{ newNodeId, address };
			this->nodeMap.insert({ newNodeId, RoxieNode{newNodeId, address} });
			return this->getNode(newNodeId);
		}
	}

	virtual void removeNode(RoxieNode node)
	{
		DBGLOG("[Roxie][Cluster] removeNode");
		this->removeNode(node.getNodeIndex());
	}

	virtual void removeNode(unsigned nodeIndex)
	{
		DBGLOG("[Roxie][Cluster] removeNode");
		this->nodeMap.erase(nodeIndex);
	}

	virtual const IRoxieNodeList &getNodeList(IRoxieNodeList &nodeList)
	{
		DBGLOG("[Roxie][Cluster] getNodeList");
		for (auto values : this->nodeMap)
		{
			nodeList.push_back(values.first);
		}
		return nodeList;
	}

	virtual IRoxieNode *getNode(unsigned nodeId)
	{
		DBGLOG("[Roxie][Cluster] getNode");
		// need to check the index range
		DBGLOG("\tnodeIndex=%u, total=%lu", nodeId, this->nodeList.size());
		return this->nodeList[nodeId];
	}

	virtual IRoxieNode *lookupNode(const char *nodeAddress)
	{
		DBGLOG("[Roxie][Cluster] lookupNode");
		DBGLOG("\thost=%s", nodeAddress);
		return this->nodeMap[nodeAddress];
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
			this->channelMap[channelIndex] = channel;
		}
		return channel;
	}

	virtual IRoxieChannel *updateChannel(IRoxieChannel *newChannel)
	{
		// TODO: is thie function safe?
		// TODO: change to implementation with set?
		DBGLOG("[Roxie][Cluster] updateChannel");
		DBGLOG("\tchannelIndex=%u", newChannel->getChannelIndex());

		IRoxieChannel *oldChannel = this->getChannel(newChannel->getChannelIndex());
		const IRoxieNodeSet &oldNodes = oldChannel->getParticipantNodes();





		if (!oldChannel)
		{
			channelMap[channel->getChannelIndex()] = new IRoxieNodeList;
		}

		if (oldChannel)
		{
		}
		else
		{
			channelMap[channel->getChannelIndex()] = 

		}

		
		this->channelList.push_back(channel);

		addEndpoint(channel->getChannelIndex(), getSelf()->getIpAddress(), ccdMulticastPort);
		joinMulticastChannel(channel->getChannelIndex());

		_updateChannelConfig(channel);

		return channel;
	}

	virtual IRoxieChannel *lookupChannel(unsigned channelIndex)
	{
		DBGLOG("[Roxie][Cluster] lookupChannel");
		DBGLOG("\tchannelIndex=%u", channelIndex);
		return this->channelMap[channelIndex];
	}

	virtual IRoxieChannel *getChannel(unsigned channelIndex)
	{
		IRoxieChannel *channel = lookupChannel(channelIndex);
		if (!channel)
			throw MakeStringException(ROXIE_INTERNAL_ERROR, "Invalid channel index %u", channelIndex);
		return channel;
	}

	virtual IRoxieChannel *getLocalChannel()
	{
		// TODO: needs to optimize to avoid lookup cost
		return this->getChannel(CHANNEL_LOCAL);
	}

	virtual IRoxieChannel *getSnifferChannel()
	{
		return this->getChannel(CHANNEL_SNIFFER);
	}

	virtual const IRoxieChannelList &getChannelList()
	{
		DBGLOG("[Roxie][Cluster] getChannelList");
		DBGLOG("\t* channel size=%lu", this->channelList.size());
		for (int i = 0; i < this->channelList.size(); i++)
		{
			IRoxieChannel *channel = this->channelList[i];
			DBGLOG("\t# channelPtr=%p", channel);
			DBGLOG("\t(2) channel=%u", channel->getChannelIndex());
		}
		// only works for the master node now
		// need to get this information for the master node

		return this->channelList;
	
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
			this->updateChannel(channelPtr);
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
		for (unsigned i = 0; i <= 1; i++)
		{
			IRoxieChannel *channel = this->createChannel(i);
			this->updateChannel(channel);
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
		return this->nodeMap.size();
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

		// TODO: enable multicast
		this->_openMulticastSocket();
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

		// TODO: stop multicast
		this->multicastSocket.clear();
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

private:
	void _updateChannelConfig(IRoxieChannel *channel)
	{
		StringBuffer xpath;
		xpath.appendf("RoxieSlaveProcess[@channel=\"%d\"]", channel);
		IPropertyTree *ci = createPTree("RoxieSlaveProcess");
		ci->setPropInt("@channel", channel);
		ci->setPropInt("@subChannel", channel->getNumOfParticipantNodes());
		if (ccdChannels->hasProp(xpath.str()))
		{
			ccdChannels->setPropTree("RoxieSlaveProcess", ci);
		}
		else
		{
			ccdChannels->addPropTree("RoxieSlaveProcess", ci);
		}
		saveChannel();
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
		DBGLOG("\tresponse length=%lu", strlen(responseStr) + 1);
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

extern IRoxieClusterManager *createRoxieClusterManager(const char *masterHost, , bool localSlave = false)
{
	return new RoxieClusterManager(masterHost, localSlave);
};

IRoxieMasterProxy *roxieMasterProxy;

extern IRoxieMasterProxy *createRoxieMasterProxy(IRoxieClusterManager *roxieClusterManager)
{
    return new RoxieMasterProxy(roxieClusterManager);
};