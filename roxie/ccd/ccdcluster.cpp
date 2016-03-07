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

class Channel : public IChannel
{
private:
	unsigned index = -1; // maybe a problem for negative?
	unsigned level = -1;
public:
	Channel(unsigned index, unsigned level = 0) : index(index), level(level)
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

	//virtual StringBuffer &getAddress(StringBuffer &sb)
	//{
	//	return this->ipAddress.getIpText(sb);
	//	//return sb;
	//}

    virtual const char *getAddress()
    {
		return this->address;
		//StringBuffer ipText;
		//this->ipAddress.getIpText(ipText);
		//return ipText.str();
    }

	virtual const IpAddress &getIpAddress()
	{
		return this->ipAddress;
	}

};

class RoxieClusterManager : public IRoxieClusterManager, public Thread
{
private:
    bool masterServiceEnabled = false;

	// the variables related to member nodes in a roxie clustler
	// [0, 1, 2, 3, 4, 5, ......, n]
	// 0 = self
	// masterIndex = the index of the master node
	// selfIndex = the index of itself
	// note: the masterIndex might change overtime in the future
	unsigned masterIndex;
	unsigned selfIndex;
	IRoxieNodeList nodeList;
	IRoxieNodeMap nodeMap; // for host to IRoxieNode lookup, and each node maintins the same copy
	IRoxieNode *masterNode;

	// the variables related to the channels for multicast groups
	IChannelList channelList;
	ChannelToNodeListMap channelToNodeListMap;
	
    // thread related variables
    bool running;

public:
    
    RoxieClusterManager() : Thread("RoxieClusterManager"), selfIndex(0), masterIndex(0), running(false)
    {
		DBGLOG("[Roxie][Manager] initializing...");
		this->nodeList.ensure(1024); // initialize the list?
		this->channelList.ensure(128);
        // the self node
		IpAddress ip = queryHostIP(); // call jsocket API
		StringBuffer ipString;
		ip.getIpText(ipString);
		this->nodeList.add(new RoxieNode(0, ipString.str(), 9876), 0);
		DBGLOG("\tself=%s", this->nodeList[0]->getAddress());
		//this->nodeMap.setValue(ipString.str(), this->nodeList[0]);
    }
    
	virtual const char *_getHost()
	{
		IpAddress ip = queryHostIP(); // call jsocket API
		StringBuffer *ipString = new StringBuffer;
		ip.getIpText(*ipString);
		const char *host = ipString->str();
		//delete ipString;
		return host;
	}
    virtual IRoxieNode &addNode(const char *nodeAddress)
    {
        DBGLOG("[Roxie][Cluster] add node -> host=%s", nodeAddress);
        DBGLOG("[Roxie][Cluster] current cluster size = %u", this->getClusterSize());

		// need to test whether this API work?
        if (!this->nodeMap.getValue(nodeAddress))
        {
			DBGLOG("\tnode does not exist, so adding to the record");
			unsigned newNodeId = this->getClusterSize() + 1; // index starts from one, is it safe?
            IRoxieNodePtr roxieNodePtr = new RoxieNode(newNodeId, nodeAddress, 9876); // fixed port for now
			this->nodeList.add(roxieNodePtr, newNodeId);
            this->nodeMap.setValue(nodeAddress, roxieNodePtr);
            unsigned nodeIndex = addRoxieNode(nodeAddress);
			DBGLOG("\tnodeList=%u", this->nodeList.length());
			DBGLOG("\tclusterNodes=%u", this->nodeMap.count());
			DBGLOG("\troxieNodes=%u", nodeIndex);
        }
        return **(this->nodeMap.getValue(nodeAddress));
    }

	virtual  void removeNode(const char *nodeAddress)
    {
    }

	virtual  unsigned getNumOfNodes()
    {
		return this->nodeMap.count();
		//return this->nodeList.length() - 1; // the first element is itself, duplicated counting
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

		// the following code works only for the first initialization
		// initialize the channel mapping table	
		for (unsigned i = 0; i <= 2; i++)
		{
			IChannelPtr channelPtr = new Channel {i}; // requires using new?
			DBGLOG("\tchannelPtr -> p=%p", channelPtr);
			//DBGLOG("[Roxie][enableMasterService] channelGroup -> index=%u, level=%u", channelGroupPtr->getChannelIndex(), channelGroupPtr->getChannelLevel());
			IRoxieNodeListPtr roxieNodeListPtr = new IRoxieNodeList();
			this->channelList.append(channelPtr);
			this->channelToNodeListMap.setValue(channelPtr->getChannelIndex(), roxieNodeListPtr);
        }

		IRoxieNode &selfNode = this->addNode(this->_getHost());
		this->masterIndex = selfNode.getNodeIndex();
		this->selfIndex = selfNode.getNodeIndex();
		this->nodeList[0] = &(selfNode);
        this->masterServiceEnabled = true;
		DBGLOG("\tmasterIndex=%u, selfIndex=%u", this->masterIndex, this->selfIndex);
		DBGLOG("\treal selfNode=%u", this->getSelf()->getNodeIndex());
        //this->_load_testing_data();
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
		//return this->selfIndex == this->masterIndex;
		DBGLOG("[Cluster] isMasterNode");
		DBGLOG("\tmaster=%s", this->getMaster()->getAddress());
		IpAddress ip(this->getMaster()->getAddress());
		DBGLOG("\tmaster=%s, isMaster=%s", this->getMaster()->getAddress(), ip.isLocal()?"True":"False");
		return ip.isLocal();
    }

	virtual void setMaster(const char *host)
    {
        DBGLOG("[Roxie][Cluster] set master -> %s", host);
        this->masterNode = new RoxieNode(1, host, 9876); // needs to change if the master node has changed
    }
    
	virtual IRoxieNode *getMaster()
    {
        return this->masterNode;
    }
    
	virtual bool hasMaster()
    {
        return this->masterNode != NULL;
    }

	virtual IRoxieNode *getSelf()
    {
		return this->nodeList[0];
    }

	virtual IRoxieNode *getNode(unsigned nodeIndex)
	{
		// need to check the index range
		DBGLOG("[Roxie][API] getnode");
		DBGLOG("\tnodeIndex=%u, total=%u", nodeIndex, this->nodeList.length());
		return this->nodeList[nodeIndex];
	}


	virtual const IRoxieNodeMap &getNodes()
	{
		return this->nodeMap;
	}

	virtual unsigned getClusterSize()
	{
		return this->getNumOfNodes();
	}


	virtual unsigned getNumOfChannels()
	{
		return this->channelList.length() - 1; // the channel 0 is for local used only?
	}

	virtual const IChannelList &getChannelList()
	{
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

	virtual void joinRoxieCluster()
    {
		DBGLOG("[Roxie][API] joinRoxieCluster");

		IPropertyTree *jsonTree = createPTree();
		IPropertyTree *paramTree = createPTree();
		DBGLOG("\t@ host=%s", this->getSelf()->getAddress());
		paramTree->setProp("./host", this->getSelf()->getAddress());
		jsonTree->setProp("./action", "join");
		jsonTree->setPropTree("./params", paramTree);

		DBGLOG("finished creating json tree");
		StringBuffer content;
		toJSON(jsonTree, content, 0, JSON_SortTags);
		DBGLOG("content=%s", content.str());

		StringBuffer cmdRequest;
		cmdRequest.append("CMD /proxy HTTP/1.1\n");
		cmdRequest.appendf("Content-Length: %u", content.length());
		cmdRequest.append("\n");
		cmdRequest.append("\r\n\r\n");
		cmdRequest.append(content.str());
		DBGLOG("req=%s", cmdRequest.str());

		DBGLOG("\t@ master=%s", this->getMaster()->getAddress());
		SocketEndpoint ep(this->getMaster()->getAddress(), 9876);
		Owned<ISocket> socket;
		socket.setown(ISocket::connect(ep));
		CSafeSocket sc(socket);
		DBGLOG("socket connection is ready");
		sc.write(cmdRequest.str(), cmdRequest.length());
		char buff[8192];
		unsigned len;
		unsigned bytesRead;
		socket->read(buff, sizeof(len), sizeof(len), bytesRead, maxBlockSize);
		DBGLOG("response=%s", buff);
		unsigned nodeIndex = atoi(buff);
		DBGLOG("\tregisted node id -> %u", nodeIndex);
		this->getSelf()->setNodeIndex(nodeIndex);
        
		// do http request to the master server
        // while (true)
        //    sleep(1000); //dangerous
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

	virtual void handleRequest(IPropertyTree *request, ISocket *client)
    {
        DBGLOG("[Roxie][Proxy] recieved a request");
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
    void echo(ISocket *client, IPropertyTree *request)
    {
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
		IRoxieNode &node = this->clusterManager->addNode(host);
		DBGLOG("\t@host=%s -> index=%u", host, node.getNodeIndex());
		StringBuffer sb;
		sb.appendf("%u___", node.getNodeIndex()); // appending str because str is not flushed.
		DBGLOG("\tresponse=%s, len=%u", sb.str(), sb.length());
        client->write(sb.str(), sb.length());
		//client->write("UNIMPLEMENTED", 13);
    }

    void leave(ISocket *client)
    {
        client->write("LEAVE", 5);
    }

	// TODO two remote calls will fail here
    void list(ISocket *client)
    {
        const MapStringTo<IRoxieNodePtr> nodes = this->clusterManager->getNodes();
        HashIterator nodeIterator(nodes);
        ForEach(nodeIterator)
        {
            IRoxieNode *node = *(nodes.mapToValue(&nodeIterator.query()));
            const char*nodeAddress = node->getAddress();
            client->write(nodeAddress, strlen(nodeAddress)); // is this correct
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