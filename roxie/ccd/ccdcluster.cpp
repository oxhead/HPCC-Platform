/* 
 * File:   ccdcluster.cpp
 * Author: Chin-Jung Hsu
 *
 * Created on January 25, 2016, 5:53 PM
 */

#include "jlog.hpp"
#include "jptree.hpp"

#include "ccdcluster.hpp"

class RoxieClusterManager : public CInterface, implements IRoxieClusterManager
{
    
private:
    StringBuffer masterNodeHost;
    bool isMaster = false;
    MapStringTo<int> *serverIdRecord;
        

public:
    IMPLEMENT_IINTERFACE;
    
    RoxieClusterManager()
    {
        serverIdRecord = new MapStringTo<int>(true);
    }
    
    void addNode(const char *nodeAddress)
    {
        DBGLOG("[Roxie][Cluster] add node -> host=%s", nodeAddress);
        unsigned clusterSize = this->serverIdRecord->ordinality();
        DBGLOG("[Roxie][Cluster] current cluster size = %u", clusterSize);
        this->serverIdRecord->setValue(nodeAddress, clusterSize);
    }
    
    void printTopology()
    {
    }
    
    
    void enableMaster()
    {
        DBGLOG("[Roxie][Cluster] enable master");
        this->isMaster = true;
    }
    
    void disableMaster()
    {
        DBGLOG("[Roxie][Cluster] disable master");
        this->isMaster = false;
    }
    
    bool isMasterNode()
    {
        return this->isMaster;
    }
    
    void setMaster(const char *host)
    {
        DBGLOG("[Roxie][Cluster] set master -> %s", host);
        this->masterNodeHost = new StringBuffer(host);
    }
    
    const StringBuffer &getMaster()
    {
        return this->masterNodeHost;
    }
    
    void start()
    {
        DBGLOG("[Roxie][Cluster] service started");
    }
    
    void stop()
    {
        DBGLOG("[Roxie][Cluster] service stopped");
    }
};

IRoxieClusterManager *createRoxieClusterManager()
{
    return new RoxieClusterManager();
}

Owned<IRoxieClusterManager> roxieClusterManager;
