/* 
 * File:   ccdcluster.h
 * Author: Chin-Jung Hsu
 *
 * Created on January 25, 2016, 5:45 PM
 */

#ifndef _CCDCLUSTER_INCL
#define _CCDCLUSTER_INCL

#include "ccd.hpp"

interface IRoxieClusterManager : extends IInterface
{
    virtual void addNode(const char *nodeAddress) = 0;
    virtual void printTopology() = 0;
    
    virtual void enableMaster() = 0;
    virtual void disableMaster() = 0;
    virtual bool isMasterNode() = 0;
    virtual void setMaster(const char *host) = 0;
    virtual const StringBuffer &getMaster() = 0;
    
    virtual void start() = 0;
    virtual void stop() = 0;
};


IRoxieClusterManager *createRoxieClusterManager();
extern Owned<IRoxieClusterManager> roxieClusterManager;

#endif

