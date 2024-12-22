#ifndef _COORDINATOR_HH_
#define _COORDINATOR_HH_

#include "../inc/include.hh"
#include "../ec/ECBase.hh"
#include "../ec/Clay.hh"
#include "../ec/BUTTERFLY.hh"
#include "../ec/RDP.hh"
#include "../ec/RSCONV.hh"
#include "../ec/HHXORPlus.hh"
#include "../ec/RSPIPE.hh"
#include "../sol/SolutionBase.hh"
#include "../sol/CentSolution.hh"
#include "../sol/OfflineSolution.hh"
#include "../sol/BalanceSolution.hh"
#include "../sol/RepairBatch.hh"
#include "Config.hh"
#include "StripeStore.hh"

using namespace std;

class Coordinator {

    private:
        Config* _conf;
        StripeStore* _ss;

        string _codename;
        int _ecn;
        int _eck;
        int _ecw;

        int _blkbytes;
        int _pktbytes;

        string _method;
        string _scenario;
        int _failnodeid;

        vector<RepairBatch*> _repair_batch_list;
        ECBase* _ec;
        SolutionBase* _sol;

        

    public:
        Coordinator(Config* conf, StripeStore* ss);
        ~Coordinator();

        bool initRepair(string method, string scenario, int failnodeid);
        vector<RepairBatch*> genRepairSolution();
        int genRepairSolutionAsync();
        void repair();

        unordered_map<int, int> _fail2repair;
};

#endif
