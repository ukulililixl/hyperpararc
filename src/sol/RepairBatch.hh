#ifndef _REPAIRBATCH_HH_
#define _REPAIRBATCH_HH_

#include "../inc/include.hh"
#include "../ec/ECDAG.hh"
#include "../ec/NodeBatchTask.hh"
#include "../common/Stripe.hh"
#include "../common/Config.hh"
#include "../protocol/AGCommand.hh"
#include "../util/DistUtil.hh"


using namespace std;

class RepairBatch {

    private:
        int _batch_id;
        vector<Stripe*> _stripe_list;

        unordered_map<int, int> _in;
        unordered_map<int, int> _out;
        int _bdwt;
        int _load;

    public:
        // han add
        vector<Stripe*> getStripeList();
        unordered_map<int, int> getInputMap();
        unordered_map<int, int> getOutputMap();
        vector<vector<int>> getLoadTable(int);
        void push(Stripe* stripe);
        void erase(Stripe* stripe);
        Stripe* pop();
        void dumpBitVec(int);
        int getBatchId();
        string dumpLoad(int agent_num);
        void tuning();
        Stripe* maxContribute(int nodeIdx, bool isIn);
        void maxContribute(int nodeIdx, bool isIn, vector<Stripe*>& stripes);
        void maxContribute(int nodeidx, bool isIn, vector<pair<Stripe*, int>>& sortStripes);
        int maxLoadNode();
        vector<int> maxLoadNodes(int maxLoad);
        // end

        RepairBatch(int id, vector<Stripe*> stripe_list);
        ~RepairBatch();
        
        void evaluateBatch(int);
        int getLoad();
        int getBdwt();
        string dump();
        void genRepairTasks(int ecn, int eck, int ecw, Config* conf,
                unordered_map<int, int> fail2repair, unsigned int coorIp);

};

#endif
