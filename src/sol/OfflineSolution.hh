#ifndef _OFFLINESOLUTION_HH_
#define _OFFLINESOLUTION_HH_

#include "../common/TradeoffPoints.hh"
#include "../ec/ECBase.hh"
#include "SolutionBase.hh"
#include "RepairBatch.hh"

using namespace std;

class OfflineSolution : public SolutionBase {

    private:
        int _batch_size;
        int _num_batches;
        TradeoffPoints* _tp;

    public:
        void genRepairBatchesForSingleFailure(int fail_node_id, string scenario, bool enqueue);
        void genOfflineColoringForSingleFailure(Stripe* stripe, unordered_map<int, int>& res, int fail_node_id, string scenario, vector<vector<int>> & loadtable);

        OfflineSolution();
        OfflineSolution(int batchsize, int standbysize, int agentsnum);

        void genRepairBatches(int num_failures, vector<int> fail_node_list, string scenario, bool enqueue);
        void setTradeoffPoints(TradeoffPoints* tp);
};

#endif
