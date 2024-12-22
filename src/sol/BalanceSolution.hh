#include <string>
#ifndef _BALANCESOLUTION_HH_
#define _BALANCESOLUTION_HH_

#include "../common/TradeoffPoints.hh"
#include "../common/Globaldata.hh"
#include "../ec/ECBase.hh"
#include "SolutionBase.hh"

#include "RepairBatch.hh"

#define DEBUG_ENABLE true 

enum INITMODEL{
    OFFLINE_INIT = 0,
    PARALLEL_INIT
};

using namespace std;
class BalanceSolution : public SolutionBase {

    private:
        TradeoffPoints* _tp;
        void genRepairBatchesForSingleFailure(int fail_node_id, string scenario, bool enqueue);
        void genRepairBatchesForSingleFailure_new(int fail_node_id, string scenario, bool enqueue,int init, bool isTunning);
    
    public:
        // han add
        bool _debug = false;
        int* _RepairGroup;
        int* _ifselect;
        int* _bipartite_matrix;
        int* _node_belong;
        int* _cur_matching_stripe;
        int _num_stripes_per_group;
        int* _mark;
        int _num_rebuilt_chunks;
        int* _record_stripe_id;
        int _rg_num;
        int _helpers_num;
        int _batch_size;
        int _num_batches;
        
        unordered_map<int, vector<int>> _balance_tp;
        
        

        // xiaolu add
        bool _enqueue;
                
        vector<RepairBatch*> formatReconstructionSets();
        void genColoringForSingleStripe(Stripe* stripe, unordered_map<int, int>& res, int fail_node_id,  string scenario, vector<int> & placement);
        void genBalanceColoringForSingleFailure(Stripe* stripe, int fail_node_id, string scenario, vector<vector<int>> & loadTable);
        void genOfflineColoringForSingleFailure(Stripe* stripe, int fail_node_id, string scenario, vector<vector<int>> & loadTable);
        
        
        void GloballyMLP(Stripe* stripe, const vector<int> & itm_idx, const vector<int> & candidates,
            ECDAG * ecdag, unordered_map<int, int> & coloring, vector<vector<int>> & loadTable);

        void GloballyMLP(Stripe* stripe, const vector<int> & itm_idx, ECDAG * ecdag, unordered_map<int, int> & coloring, vector<vector<int>> & loadTable);
        void GloballyMLP(Stripe* stripe, const vector<int> & itm_idx, unordered_map<int, int> & coloring, vector<vector<int>> & loadTable);
        void SingleMLP(Stripe* stripe, const vector<int> & itm_idx, const vector<int> & candidates,ECDAG * ecdag, unordered_map<int, int> & coloring);
        int chooseColor_single(Stripe* stripe, vector<int> childColors, unordered_map<int, int> coloring, int idx);
        int chooseColor_fullnode(Stripe* stripe,const vector<int> & childColors, const vector<vector<int>> & loadTable, int idx);
        bool isBetter(State st1, State st2);
        bool isBetter(State st1,int color1, State st2, int color2,const vector<vector<int>> & table);

        void initBalanceTP();
        vector<int> getColoringByIdx(int fail_block_idx);
        void tuning(RepairBatch* batch,vector<Stripe*> stripe, int fail_node_id, string scenario);
        // end
        BalanceSolution();
        BalanceSolution(int batchsize, int standbysize, int agentsnum);
        void genRepairBatches(int num_failures, vector<int> fail_node_list, string scenario, bool enqueue);
        void prepare(Stripe* stripe, int fail_node_id, unordered_map<int, int> & res, string scenario,const vector<vector<int>> & loadTable);

        // Table
        State evalTable(const vector<vector<int>> & table);
        State evalTable(vector<vector<int>> table, vector<int> colors);
        string dumpTable(const vector<vector<int>> & table);
};

#endif
