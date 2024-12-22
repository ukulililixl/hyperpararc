#ifndef _PARALLELSOLUTION_HH_
#define _PARALLELSOLUTION_HH_

#include "../common/TradeoffPoints.hh"
#include "../ec/ECBase.hh"
#include "SolutionBase.hh"
#include "RepairBatch.hh"

#define DEBUG_ENABLE true 

using namespace std;

class ParallelSolution : public SolutionBase {

    private:
        TradeoffPoints* _tp;
        void genRepairBatchesForSingleFailure(int fail_node_id, string scenario);
        

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

        // xiaolu add
        bool _enqueue;

        void improve(int failnodeid, string);
        void improve_enqueue(int failnodeid, string);
        void improve_hybrid(int failnodeid, string);
        void improve_hungary(int failnodeid, string);
        
        vector<RepairBatch*> findRepairBatchs(int soon_to_fail_node, string scenario);
        int if_insert(Stripe* repairstripe, int rg_id, int cur_match_stripe, int soon_to_fail_node);
        int hungary(int rg_id, int cur_match_stripe, int* matrix, int matrix_start_addr, int* node_selection);
        int greedy_replacement(int num_related_stripes, int soon_to_fail_node, int rg_id);
        int replace(int src_id, int des_id, int rg_id, int* addi_id, int num_related_stripes, string flag, int soon_to_fail_node);
        void update_bipartite_for_replace(int des_id, int stripe_id, int rg_id, int index_in_rg, string flag, int soon_to_fail_node);        
        vector<RepairBatch*> formatReconstructionSets();
        void genParallelColoringForSingleFailure(Stripe* stripe, int fail_node_id,  string scenario, vector<vector<int>> & loadTable);
        void genOfflineColoringForSingleFailure(Stripe* stripe, int fail_node_id, string scenario, vector<vector<int>> & loadTable, vector<int> & placement);
        void genColoringForSingleFailure(Stripe* stripe, unordered_map<int, int>& res, int fail_node_id, string scenario, vector<int> & placement);
        State evalTable(const vector<vector<int>> & table);
        State evalTable(vector<vector<int>> table, vector<int> colors);
        void GloballyMLP(Stripe* stripe, const vector<int> & itm_idx, const vector<int> & candidates,
            ECDAG * ecdag, unordered_map<int, int> & coloring, vector<vector<int>> & loadTable);
        void SingleMLP(Stripe* stripe, const vector<int> & itm_idx, const vector<int> & candidates,ECDAG * ecdag, unordered_map<int, int> & coloring);
        int chooseColor_single(Stripe* stripe, vector<int> childColors, unordered_map<int, int> coloring, int idx);
        int chooseColor_fullnode(Stripe* stripe,const vector<int> & childColors, const vector<vector<int>> & loadTable, int idx);
        bool isBetter(State st1, State st2);
        bool isBetter(State st1,int color1, State st2, int color2,const vector<vector<int>> & table);

        Stripe* choose(vector<bool> flags, vector<Stripe*> _stripe_list);
        
        // end
        ParallelSolution();
        ParallelSolution(int batchsize, int standbysize, int agentsnum);
        void genRepairBatches(int num_failures, vector<int> fail_node_list, string scenario, bool enqueue);

};

#endif
