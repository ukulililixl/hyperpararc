#ifndef _STRIPE_HH_
#define _STRIPE_HH_

#include "../inc/include.hh"
#include "../util/DistUtil.hh"
#include "../common/Globaldata.hh"
#include "../ec/ECBase.hh"
#include "../ec/ECDAG.hh"
#include "../ec/Task.hh"

using namespace std;
#define IN 1
#define OUT 0

class State{
    public:
    int _load = 0;
    int _bdwt = 0;
    State(int load, int bdwt){
        _load = load;
        _bdwt = bdwt;
    }
};

class Stripe {

    private:
        int _stripe_id;

        // node that stores blocks of this stripe
        vector<int> _nodelist;

        // the ecdag to repair a block
        ECDAG* _ecdag;

        // map a sub-packet idx to a **real physical** node id
        unordered_map<int, int> _coloring;
        
        // statistics
        // for each node, count the number of sub-packets that it receives
        unordered_map<int, int> _in; 
        // for each node, count the number of sub-packets that it sends
        unordered_map<int, int> _out;

        int _bdwt;
        int _load;

        // for prototype
        string _stripe_name;
        vector<string> _blklist;
        unordered_map<int, vector<Task*>> _taskmap;

        // for single failure
        int _fail_blk_idx;
        

    public:
        Stripe(int stripeid, vector<int> nodelist);
        Stripe(int stripeid, string stripename, vector<string> blklist, vector<int> nodelist);
        Stripe(int stripeid, string stripename, vector<string> blklist, vector<unsigned int> loclist, vector<int> nodelist);
        ~Stripe();
        vector<int> getPlacement();
        int getStripeId();

        ECDAG* genRepairECDAG(ECBase* ec, int fail_node_id);
        ECDAG* getECDAG();
        
        void setColoring(unordered_map<int, int> coloring);
        unordered_map<int, int> getColoring();
        void evaluateColoring();

        unordered_map<int, int> getInMap();
        unordered_map<int, int> getOutMap();
        int getBdwt();
        int getLoad();

        unordered_map<int, vector<Task*>> genRepairTasks(int batchid, int ecn, int eck, int ecw, unordered_map<int, int> fail2repair);
        void genLevel0(int batchid, int ecn, int eck, int ecw, vector<int> leaves, unordered_map<int, vector<Task*>>& res);
        void genIntermediateLevels(int batchid, int ecn, int eck, int ecw, vector<int> leaves, unordered_map<int, vector<Task*>>& res);
        void genLastLevel(int batchid, int ecn, int eck, int ecw, vector<int> leaves, unordered_map<int, vector<Task*>>& res);

        int getTaskNumForNodeId(int nodeid);
        vector<Task*> getTaskForNodeId(int nodeid);
        int _new_node = -1;
        void dumpLoad(int);
        void dumpPlacement();
        void dumpColoring();
        void dumpTrans();
        vector<vector<int>> evalColoringGlobal(vector<vector<int>> loadTable);
        void changeColor(int idx, int new_color);
        vector<vector<int>> evaluateChange(int agent_num, int idx, int new_color);
        int getMinOutput(int nodeid);

        vector<int> getsolution();

        int getFailBlkIdx(int fail_node_id);
        void BruteForce(); // 穷举

        void addToTable(vector<vector<int>>& loadTable);
        string getStringAffinity(int cluster_size, vector<int> itm_sol) ;
};

#endif
