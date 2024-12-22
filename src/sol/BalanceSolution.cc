#include "BalanceSolution.hh"
#include "OfflineSolution.hh"
#include "RepairBatch.hh"
#include <cstdlib>
#include <iostream>
#include <ostream>
#include <unordered_map>


BalanceSolution::BalanceSolution(){
}

BalanceSolution::BalanceSolution(int batchsize, int standbysize, int agentsnum) {
    _batch_size = batchsize;
    _standby_size = standbysize;
    _agents_num = agentsnum;
    _cluster_size = standbysize + agentsnum;
}

vector<vector<int>> deepCopyTable(const std::vector<std::vector<int>>& source) {
    vector<vector<int>> destination;
    destination.resize(source.size());
    for (size_t i = 0; i < source.size(); ++i) {
        destination[i].resize(source[i].size());
        copy(source[i].begin(), source[i].end(), destination[i].begin());
    }
    return destination;
}

void dumpTable(const vector<vector<int>>& table) {
    const int width = 4; 
    int load = 0;
    int bdwt = 0;
    
    cout << "    ";
    for (int i = 0; i < table.size(); ++i) {
        cout << setw(width) << i << " ";
    }
    cout << endl;

    cout << "in  ";
    for (const auto& row : table) {
        cout << setw(width) << row[1] << " ";
        bdwt += row[1];
        load = max(load, row[1]);
    }
    cout << endl;

    cout << "out ";
    for (const auto& row : table) {
        cout << setw(width) << row[0] << " ";
        load = max(load, row[0]);
    }
    cout << endl;

    cout << "load = " << load << " bdwt = " << bdwt << endl;

}

void BalanceSolution::genRepairBatches(int num_failures, vector<int> fail_node_list, string scenario, bool enqueue) {
    cout << "BalanceSolution::genRepairBatches start" << endl;
    // We assume that the replacement node ids are the same with the failed ids
    int ecn = _ec->_n;
    int eck = _ec->_k;
    int ecw = _ec->_w;
    string offline_solution_path = _conf->_tpAffinityDir+"/"+_codename+"_"+to_string(ecn)+"_"+to_string(eck)+"_"+to_string(ecw)+".xml";
    cout << "offline_solution_path: " << offline_solution_path << endl;
    _tp = new TradeoffPoints(offline_solution_path);

    struct timeval time1, time2, time3;

    // Xiaolu comment
    // enqueue == true: generated RepairBatch is pushed into a queue in SolutionBase::_batch_queue
    // enqueue == false: generated RepairBatch is added into a vector in SolutionBase::_batch_list
    _enqueue = enqueue;

    if (num_failures == 1) {
        switch(GlobalData::getInstance()->testModel){
            case OFFLINE:
                genRepairBatchesForSingleFailure_new(fail_node_list[0], scenario, enqueue,OFFLINE_INIT,  false);  // offline 
                break;
            case OFFLINE_TUNING:
                genRepairBatchesForSingleFailure_new(fail_node_list[0], scenario, enqueue,OFFLINE_INIT,  true);  // offline + tuning
                break;
            case PARALLEL:
                genRepairBatchesForSingleFailure_new(fail_node_list[0], scenario, enqueue,PARALLEL_INIT,  false); // parallel
                break;
            case PARALLEL_TUNING:
                genRepairBatchesForSingleFailure_new(fail_node_list[0], scenario, enqueue,PARALLEL_INIT,  true);  // parallel + tuning
                break;
            default:
                cout << "[ERROR] WRONG TEST MODEL" << endl;
                exit(1);
        }
    }else{
        cout << "not support multiple failure" << endl;
        exit(1);
    }
    cout << "after gen batchs" << endl;
    gettimeofday(&time3, NULL);
    cout << "[INFO] Duration init: " << DistUtil::duration(time1, time2) << endl;
    cout << "[INFO] Gen batches duration: " << DistUtil::duration(time2, time3) << endl;
    // Xiaolu comment
    // we have finished generating RepairBatches
    _finish_gen_batches = true;
}



vector<RepairBatch*> BalanceSolution::formatReconstructionSets() {
    vector<RepairBatch*> toret;
    for(int i=0; i<_rg_num; i++){
        vector<Stripe*> cursetstripe;
        for(int j=0; j<_num_stripes_per_group; j++){
            int idx = _RepairGroup[i*_num_stripes_per_group+j];
            if (idx >= 0) {
                Stripe* repstripe = _stripe_list[idx];
                cursetstripe.push_back(repstripe);
            }
        }
        if (cursetstripe.size() > 0) {
            RepairBatch* curset = new RepairBatch(i, cursetstripe);
            toret.push_back(curset);
        }
    }
    
    cout << "debug placement1, batch_num = "<< toret.size() << endl;
    for(auto batch : toret)
    {
        for(auto stripe: batch->getStripeList())
        {
            cout << " " << stripe->getStripeId();
        }
        cout << endl;
    }

    return toret;
}



// if st1 is better than st2, then return ture;
bool BalanceSolution::isBetter(State st1, State st2)
{
    // if load and bandwidth both equal, will return false
    if(st1._load < st2._load){
        return true;
    }else if(st1._load == st2._load){
        // if(st1._bdwt < st2._bdwt){
        if(st1._bdwt <= st2._bdwt){
            return true;
        }else if(st1._bdwt == st2._bdwt){
            return rand()%2 == 0 ;
        }
    }
    return false;
}




// if st1 is better than st2, then return ture;
bool BalanceSolution::isBetter(State st1,int color1, State st2, int color2,const vector<vector<int>> & table)
{
    // just consider global input, because input load is easier to occur imbanlance
    if(st1._load < st2._load){
        return true;
    }else if(st1._load == st2._load){
        if (st1._bdwt < st2._bdwt){
            return true;
        }else{
            return table[color1][0] < table[color2][0];
        }
    }
    return false;
}

int BalanceSolution::chooseColor_single(Stripe* stripe, vector<int> childColors, unordered_map<int, int> coloring, int idx)
{
    // choose the color which has the minimum local load among the childColors node.
    int bestColor = -1;  
    State bestState(INT_MAX, INT_MAX);
    for(auto newColor : childColors)
    {   
        // try new color, and evaluate state
        // cout << "   child color: " << newColor << endl; 
        if(newColor == -1) continue;
        vector<vector<int>> testTable = stripe->evaluateChange(_cluster_size, idx, newColor);
        State state = evalTable(testTable, childColors);
        if(isBetter(state, bestState)){
            bestState = state;
            bestColor = newColor;
        }
    }
    return bestColor;
}

int BalanceSolution::chooseColor_fullnode(Stripe* stripe,const vector<int> & childColors, const vector<vector<int>> & loadTable, int idx)
{
    // choose the color which leads to minimum global_load
    int bestColor = -1;
    int minLoad = INT_MAX;  
    State bestState(INT_MAX, INT_MAX);
    
    for(auto newColor : childColors) 
    {   
        // try new color, and evaluate state
        vector<vector<int>> stripeTable = stripe->evaluateChange(loadTable.size(), idx, newColor);
        vector<vector<int>> currTable = loadTable;
        for(int i = 0; i < loadTable.size(); i++)
        {
            currTable[i][0] += stripeTable[i][0];
            currTable[i][1] += stripeTable[i][1];
        }

        State state = evalTable(currTable); // eval gloabl load
        if(isBetter(state, newColor, bestState, bestColor, currTable)){
            bestState = state;
            bestColor = newColor;
        }
    }
    return bestColor;
}


void BalanceSolution::SingleMLP(Stripe* stripe, const vector<int> & itm_idx, const vector<int> & candidates,ECDAG * ecdag, unordered_map<int, int> & coloring)
{
    // 1. init information and blank solution
    vector<int> topoIdxs = ecdag->genTopoIdxs();
    for(auto it : itm_idx)
    {
        coloring.insert(make_pair(it, -1));
        // coloring.insert(make_pair(it, stripe->_new_node));
    }
    stripe->setColoring(coloring);
    stripe->evaluateColoring();   
  
    // 2. coloring the blank node one by one    
    for (int i = 0; i < topoIdxs.size(); i++) 
    {
        // init global tabl
        int idx = topoIdxs[i];
        ECNode* node = ecdag->getECNodeMap()[idx];
        int oldColor = coloring[idx];

        // color the vetex with child colors
        vector<int> childColors = node->getChildColors(coloring);        
        int bestColor = chooseColor_single(stripe, childColors, coloring, idx);

        // for(auto child : node->getChildNodes()){
        //     cout << "   child " << child->getNodeId() << " color is " << coloring[child->getNodeId()] << endl;
        // }
        // cout << "for node " << idx  << " color = " << bestColor << endl;

        if (bestColor == oldColor)
            continue;
        coloring[idx] = bestColor;
        stripe->changeColor(idx, bestColor);
    }
    
    stripe->evaluateColoring(); 


    // vector<int> ret = stripe->getsolution();
    // for(auto it : ret) {
    //     cout << it << " " ;
    // }
    // cout << endl;

    return;
}

void BalanceSolution::GloballyMLP(Stripe* stripe, const vector<int> & itm_idx ,unordered_map<int, int> & coloring, vector<vector<int>> & 
loadTable)
{
    cout << "[DEBUG] GLOBALLY MLP: stripe" << stripe->getStripeId() << endl;

    // 1. init information and blank solution
    ECDAG * ecdag = stripe->getECDAG();
    vector<int> topoIdxs = ecdag->genTopoIdxs();
    for(auto it : itm_idx)
    {
        coloring.insert(make_pair(it, -1));
        // coloring.insert(make_pair(it, stripe->_new_node));
    }
    stripe->setColoring(coloring);
    stripe->evaluateColoring();

    // cout << "000 <<<<<<<<<<<<<<< " << endl;
    dumpTable(loadTable);
    // 2. coloring the blank node one by one
    for (int i = 0; i < topoIdxs.size(); i++) 
    {
        // init global tabl
        int idx = topoIdxs[i];
        ECNode* node = ecdag->getECNodeMap()[idx];
        int oldColor = coloring[idx];

        // color the vetex with child colors
        vector<int> childColors = node->getChildColors(coloring);
        
        int bestColor;
        if(childColors.size() == 1 && childColors[0] == -1)
        {
            bestColor = -1;
        }else {
            bestColor = chooseColor_fullnode(stripe, childColors, loadTable, idx);             
        }

        // cout << "[DEBUG] choose color" << endl;
        // cout << "node " << idx << endl;
        // for(auto v : node->getChildNodes())
        // {
        //     cout << "   node " << v->getNodeId() << " color is " << coloring[v->getNodeId()] << endl;
        //     for(auto p : v->getParentNodes())
        //     {
        //         cout << "        node " << p->getNodeId() << " color is " << coloring[p->getNodeId()] << endl; 
        //     }
        // }
        // cout << "vetex " << idx << " choose color " << bestColor << endl;

        
        if (bestColor == oldColor)
            continue;
        coloring[idx] = bestColor;
        stripe->changeColor(idx, bestColor);      
    }
    
    // 3. add into loadtable
    dumpTable(loadTable);
    loadTable = stripe->evalColoringGlobal(loadTable);
    dumpTable(loadTable);
    stripe->evaluateColoring();  
    return;
}

void BalanceSolution::prepare(Stripe* stripe, int fail_node_id, unordered_map<int, int> & res, string scenario,const vector<vector<int>> & loadTable)
{
    vector<int> curplacement = stripe->getPlacement();
    int ecn = _ec->_n;
    int eck = _ec->_k;
    int ecw = _ec->_w;

    ECDAG* ecdag = stripe->getECDAG();
    unordered_map<int, ECNode*> ecNodeMap = ecdag->getECNodeMap();
    vector<int> ecHeaders = ecdag->getECHeaders();
    vector<int> ecLeaves = ecdag->getECLeaves();
    int intermediate_num = ecNodeMap.size() - ecHeaders.size() - ecLeaves.size();

    // 1. leaves (source sub pkt)
    int realLeaves = 0;
    vector<int> avoid_node_ids;
    avoid_node_ids.push_back(fail_node_id);
    for (auto dagidx: ecLeaves) {
        int blkidx = dagidx / ecw;
        int nodeid = -1;

        if (blkidx < ecn) {
            nodeid = curplacement[blkidx];
            avoid_node_ids.push_back(nodeid);
            realLeaves++;
        }
        res.insert(make_pair(dagidx, nodeid));
    }

    // 2. headers (concact sub pkt)
    int repair_node_id = getReplacementNode(stripe->getStripeId(), scenario, avoid_node_ids, loadTable);
    stripe->_new_node = repair_node_id;
    res.insert(make_pair(ecHeaders[0], repair_node_id));

    int fail_block_idx = stripe->getFailBlkIdx(fail_node_id);
    cout << "[INFO] stripe " << stripe->getStripeId() << " fail nodedix is " << fail_block_idx << " choose new node" << repair_node_id << endl;
}

void BalanceSolution::genBalanceColoringForSingleFailure(Stripe* stripe, 
        int fail_node_id, string scenario,  vector<vector<int>> & loadTable) {
    assert(loadTable.size() == _cluster_size);
    unordered_map<int,int>coloring;
    prepare(stripe, fail_node_id, coloring, scenario, loadTable);
    ECDAG* ecdag = stripe->getECDAG();
    vector<int> itm_idx = ecdag->genItmIdxs();
    stripe->dumpPlacement();    
    GloballyMLP(stripe ,itm_idx, coloring, loadTable);
} 

void BalanceSolution::genOfflineColoringForSingleFailure(Stripe* stripe, 
        int fail_node_id, string scenario,  vector<vector<int>> & loadTable) {
    
    unordered_map<int,int>coloring;
    prepare(stripe, fail_node_id, coloring, scenario, loadTable);
    ECDAG* ecdag = stripe->getECDAG();
    vector<int> itm_idx = ecdag->genItmIdxs();
    
    int fail_block_idx = stripe->getFailBlkIdx(fail_node_id);

    // get offline result and color
    int repair_node_id = stripe->_new_node;
    vector<int> itm_offline_coloring = getColoringByIdx(fail_block_idx);
    for (int i=0; i<itm_idx.size(); i++) {
        int dagidx = itm_idx[i];
        int blkidx = itm_offline_coloring[i];
        int nodeid = -1;
        if (blkidx == fail_block_idx)
            nodeid = repair_node_id;
        else if (blkidx == -1)
            nodeid = -1;
        else
            nodeid = stripe->getPlacement()[blkidx];
        // cout << "[DEBUG] offline coloring " << dagidx << " " << blkidx << " " << nodeid << endl; 
        coloring.insert(make_pair(dagidx, nodeid));
    }

    
    stripe->setColoring(coloring);
    stripe->evaluateColoring();

    for(auto it: stripe->getInMap())
    {
        int idx = it.first;
        loadTable[idx][1] += it.second;
    }

    for(auto it: stripe->getOutMap())
    {
        int idx = it.first;
        loadTable[idx][0] += it.second;
    }
} 



vector<int> BalanceSolution::getColoringByIdx(int fail_block_idx)
{
    return _tp->getColoringByIdx(fail_block_idx);
}

void BalanceSolution::initBalanceTP()
{
    vector<unsigned int> loclist;
    vector<int> nodelist;
    for (int i=0; i<_ec->_n; i++) {
        nodelist.push_back(i);
    }

    vector<string> blklist;
    for (int i=0; i<_ec->_n; i++) {
        string blkname = "blk"+to_string(i);
        blklist.push_back(blkname);              
    }

    for(int failnode = 0; failnode < _ec->_n; failnode++)
    {
        Stripe* stripe = new Stripe(0, "stripe0", blklist, nodelist);
        stripe->genRepairECDAG(_ec,failnode);
        BalanceSolution* sol = new BalanceSolution(1,1,_ec->_n+1);
        sol->init({stripe}, _ec, _codename, _conf);
        auto placement = stripe->getPlacement();
        placement.erase(std::remove(placement.begin(), placement.end(), failnode), placement.end());

        unordered_map<int, int> coloring;
        auto loadtable = vector<vector<int>>(_cluster_size, {0,0});
        sol->genBalanceColoringForSingleFailure(stripe,failnode, "standby", loadtable);

        vector<int> ret = stripe->getsolution();
        cout << "[DEBUG] failnode = " << failnode << " load = " << stripe->getLoad() << " bdwt = " << stripe->getBdwt() << endl; 
        stripe->dumpLoad(_ec->_n+1);
        for(auto it : ret)
        {
            cout << " " << it;
        }
        cout << endl;
        _balance_tp[failnode] = ret;
    }
}


void dumpPlacement(vector<RepairBatch*> batch_list)
{
    for(auto batch: batch_list)
    {
        for(auto stripe: batch->getStripeList())
        {
            cout << " " << stripe->getStripeId();
        }
        cout << " load=" << batch->getLoad() <<" avgload = " << batch->getLoad()*1.0/ batch->getStripeList().size();
        cout << endl;
    }
}

State BalanceSolution::evalTable(const vector<vector<int>> & table)
{
    int bdwt = 0;
    int load = 0;
    for(auto item: table)
    {
        // bdwt += item[0]; // out
        bdwt += item[1]; // in
        load = max(load, item[0]); 
        load = max(load, item[1]);
    }
    return State(load,bdwt);
}

State BalanceSolution::evalTable(vector<vector<int>> table, vector<int> colors)
{
    // return the load which is the maximum load among the colors_node
    // return the bdwt which is full dag bandwidth
    int bdwt = 0;
    int load = 0;
    for(auto item: table)
    {
        bdwt += item[1]; 
    }

    for(auto color : colors)
    {
        load = max(load, table[color][0]); 
        load = max(load, table[color][1]);
    }
    return State(load,bdwt);
}

string BalanceSolution::dumpTable(const vector<vector<int>> & table)
{
    string ret;
    ret += "IN:  ";
    for(auto it : table){
        ret += to_string(it[IN]) + " ";
    }
    ret += "\n";
    ret += "OUT: ";
    for(auto it : table){
        ret += to_string(it[OUT]) + " ";
    }
    ret += "\n";

    ret.pop_back();
    ret += "\n";
    return ret;
}

vector<int> genBitVec(vector<int> locList, int clusterSize)
{
    vector<int> bitVec(clusterSize, 0);
    for(auto it : locList)
    {
        bitVec[it] = 1;
    }
    return bitVec;
}

// void BalanceSolution::genRepairBatchesForSingleFailure(int fail_node_id,  string scenario, bool enqueue) {

//     cout << "BalanceSolution::genRepairBatchesForSingleFailure.fail_node_id = " << fail_node_id << endl;

//     // 0. we first figure out stripes that stores a block in $fail_node_id
//     filterFailedStripes({fail_node_id});
//     cout << "BalanceSolution::genRepairBatchesForSingleFailure.stripes to repair: " << _stripes_to_repair.size() << endl;

//     // 1. we divide stripes to repair into batches of size $batchsize
//     _num_batches = _stripes_to_repair.size() / _batch_size;
//     if (_stripes_to_repair.size() % _batch_size != 0) {        
//         _num_batches += 1; 
//     }
//     cout << "BalanceSolution::genRepairBatchesForSingleFailure.num batches = " << _num_batches << endl;

//     for (int batchid=0; batchid<_num_batches; batchid++) {
//         vector<Stripe*> cur_stripe_list;
//         vector<vector<int>> loadtable = vector<vector<int>> (_cluster_size, {0,0});
//         // i refers to the i-th stripe in this batch
//         for (int i=0; i<_batch_size; i++) {
//             // stripeidx refers to the idx in _stripes_to_repair
//             int stripeidx = batchid * _batch_size + i;
//             if (stripeidx < _stripes_to_repair.size()) {
//                 // stripeid refers to the actual id of stripe in all the stripes
//                 int stripeid = _stripes_to_repair[stripeidx];
//                 Stripe* curstripe = _stripe_list[stripeid];
//                 // 1.1 construct ECDAG to repair
//                 ECDAG* curecdag = curstripe->genRepairECDAG(_ec, fail_node_id);
//                 // 1.2 generate centralized coloring for the current stripe
//                 genBalanceColoringForSingleFailure(curstripe, fail_node_id, scenario, loadtable);
//                 // 1.3 set the coloring result in curstripe
//                 // 1.4 evaluate the coloring solution
//                 curstripe->dumpLoad(_cluster_size);
//                 // 1.4 insert curstripe into cur_stripe_list
//                 cur_stripe_list.push_back(curstripe);
//             }

//         }

//         // generate a batch based on current stripe list
//         RepairBatch* curbatch = new RepairBatch(batchid, cur_stripe_list);
//         curbatch->evaluateBatch(_cluster_size);
//         // insert current batch into batch list or batch queue
//         if (enqueue) {
//             _batch_queue.push(curbatch);
//         } else {
//             _batch_list.push_back(curbatch);
//         }
//         curbatch->dump();

//         //break;
//     }
// }

void BalanceSolution::genRepairBatchesForSingleFailure_new(int fail_node_id,  string scenario, bool enqueue, int initModel, bool isTunning) {
    cout << "[INFO] fail_node_id = " << fail_node_id << endl;
    // 0. we first figure out stripes that stores a block in $fail_node_id
    filterFailedStripes({fail_node_id});
    cout << "[INFO] stripes to repair: " << _stripes_to_repair.size() << endl;

    // 1. we divide stripes to repair into batches of size $batchsize
    _num_batches = _stripes_to_repair.size() / _batch_size;
    if (_stripes_to_repair.size() % _batch_size != 0) {        
        _num_batches += 1; 
    }
    cout << "[INFO] num batches = " << _num_batches << endl;

    for (int batchid=0; batchid<_num_batches; batchid++) {
        cout << "[INFO] INIT BACTH = " << batchid << endl;
        // for one batch

        // 1.initiate
        vector<Stripe*> cur_stripe_list;
        vector<vector<int>> loadtable = vector<vector<int>> (_cluster_size, {0,0});
        // i refers to the i-th stripe in this batch
        for (int i=0; i<_batch_size; i++) {
            // stripeidx refers to the idx in _stripes_to_repair
            int stripeidx = batchid * _batch_size + i;
            cout << "[INFO] INIT STRIPE= " << stripeidx << endl;
            // stripeid refers to the actual id of stripe in all the stripes
            int stripeid = _stripes_to_repair[stripeidx];
            Stripe* curstripe = _stripe_list[stripeid];

            // 1.1 construct ECDAG to repair
            ECDAG* curecdag = curstripe->genRepairECDAG(_ec, fail_node_id);

            // 1.2 generate centralized coloring for the current stripe
            if(initModel == OFFLINE_INIT)
            {
                genOfflineColoringForSingleFailure(curstripe, fail_node_id, scenario, loadtable);
            }
            else {
                genBalanceColoringForSingleFailure(curstripe, fail_node_id, scenario, loadtable);
            }

            curstripe->dumpLoad(_cluster_size);
            curstripe->dumpTrans();
            // if(curstripe->getStripeId() == 4) 
            //     exit(1);

            // 1.3 insert curstripe into cur_stripe_list
            cur_stripe_list.push_back(curstripe);        
        }
        RepairBatch* curbatch = new RepairBatch(batchid, cur_stripe_list);
        curbatch->evaluateBatch(_cluster_size);
        cout << curbatch->dumpLoad(_cluster_size);
        
        // 2. fine
        while(isTunning)
        {
            // 1. find the maximum load node;
            int maxLoadBefore = curbatch->getLoad();
            int nodeidx = curbatch->maxLoadNode();
            vector<int> nodeidxs = curbatch->maxLoadNodes(maxLoadBefore);
            vector<pair<Stripe*,int>> sortStripes;
            for (auto nodeidx : nodeidxs)
            {
                bool isIn = false; 
                if(curbatch->getInputMap()[nodeidx] == curbatch->getLoad())
                {
                    isIn = true; // max load is input load
                }
                curbatch->maxContribute(nodeidx, isIn, sortStripes);
            }

            sort(sortStripes.begin(), sortStripes.end(), [](pair<Stripe*,int> p1, pair<Stripe*, int> p2){
                return p1.second > p2.second;
            });

            vector<unordered_map<int,int>> history_coloring; 
            vector<Stripe*> stripes;
            for (auto & pair : sortStripes)
            {
                auto stripe = pair.first;
                stripes.push_back(stripe);
                history_coloring.push_back(stripe->getColoring());
            }
            
            // tuning
            tuning(curbatch, stripes, fail_node_id, scenario);
            int maxLoadAfter = curbatch->getLoad();
            // dumpTable(curbatch->getLoadTable(_cluster_size));
            cout << "[DEBUG] Before tuning: " << maxLoadBefore << " after tuning: " << maxLoadAfter << endl;
            if(maxLoadAfter >= maxLoadBefore)
            {
                cout << "[DEBUG] Tuning Finished" << endl;
                for (size_t i = 0; i < stripes.size(); i++)
                {
                    Stripe* stripe = stripes[i];
                    stripe->setColoring(history_coloring[i]);
                    stripe->evaluateColoring();
                }
                curbatch->evaluateBatch(_cluster_size);
                break;
            }
        }

        // generate a batch based on current stripe list
        // insert current batch into batch list or batch queue
        if (enqueue) {
            _batch_queue.push(curbatch);
        } else {
            _batch_list.push_back(curbatch);
        }
        curbatch->dump();
    }
}

// void BalanceSolution::tuning(RepairBatch* batch,Stripe* stripe,  int fail_node_id, string scenario)
// {
    
//     vector<vector<int>> loadTable = vector<vector<int>> (_cluster_size, {0,0});
//     for(auto it: batch->getStripeList())
//     {
//         if(it == stripe){
//             cout   << "[DEBUG] strip idx = " << stripe->getStripeId() << endl;
//             int debug;
//             // cin >> debug;
//              continue;
//         }
           
//         it->addToTable(loadTable);
//     }
//     cout << "[DEBUG] after load" << endl;
//     dumpTable(loadTable);
//     genBalanceColoringForSingleFailure(stripe, fail_node_id, scenario, loadTable);
//     batch->evaluateBatch(_cluster_size);
// }

void BalanceSolution::tuning(RepairBatch* batch, vector<Stripe*> stripes,  int fail_node_id, string scenario)
{
    // 1. withdraw
    vector<vector<int>> loadTable = vector<vector<int>> (_cluster_size, {0,0});

    for(auto it: batch->getStripeList())
    {
        // skip
        if (find(stripes.begin(), stripes.end(), it) != stripes.end())
        {
            continue;
        }  
        // update load table         
        it->addToTable(loadTable);
    }
    cout << "[DEBUG] Init: after withdraw " << endl;
    cout << dumpTable(loadTable);

    cout << "[DEBUG] withdraw contibute stripes: ";
    for (auto stripe: stripes)
    {
        cout << stripe->getStripeId() << " ";
    }
    cout << endl;
    
    // Insert strips 
    for (auto stripe : stripes)
    {
        genBalanceColoringForSingleFailure(stripe, fail_node_id, scenario, loadTable);
        batch->evaluateBatch(_cluster_size);
        stripe->dumpLoad(_cluster_size);
        stripe->dumpTrans();
        cout << "[DEBUG] After insert stripe" << stripe->getStripeId() << " load is : " << endl;
        cout << dumpTable(loadTable);
        
    }
}
