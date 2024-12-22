#include "OfflineSolution.hh"

OfflineSolution::OfflineSolution(){

}

OfflineSolution::OfflineSolution(int batchsize, int standbysize, int agentsnum) {
    _batch_size = batchsize;
    _standby_size = standbysize;
    _agents_num = agentsnum;
    _cluster_size = agentsnum+standbysize;
}
void OfflineSolution::genRepairBatches(int num_failures, vector<int> fail_node_list, string scenario, bool enqueue) {
    // We assume that the replacement node ids are the same with the failed ids
    cout << "OfflineSolution::genRepairBatches" << endl;

    // read offline tradeoff points
    int ecn = _ec->_n;
    int eck = _ec->_k;
    int ecw = _ec->_w;
    string offline_solution_path = _conf->_tpDir+"/"+_codename+"_"+to_string(ecn)+"_"+to_string(eck)+"_"+to_string(ecw)+".xml";
    cout << "offline solution path: " << offline_solution_path << endl;
    _tp = new TradeoffPoints(offline_solution_path);

    if (num_failures == 1) {
        genRepairBatchesForSingleFailure(fail_node_list[0], scenario, enqueue);
    }
    _finish_gen_batches = true;
}

void OfflineSolution::genRepairBatchesForSingleFailure(int fail_node_id, string scenario, bool enqueue) {
    // 0. we first figure out stripes that stores a block in $fail_node_id
    filterFailedStripes({fail_node_id});
    cout << "OfflineSolution::genRepairBatchesForSingleFailure stripes to repair: " << _stripes_to_repair.size() << endl;
    cout << "[DEBUG] stripes to repair: " ;
    for (auto it : _stripes_to_repair)
    {
        cout << it << " ";
    }
    cout << endl;
    
    // 1. we divide stripes to repair into batches of size $batchsize
    _num_batches = _stripes_to_repair.size() / _batch_size;
    if (_stripes_to_repair.size() % _batch_size != 0) {        
        _num_batches += 1;
    }
    for (int batchid=0; batchid<_num_batches; batchid++) {
        vector<Stripe*> cur_stripe_list;
        vector<vector<int>> loadTable = vector<vector<int>> (_cluster_size, {0,0});
        // i refers to the i-th stripe in this batch
        for (int i=0; i<_batch_size; i++) {
            // stripeidx refers to the idx in _stripes_to_repair
            int stripeidx = batchid * _batch_size + i;
            if (stripeidx < _stripes_to_repair.size()) {
                // stripeid refers to the actual id of stripe in all the stripes
                int stripeid = _stripes_to_repair[stripeidx];
                Stripe* curstripe = _stripe_list[stripeid];

                // 1.1 construct ECDAG to repair
                cout << "[DEBUG] gen repair ecdag: " << fail_node_id << endl;
                ECDAG* curecdag = curstripe->genRepairECDAG(_ec, fail_node_id);
                curecdag->dumpTOPO();

                // 1.2 generate offline coloring for the current stripe
                unordered_map<int, int> curcoloring;
                genOfflineColoringForSingleFailure(curstripe, curcoloring, fail_node_id, scenario, loadTable);
                // 1.3 set the coloring result in curstripe
                curstripe->setColoring(curcoloring);
                curstripe->dumpColoring();
                // 1.4 evaluate the coloring solution
                curstripe->evaluateColoring();

                curstripe->getECDAG()->dumpTOPO();
                curstripe->dumpTrans();
                cout << "[DEBUG] ecdag size = " << curcoloring.size() << endl;
               
                curstripe->dumpLoad(_cluster_size);
                loadTable = curstripe->evalColoringGlobal(loadTable);
            
                // 1.5 insert curstripe into cur_stripe_list
                cur_stripe_list.push_back(curstripe);
            }
        }
        // generate a batch based on current stripe list
        RepairBatch* curbatch = new RepairBatch(batchid, cur_stripe_list);
        curbatch->evaluateBatch(_cluster_size);
        // insert current batch into batch list
        if (enqueue) {
            _batch_queue.push(curbatch);
        } else {
            _batch_list.push_back(curbatch);
        }
        curbatch->dump();
    }
}


void OfflineSolution::genOfflineColoringForSingleFailure(Stripe* stripe, unordered_map<int, int>& res, 
        int fail_node_id,  string scenario, vector<vector<int>> & loadTable) {
    // map a sub-packet idx to a real physical node id
    
    ECDAG* ecdag = stripe->getECDAG();
    vector<int> curplacement = stripe->getPlacement();
    int ecn = _ec->_n;
    int eck = _ec->_k;
    int ecw = _ec->_w;

    int fail_block_idx = -1;
    for (int i=0; i<curplacement.size(); i++) {
        if (curplacement[i] == fail_node_id)
            fail_block_idx = i;
    }

    // 0. get data structures of ecdag
    unordered_map<int, ECNode*> ecNodeMap = ecdag->getECNodeMap();
    vector<int> ecHeaders = ecdag->getECHeaders();
    vector<int> ecLeaves = ecdag->getECLeaves();

    int intermediate_num = ecNodeMap.size() - ecHeaders.size() - ecLeaves.size();
    cout << "number of intermediate vertices: " << intermediate_num << endl;
    cout << "failed blockidx: " << fail_block_idx << endl;
    // 1. color leave vertices 
    // If a leave vertex is part of a real block, we first figure out its block index, and then find corresponding node id
    // If a leave vertex is part of a virtual block (shortening), we mark nodeid as -1

    int realLeaves = 0;
    vector<int> avoid_node_ids;
    for (auto dagidx: ecLeaves) {
        int blkidx = dagidx / ecw;
        int nodeid = -1;

        if (blkidx < ecn) {
            nodeid = curplacement[blkidx];
            avoid_node_ids.push_back(nodeid);
            realLeaves++;
        }
        cout << "[MAPPING] leaves dagidx: " << dagidx << ", blkidx_in_n: " << blkidx << " nodeid_in_cluster: "<< nodeid << endl;
        res.insert(make_pair(dagidx, nodeid));
    }
    avoid_node_ids.push_back(fail_node_id);

    // 2. color header
    int repair_node_id = getReplacementNode(stripe->getStripeId(), scenario, avoid_node_ids, loadTable);    
    stripe->_new_node = repair_node_id;
    res.insert(make_pair(ecHeaders[0], repair_node_id));

    // 3. read from the offline solution file for coloring of intermediate nodes
    vector<int> itm_idx;
    for (auto item: ecNodeMap) {
        int dagidx = item.first;
        if (find(ecHeaders.begin(), ecHeaders.end(), dagidx) != ecHeaders.end())
            continue;
        if (find(ecLeaves.begin(), ecLeaves.end(), dagidx) != ecLeaves.end())
            continue;
        itm_idx.push_back(dagidx);
    }
    sort(itm_idx.begin(), itm_idx.end());

    // note that in offline solution, colors are within ecn
    // if color == fail_block_idx, find corresponding repair node as the real color
    // otherwise, color = block idx, find corresponding node id
    vector<int> itm_offline_coloring = _tp->getColoringByIdx(fail_block_idx);
    for (int i=0; i<itm_idx.size(); i++) {
        int dagidx = itm_idx[i];
        int blkidx = itm_offline_coloring[i];
        

        int nodeid = -1;
        if (blkidx == fail_block_idx)
            nodeid = repair_node_id;
        else
            nodeid = curplacement[blkidx];
        cout << "[MAPPING] intermediate dagidx: " << dagidx << ", blkidx_in_n: " << blkidx << " nodeid_in_cluster: "<< nodeid << endl;
        res.insert(make_pair(dagidx, nodeid));
    }
}

void OfflineSolution::setTradeoffPoints(TradeoffPoints* tp) {
    _tp = tp;
}
