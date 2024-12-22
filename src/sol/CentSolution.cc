#include "CentSolution.hh"

CentSolution::CentSolution(){
}


CentSolution::CentSolution(int batchsize, int standbysize, int agentsnum) {
    _batch_size = batchsize;
    _standby_size = standbysize;
    _agents_num = agentsnum;
    _cluster_size = agentsnum+standbysize;
}

void CentSolution::genRepairBatches(int num_failures, vector<int> fail_node_list, string scenario, bool enqueue) {
    // We assume that the replacement node ids are the same with the failed ids
    cout << "CentSolution::genRepairBatches" << endl;

    if (num_failures == 1) {
        genRepairBatchesForSingleFailure(fail_node_list[0], scenario, enqueue);
    }

    _finish_gen_batches = true;
}

void CentSolution::genRepairBatchesForSingleFailure(int fail_node_id, string scenario, bool enqueue) {

    LOG << "CentSolution::genRepairBatchesForSingleFailure.fail_node_id = " << fail_node_id << endl;

    // 0. we first figure out stripes that stores a block in $fail_node_id
    filterFailedStripes({fail_node_id});
    LOG << "CentSolution::genRepairBatchesForSingleFailure.stripes to repair: " << _stripes_to_repair.size() << endl;

    // 1. we divide stripes to repair into batches of size $batchsize
    _num_batches = _stripes_to_repair.size() / _batch_size;
    if (_stripes_to_repair.size() % _batch_size != 0) {        
        _num_batches += 1; 
    }
    LOG << "CentSolution::genRepairBatchesForSingleFailure.num batches = " << _num_batches << endl;

    for (int batchid=0; batchid<_num_batches; batchid++) {

        vector<Stripe*> cur_stripe_list;
        LOG << "debug 43 clustersize = " << _cluster_size << endl;
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
                ECDAG* curecdag = curstripe->genRepairECDAG(_ec, fail_node_id);

                // 1.2 generate centralized coloring for the current stripe
                unordered_map<int, int> curcoloring;
                genCentralizedColoringForSingleFailure(curstripe, curcoloring, fail_node_id, scenario, loadTable);

                // set the coloring result in curstripe
                curstripe->setColoring(curcoloring);

                // evaluate the coloring solution
                curstripe->evaluateColoring();
                loadTable = curstripe->evalColoringGlobal(loadTable);
                // insert curstripe into cur_stripe_list
                cur_stripe_list.push_back(curstripe);
                
            }

        }

        // generate a batch based on current stripe list
        RepairBatch* curbatch = new RepairBatch(batchid, cur_stripe_list);
        curbatch->evaluateBatch(_cluster_size);

        // insert current batch into batch list or batch queue
        if (enqueue) {
            _batch_queue.push(curbatch);
        } else {
            _batch_list.push_back(curbatch);
        }
        curbatch->dump();

        //break;
    }

}


void CentSolution::genCentralizedColoringForSingleFailure(Stripe* stripe, unordered_map<int, int>& res, 
    int fail_node_id, string scenario, vector<vector<int>> & loadTable) {
    // map a sub-packet idx to a real physical node id
    
    ECDAG* ecdag = stripe->getECDAG();
    vector<int> curplacement = stripe->getPlacement();
    int ecn = _ec->_n;
    int eck = _ec->_k;
    int ecw = _ec->_w;

    // 0. get leave vertices
    vector<int> leaves = ecdag->getECLeaves();
    // 1. get all vertices
    vector<int> allvertices = ecdag->getAllNodeIds();

    // 2. figure out node id of leaves
    vector<int> avoid_node_ids;
    for (int i=0; i<leaves.size(); i++) {
        int dagidx = leaves[i];
        int blkidx = dagidx/ecw;

        int nodeid = -1;
        if (blkidx < ecn) {
            // it's a real block, otherwise, it's a virtual block(shortening)
            nodeid = curplacement[blkidx];
            avoid_node_ids.push_back(nodeid);
        }
        res.insert(make_pair(dagidx, nodeid));
    }
    // 2.1 avoid fail nodeid
    avoid_node_ids.push_back(fail_node_id);

    // 3. figure out a nodeid that performs the centralized repair
    int repair_node_id = getReplacementNode(stripe->getStripeId(), scenario, avoid_node_ids, loadTable);
    stripe->_new_node = repair_node_id;

    // 4. for all the dagidx in allvertices, record nodeid in res
    for (int i=0; i<allvertices.size(); i++) {
        int dagidx = allvertices[i];
        if (res.find(dagidx) != res.end())
            continue;
        else {
            res.insert(make_pair(dagidx, repair_node_id));
        }
    }
}
