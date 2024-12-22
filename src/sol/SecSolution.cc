#include "SecSolution.hh"

SecSolution::SecSolution(){
}

void SecSolution::genRepairBatches(int num_failures, vector<int> fail_node_list, string scenario, bool enqueue) {
    // We assume that the replacement node ids are the same with the failed ids
    cout << "SecSolution::genRepairBatches" << endl;

    if (num_failures == 1) {
        genRepairBatchesForSingleFailure(fail_node_list[0], scenario, enqueue);
    }

    _finish_gen_batches = true;
}

void SecSolution::genRepairBatchesForSingleFailure(int fail_node_id, string scenario, bool enqueue) {

    cout << "SecSolution::genRepairBatchesForSingleFailure.fail_node_id = " << fail_node_id << endl;

    // 0. we first figure out stripes that stores a block in $fail_node_id
    filterFailedStripes({fail_node_id});
    cout << "SecSolution::genRepairBatchesForSingleFailure.stripes to repair: " << _stripes_to_repair.size() << endl;

    // 1. we apply the algorithm of selective ec to construct batches
    int remaining_stripes = _stripes_to_repair.size();
    unordered_map<int, int> schedule_map;

    // print placement 
    vector<int> node_place_count(_agents_num+1,0);

    for (int i=0; i<_stripes_to_repair.size(); i++) {
       int stripeid = _stripes_to_repair[i];
       Stripe* curstripe = _stripe_list[stripeid];
       vector<int> nodeidlist = curstripe->getPlacement();
       cout << "stripe " << stripeid << ": ";
       for (int j=0; j<nodeidlist.size(); j++) {
           cout << nodeidlist[j] << " ";
           cout << nodeidlist[j] << endl;
           node_place_count[nodeidlist[j]]++;
       }
       cout << endl;
    }

    cout << "node place load" << endl;
    for(int i = 0; i < node_place_count.size(); i++)
    {
        cout << i << ":" << node_place_count[i] << endl;
    }
    int batchid = 0;

    while (remaining_stripes) {

        // 2. each time, we start with a initial batch with up to bs = _agents_num-1 stripes
        unordered_map<int, Stripe*> curstripemap;
        unordered_map<int, int> pendingmap;

        for (int i=0; i<_stripes_to_repair.size(); i++) {
            int stripeid = _stripes_to_repair[i];
            if (schedule_map.find(stripeid) != schedule_map.end())
                continue;

            if (curstripemap.size() < _agents_num - 1) { 
                curstripemap.insert(make_pair(stripeid, _stripe_list[stripeid]));
            } else
                pendingmap.insert(make_pair(stripeid, 1)); // it has not been visited before
        }
        cout << "initial stripe num: " << curstripemap.size() << endl;

        // 3 construct flowgraph for a max flow
        FlowGraph* flowgraph;

        FlowGraph* maxflowgraph;
        int maxmaxflow;
        
        while(true) {

            // 1.2 find maxflow for the current batch
            // flowgraph = new FlowGraph(curstripemap, _ec->_n-1 + _ec->_n - _ec->_k, fail_node_id);
            // flowgraph = new FlowGraph(curstripemap, _ec->_n - _ec->_k, fail_node_id);
            flowgraph = new FlowGraph(curstripemap, _ec->_n-1, fail_node_id);
            int maxflow = flowgraph->findMaxFlow();
            cout << "maxflow: " << maxflow << endl;

            if(maxflow > maxmaxflow){
                maxmaxflow = maxflow;
                maxflowgraph = flowgraph;
            }

            if (flowgraph->isSaturated()) {
                // saturated, we have find a batch
                break;
            } else {
                // find a stripe that has the minimum flow
                int minflow_stripeid = flowgraph->getStripeIdWithMinFlow();
                Stripe* minflow_stripe = curstripemap[minflow_stripeid];
                // figure out the number of saturated nodes for this stripe
                int minflow_fullnum = flowgraph->getFullNodeNumForStripe(minflow_stripe, _ec->_n-1, fail_node_id);
                cout << "minflow_stripeid: " << minflow_stripeid << ", minflow_fullnum: " << minflow_fullnum << endl;

                // try to change it with other stripes that we hasn't visited
                bool change = false;
                for (auto item: pendingmap) {
                    
                    int changeto_stripeid = item.first;
                    Stripe* changeto_stripe = _stripe_list[changeto_stripeid];
                    int changeto_fullnum = flowgraph->getFullNodeNumForStripe(changeto_stripe, _ec->_n-1, fail_node_id);
                    // cout << "changeto_stripeid: " << changeto_stripeid << ", changeto_fullnum: " << changeto_fullnum << endl;

                    if (changeto_fullnum < minflow_fullnum) {
                        cout << "try to replace " << minflow_stripeid << " with " << changeto_stripeid << endl;
                        curstripemap.erase(curstripemap.find(minflow_stripeid));
                        curstripemap.insert(make_pair(changeto_stripeid, changeto_stripe));
                        pendingmap.erase(pendingmap.find(changeto_stripeid));

                        change = true;
                        break;
                    }
                }

                if (change == true)
                    delete flowgraph;
                else
                    break;
            }
        }

        // 4. select stripes based on flow graph
        cout << "choose stripes: " << endl;
        vector<int> choose_stripeid_list = flowgraph->chooseStripes(_ec->_n-1);
        vector<Stripe*> cur_stripe_list;
        for (int i=0; i<choose_stripeid_list.size(); i++) {
            int stripeid = choose_stripeid_list[i];
            Stripe* curstripe= _stripe_list[stripeid];
            cur_stripe_list.push_back(curstripe);

            cout << "stripe " << stripeid << ": ";
            for (auto nid: curstripe->getPlacement()) {
               cout << nid << " ";
            }
            cout << endl;
        }

        // 5. find block placement
        FlowGraph* placement_flowgraph = new FlowGraph(cur_stripe_list, fail_node_id, _agents_num);
        placement_flowgraph->findMaxFlow();
        unordered_map<int, int> sid2nid = placement_flowgraph->getRepairNodes();
        assert(sid2nid.size() == choose_stripeid_list.size());

        // 6. coloring in each stripe
        for (int i=0; i<choose_stripeid_list.size(); i++) {
            int stripeid = choose_stripeid_list[i];
            Stripe* curstripe= _stripe_list[stripeid];
            int repairnodeid = sid2nid[stripeid];
            curstripe->_new_node = repairnodeid;
            // construct ECDAG to repair
            ECDAG* curecdag = curstripe->genRepairECDAG(_ec, fail_node_id);

            // generate secralized coloring for the current stripe
            unordered_map<int, int> curcoloring;
            genCentralizedColoringForSingleFailure(curstripe, curcoloring, fail_node_id, scenario, repairnodeid);

            // set the coloring result in curstripe
            curstripe->setColoring(curcoloring);

            // evaluate the coloring solution
            curstripe->evaluateColoring();

            // mark stripeid as scheduled
            schedule_map.insert(make_pair(stripeid, 1));
        }
        remaining_stripes -= choose_stripeid_list.size();

        // 7. construct repairbatch
        RepairBatch* curbatch = new RepairBatch(batchid++, cur_stripe_list);
        _batch_list.push_back(curbatch);
        curbatch->evaluateBatch(_agents_num);
        curbatch->dump();
        curbatch->dumpLoad(_agents_num);
        delete flowgraph;
        delete placement_flowgraph;
    }

}


void SecSolution::genCentralizedColoringForSingleFailure(Stripe* stripe, unordered_map<int, int>& res, 
        int fail_node_id, string scenario, int repairnodeid) {
    // map a sub-packet idx to a real physical node id
    
    ECDAG* ecdag = stripe->getECDAG();
    vector<int> curplacement = stripe->getPlacement();
    int ecn = _ec->_n;
    int eck = _ec->_k;
    int ecw = _ec->_w;

    // 0. get leave vertices
    vector<int> leaves = ecdag->getECLeaves();
    //cout << "leave num: " << leaves.size() << endl;
    //cout << "  ";
    //for (int i=0; i<leaves.size(); i++)
    //    cout << leaves[i] << " ";
    //cout << endl;

    // 1. get all vertices
    vector<int> allvertices = ecdag->getAllNodeIds();
    //cout << "all idx: " << allvertices.size() << endl;
    //cout << "  ";
    //for (int i=0; i<allvertices.size(); i++)
    //    cout << allvertices[i] << " ";
    //cout << endl;

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

        //cout << "dagidx: " << dagidx << ", blkidx: " << blkidx << ", nodeid: " << nodeid << endl;
        res.insert(make_pair(dagidx, nodeid));
    }
    // 2.1 avoid fail nodeid
    avoid_node_ids.push_back(fail_node_id);

    // 3. figure out a nodeid that performs the centralized repair
    int repair_node_id = repairnodeid;

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
