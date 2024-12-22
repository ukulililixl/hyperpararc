#include "SolutionBase.hh"

SolutionBase::SolutionBase() {
    _finish_gen_batches = false;
    _batch_request = false;
}

SolutionBase::SolutionBase(string param) {
    cout << "SolutionBase::param = " << param << endl;
}

void SolutionBase::init(vector<Stripe*> stripe_list, ECBase* ec, string codename, Config* conf) {
    _stripe_list = stripe_list;
    _ec = ec;
    _codename = codename;
    _conf = conf;
}

vector<RepairBatch*> SolutionBase::getRepairBatches() {
    cout << "SolutionBase::getRepairBatches" << endl;
    return _batch_list;
}

RepairBatch* SolutionBase::getRepairBatchFromQueue() {
    if(_batch_queue.getSize()== 0){
        _lock.lock();
        _batch_request = true;
        _lock.unlock();
    }
    RepairBatch* toret = _batch_queue.pop();
    return toret;
}

void SolutionBase::filterFailedStripes(vector<int> fail_node_list) {

    for (int stripeid=0; stripeid < _stripe_list.size(); stripeid++) {
        Stripe* curstripe = _stripe_list[stripeid];
        vector<int> curplacement = curstripe->getPlacement();

        for (int blkid=0; blkid<curplacement.size(); blkid++) {
            int curnode = curplacement[blkid];
            if (find(fail_node_list.begin(), fail_node_list.end(), curnode) == fail_node_list.end())
                continue;
            else
                _stripes_to_repair.push_back(stripeid);
        }
    }

}

bool SolutionBase::hasNext() {
    if (!_finish_gen_batches) 
        return true;
    else if (_batch_queue.getSize())
        return true;
    else
        return false;
}



int SolutionBase::getReplacementNode(int stripeid, string scenario, vector<int> & avoid_node_ids,const vector<vector<int>> & loadTable){
    int repair_node_id;
    if(scenario == "scatter"){
        // choose the node with the minimum input load
        repair_node_id = -1;
        vector<int> candidates;
        // remove source nodes that we should avoid 
        for (int i=0; i<_agents_num; i++) {
            if (find(avoid_node_ids.begin(), avoid_node_ids.end(), i) == avoid_node_ids.end())
                candidates.push_back(i);
        }
        
        // choose the minimum inputLoad node as new_node
        int minLoad = INT_MAX;
        for(auto it : candidates)
        {
            int inload = loadTable[it][1];

            if(inload < minLoad)
            {
                minLoad = inload;
                repair_node_id = it;
            }
        }
        
        
    }else if(scenario == "standby"){
        // pooling
        int idx = stripeid % _standby_size;
        repair_node_id =  _agents_num + idx;
    }else{
        cout << "Error scenario: "<< scenario << endl;
    }
    return repair_node_id;
}