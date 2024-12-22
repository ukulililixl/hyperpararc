#include "RepairBatch.hh"
#include <cstdlib>

RepairBatch::RepairBatch(int id, vector<Stripe*> stripe_list) {

    _batch_id = id;
    _stripe_list = stripe_list;
}

RepairBatch::~RepairBatch() {

}

void RepairBatch::evaluateBatch(int agents_num ) {
    _load = 0;
    _bdwt = 0;
    _in.clear();
    _out.clear();

    for (int i=0; i<_stripe_list.size(); i++) {
        Stripe* curstripe = _stripe_list[i];
        int stripeid = curstripe->getStripeId();

        unordered_map<int, int> stripe_in_map = curstripe->getInMap();
        unordered_map<int, int> stripe_out_map = curstripe->getOutMap();

        LOG << "Stripe " << stripeid << endl;
        LOG << "  inmap:  ";
        for (int nodeid=0; nodeid<agents_num; nodeid++) {
            int count = 0;
            if (stripe_in_map.find(nodeid) != stripe_in_map.end())
                count = stripe_in_map[nodeid];
            
            if (_in.find(nodeid) == _in.end()) {
                _in.insert(make_pair(nodeid, count));
            } else {
                _in[nodeid] += count;
            } 

            LOG << count << " ";
        }
        LOG << endl;

        LOG << "  outmap: ";
        for (int nodeid=0; nodeid<agents_num; nodeid++) {
            int count = 0;
            if (stripe_out_map.find(nodeid) != stripe_out_map.end())
                count = stripe_out_map[nodeid];
            
            if (_out.find(nodeid) == _out.end()) {
                _out.insert(make_pair(nodeid, count));
            } else {
                _out[nodeid] += count;
            } 

            LOG << count << " ";
        }
        LOG << endl;
    }

    // LOG << "Batch " << _batch_id << endl;
    // LOG << "  inmap:  ";
    for (int nodeid=0; nodeid<agents_num; nodeid++) {
        int count=0;
        if (_in.find(nodeid) != _in.end())
            count = _in[nodeid];
        _bdwt += count;
        if (count > _load)
            _load = count;

        // LOG << count << " ";
    }
    // LOG << endl;

    // LOG << "  outmap: ";
    for (int nodeid=0; nodeid<agents_num; nodeid++) {
        int count=0;
        if (_out.find(nodeid) != _out.end())
            count = _out[nodeid];

        if (count > _load)
            _load = count;
        // LOG << count << " ";
    }
    
    LOG << "Batch" <<  getBatchId() << " load = " << _load << endl;
} 

vector<Stripe*> RepairBatch::getStripeList()
{
    return _stripe_list;
}

unordered_map<int,int>  RepairBatch::getInputMap()
{
    return _in;
}

unordered_map<int,int> RepairBatch::getOutputMap()
{
    return _out;
}

vector<vector<int>> RepairBatch::getLoadTable(int agents_num)
{
    vector<vector<int>> ret(agents_num, {0,0});
    for(auto stripe : _stripe_list)
    {
        for(auto it : stripe->getInMap())
        {
            ret[it.first][1] += it.second;
        }
        for(auto it : stripe->getOutMap())
        {
            ret[it.first][0] += it.second;
        }
    }
    return ret;
}

void RepairBatch::erase(Stripe* stripe)
{
    _stripe_list.erase(remove(_stripe_list.begin(), _stripe_list.end(), stripe), _stripe_list.end());
}

void RepairBatch::push(Stripe* stripe)
{
    _stripe_list.push_back(stripe);

    for(auto it : stripe->getInMap())
    {
        _in[it.first] += it.second;
    }
    for(auto it : stripe->getOutMap())
    {
        _out[it.first] += it.second;
    }
    
    _bdwt = 0;
    _load = 0;
    for(auto it: _in)
    {
        _bdwt += it.second;
        _load = max(_load, it.second);
    }
    for(auto it: _out)
    {
        _load = max(_load, it.second);
    }
}

Stripe* RepairBatch::pop()
{
    Stripe* ret = _stripe_list.back();
    _stripe_list.pop_back();
    return ret;
}

string RepairBatch::dumpLoad(int agent_num)
{
    string ret;
    ret += "batch: " +  to_string(_batch_id) + '\n';
    ret +=  "   in :";
    for(int i = 0 ; i < agent_num; i ++)
    {
        ret += to_string(_in[i]) + " ";
    }
    ret += '\n';
    ret += "   out:";
    for(int i = 0 ; i < agent_num; i ++)
    {
        ret +=  to_string(_out[i]) + " ";
    }
    ret += '\n';
    ret += "load = " + to_string(_load) + "  bdwt = " + to_string(_bdwt) + '\n';
    return ret;
}



Stripe* RepairBatch::maxContribute(int nodeidx, bool isIn)
{
    int maxContribute = 0;
    Stripe* ret = nullptr;
    for(auto stripe: _stripe_list)
    {
        int contribute = isIn ? stripe->getInMap()[nodeidx] : stripe->getOutMap()[nodeidx];
        if(contribute > maxContribute)
        {
            maxContribute = contribute;
            ret = stripe;
        }    
    }
    cout << "[INFO] max contribute stripe is " << ret->getStripeId() << " contribute is " << maxContribute << endl;
    return ret;
}

void RepairBatch::maxContribute(int nodeidx, bool isIn, vector<Stripe*>& stripes)
{
    int maxContribute = 0;
    for(auto stripe: _stripe_list)
    {
        int contribute = isIn ? stripe->getInMap()[nodeidx] : stripe->getOutMap()[nodeidx];
        if(contribute > maxContribute)
        {
            maxContribute = contribute;
        }    
    }

    for(auto stripe: _stripe_list)
    {
        int contribute = isIn ? stripe->getInMap()[nodeidx] : stripe->getOutMap()[nodeidx];
        if (contribute == maxContribute)
        {
            stripes.push_back(stripe);
            cout << "[INFO] max contribute stripe is " << stripe->getStripeId() << " contribute is " << maxContribute << " to node" << nodeidx<< endl;
        }
    }
}

void RepairBatch::maxContribute(int nodeidx, bool isIn, vector<pair<Stripe*, int>>& sortStripes)
{
    int maxContribute = 0;
    for(auto stripe: _stripe_list)
    {
        int contribute = isIn ? stripe->getInMap()[nodeidx] : stripe->getOutMap()[nodeidx];
        if(contribute > maxContribute)
        {
            maxContribute = contribute;
        }    
    }

    for(auto stripe: _stripe_list)
    {
        int contribute = isIn ? stripe->getInMap()[nodeidx] : stripe->getOutMap()[nodeidx];
        if (contribute != maxContribute)
            continue;
        bool exist = false; 
        int idx = -1;
        for (size_t i = 0; i < sortStripes.size(); i++)
        {
            auto pair = sortStripes[i];
            if (pair.first == stripe)
            {
                exist = true;
                idx = i;
                break;
            }
        }
        if (exist) 
        {
            sortStripes[idx].second += contribute;
        } else{
            sortStripes.push_back(make_pair(stripe, contribute));  
            idx = sortStripes.size() -1;
        }
        cout << "[INFO] In Batch "<< _batch_id <<" max contribute stripe is " << stripe->getStripeId() << " contribute is " << sortStripes[idx].second << " to node" << nodeidx<< endl;
    }
}



int RepairBatch::maxLoadNode()
{
    int max_load = 0;
    int node_idx = -1;

    for(auto it : _in)
    {
        if(it.second > max_load){
            node_idx = it.first;
            max_load = it.second;
        }
    }

    for(auto it : _out)
    {
        if(it.second > max_load)
        {
            node_idx = it.first;
            max_load = it.second;
        }
    }
    return node_idx;
}

vector<int> RepairBatch::maxLoadNodes(int max_load)
{
    vector<int> nodes;
    for(auto it : _in)
    {
        if(it.second == max_load){
            nodes.push_back(it.first);
        }
    }

    for(auto it : _out)
    {
        if(it.second == max_load && find(nodes.begin(), nodes.end(), it.first) == nodes.end())
        {
            nodes.push_back(it.first);
        }
    }
    return nodes;
}



int RepairBatch::getLoad() {
    return _load;
}

int RepairBatch::getBdwt() {
    return _bdwt;
}

string RepairBatch::dump() {
    string s = "[DUMP] RepairBatch " + to_string(_batch_id) + ": \n";
    for (int idx=0; idx<_stripe_list.size(); idx++) {
        Stripe* curstripe = _stripe_list[idx];
        int stripeid = curstripe->getStripeId();
        vector<int> curplace = curstripe->getPlacement();
        s += "    stripe " + to_string(stripeid) + ": ";
        for (int i=0; i<curplace.size(); i++) {
            s += to_string(curplace[i]) + " ";
        }
        s += "  newnode="  + to_string(curstripe->_new_node);
        s += "\n";
    }
    s += "    stirpe num = "  + to_string(_stripe_list.size());
    s += "    load=" +  to_string(_load);
    s += "    avgload = " + to_string(_load*1.0/ _stripe_list.size()); 
    s += "    bdwt = " + to_string(_bdwt) + '\n';
    return s;
}

void RepairBatch::genRepairTasks(int ecn, int eck, int ecw, Config* conf, 
        unordered_map<int, int> fail2repair, unsigned int coorIp) {

    struct timeval time1, time2, time3, time4, time5, time6;
    gettimeofday(&time1, NULL);

    // 0. generate tasks for each stripe
    unordered_map<int, vector<int>> nodeid2internalidx;

    for (int i=0; i<_stripe_list.size(); i++) {
        Stripe* curstripe = _stripe_list[i];
        unordered_map<int, vector<Task*>> curtaskmap = curstripe->genRepairTasks(_batch_id, ecn, eck, ecw, fail2repair);
        
        // mark that nodeid needs to deal with tasks for the i-th stripe in this batch
        for (auto item: curtaskmap) {
            int nodeid = item.first;
            if (nodeid2internalidx.find(nodeid) == nodeid2internalidx.end()) {
                vector<int> list = {i};
                nodeid2internalidx.insert(make_pair(nodeid, list));
            } else
                nodeid2internalidx[nodeid].push_back(i);
        }

        //break;
    }
    gettimeofday(&time2, NULL);
    LOG << "RepairBatch::genRepairTasks.time1-time2.sort task time: " << DistUtil::duration(time1, time2) << endl;

    // 1. for each node, send command to agent to tell the number of stripes that the agent need to deal with
    unordered_map<int, NodeBatchTask*> batchtaskmap;
    for (auto item: nodeid2internalidx) {
        int nodeid = item.first;
        vector<int> internalidx = item.second;

        // 1.0 nodeid needs to deal with tasks from $nstripes stripes
        int nstripes = internalidx.size();
        vector<int> stripelist;
        vector<int> numlist;
        unordered_map<int, vector<Task*>> stripetaskmap;

        for (int i=0; i<internalidx.size(); i++) {
            int ii = internalidx[i];
            Stripe* curstripe = _stripe_list[ii];
            int stripeid = curstripe->getStripeId();
            int tasknum = curstripe->getTaskNumForNodeId(nodeid);
            vector<Task*> tasklist = curstripe->getTaskForNodeId(nodeid);
            
            for(auto it : tasklist)
            {
                LOG << "NODE:" << nodeid << " "<<  it->dumpStr() << endl;
            }

            stripelist.push_back(stripeid);
            numlist.push_back(tasknum);
            stripetaskmap.insert(make_pair(stripeid, tasklist));
        }

        // 1.1 get ip for nodeid
        unsigned int ip;
        if (nodeid < conf->_agentsIPs.size()) {
            // nodeid is an agent
            ip = conf->_agentsIPs[nodeid];
        } else {
            // nodeid is a repair node
            int idx = nodeid - conf->_agentsIPs.size();
            ip = conf->_repairIPs[idx];
        }

        // 1.2 prepare NodeBatchTask
        NodeBatchTask* nbtask = new NodeBatchTask(_batch_id, stripelist, numlist, stripetaskmap, ip);

        //// 1.3 send AGCommand
        //nbtask->sendAGCommand(ip);
        //LOG << "RepairBatch::send to " << RedisUtil::ip2Str(ip) << endl;

        //// 1.4 send TaskCommands
        //nbtask->sendTaskCommands(ip);

        // 1.5 add nbtask into map
        batchtaskmap.insert(make_pair(nodeid, nbtask));
    }
    gettimeofday(&time3, NULL);
    
    LOG << "RepairBatch::genRepairTasks.time2-time3.generate tasks for nodes: " << DistUtil::duration(time2, time3) << endl;

    int threadnum = batchtaskmap.size();
    thread thrds[threadnum];
    int tid=0;
    for (auto item: batchtaskmap) { 
        int nodeid = item.first;
        NodeBatchTask* nbtask = item.second;
        thrds[tid++] = thread([=]{nbtask->sendCommands();});
    }

    for (int i=0; i<tid; i++) { 
        thrds[i].join();
    }
    gettimeofday(&time4, NULL);
    LOG << "RepairBatch::genRepairTasks.time3-time4.send tasks for nodes: " << DistUtil::duration(time3, time4) << endl;

    LOG << "RepairBatch::genRepairTasks.node2internalidx.size = " << nodeid2internalidx.size() << endl;
    LOG << "RepairBatch::genRepairTasks.batchtaskmap.size = " << batchtaskmap.size() << endl;

    // 2. for each node, we wait for a response that make sure corresponding node finishes assigned tasks for this batch
    tid=0;
    for (auto item: nodeid2internalidx) {
        int nodeid = item.first;
        NodeBatchTask* nbtask = batchtaskmap[nodeid];
        thrds[tid++] = thread([=]{nbtask->waitFinishFlag(nodeid, coorIp);});
    }
    for (int i=0; i<tid; i++) {
        thrds[i].join();
    }
    gettimeofday(&time5, NULL);
    LOG << "RepairBatch::genRepairTasks.time4-time5.wait finish flag: " << DistUtil::duration(time4, time5) << endl;

    // 3. clean
    for (auto item: batchtaskmap) {
        NodeBatchTask* nbtask = item.second;
        if (nbtask)
            delete nbtask;
    }
    gettimeofday(&time6, NULL);
    LOG << "RepairBatch::genRepairTasks.time5-time6.clean: " << DistUtil::duration(time5, time6) << endl;
}

int RepairBatch::getBatchId()
{
    return _batch_id;
}

