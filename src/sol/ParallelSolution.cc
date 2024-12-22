#include "ParallelSolution.hh"
#include "OfflineSolution.hh"

ParallelSolution::ParallelSolution(){
}
ParallelSolution::ParallelSolution(int batchsize, int standbysize, int agentsnum) {
    _batch_size = batchsize;
    _standby_size = standbysize;
    _agents_num = agentsnum;
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

void dumpTable(vector<vector<int>> table)
{
    cout << "in  ";
    for(auto it : table)
    {
        cout  << it[1]<<" ";
    }
    cout << endl;

    cout << "out ";
    for(auto it : table)
    {
        cout  << it[0] << " ";
    }
    cout << endl;
}

void ParallelSolution::genRepairBatches(int num_failures, vector<int> fail_node_list, string scenario, bool enqueue) {
    // We assume that the replacement node ids are the same with the failed ids
    int ecn = _ec->_n;
    int eck = _ec->_k;
    int ecw = _ec->_w;
    string offline_solution_path = _conf->_tpDir+"/"+_codename+"_"+to_string(ecn)+"_"+to_string(eck)+"_"+to_string(ecw)+".xml";
    _tp = new TradeoffPoints(offline_solution_path);

    // Xiaolu comment
    // enqueue == true: generated RepairBatch is pushed into a queue in SolutionBase::_batch_queue
    // enqueue == false: generated RepairBatch is added into a vector in SolutionBase::_batch_list
    _enqueue = enqueue;

    if (num_failures == 1) {
        genRepairBatchesForSingleFailure(fail_node_list[0], scenario);
    }else{
        cout << "not support multiple failure" << endl;
        exit(1);
    }
    cout << "after gen batchs" << endl;

    // Xiaolu comment
    // we have finished generating RepairBatches
    _finish_gen_batches = true;
}

int ParallelSolution::hungary(int rg_id, int cur_match_stripe, int* matrix, int matrix_start_addr, int* node_selection) {
    int i;
    int ret;

    for(i=0; i<_agents_num; i++){ // 遍历每个node
        if((matrix[matrix_start_addr + cur_match_stripe*_agents_num + i]==1) && (_mark[i]==0)){ 
            if (_debug)
                cout << "hungary:checknode" << i << endl;
            _mark[i]=1;

            if (_debug)
                cout << "node_selection " << i << " = " << node_selection[rg_id*_agents_num+i] << endl;

            if(node_selection[rg_id*_agents_num+i]==-1){ // 该node尚未匹配
                if (_debug)
                    cout << "hungary ret1.1: rg_id " << rg_id << ", mark (" << rg_id << ", " << i << ") = " << cur_match_stripe << endl;
                node_selection[rg_id*_agents_num+i]=cur_match_stripe;
                return 1; // 
            } else if ((ret=hungary(rg_id, node_selection[rg_id*_agents_num+i], matrix, matrix_start_addr, node_selection))==1){
                node_selection[rg_id*_agents_num+i] = cur_match_stripe;
                if (_debug)
                    cout << "hungary ret1.2: rg_id " << rg_id << ", mark (" << rg_id << ", " << i << ") = " << cur_match_stripe << endl;
                return 1;
            }
        }
    }
    if (_debug)
        cout << "hungary: rg_id " << rg_id << ", cur_match_stripe " << cur_match_stripe << ", return 0"<< endl;
    return 0;
}

int ParallelSolution::if_insert(Stripe* repairstripe, int rg_id, int cur_match_stripe, int soon_to_fail_node) {
    if (_debug) {
        cout << "if_insert::stripe_id = " << repairstripe->getStripeId() << endl;
        cout << "if_insert::rg_id = " << rg_id << endl;
        cout << "if_insert::cur_match_stripe = " << cur_match_stripe << endl;
        cout << "if_insert::soon_to_fail_node = " << soon_to_fail_node << endl;
    }

    int chunk_id;
    int k;
    int node_id;
    int ret;

    int* bak_node_belong=(int*)malloc(sizeof(int)*_agents_num); // clusterSize

    // 0. get out the nodeid of chunks in the repairstripe, and try to set bipartite matrix
    vector<int> nodeList = repairstripe->getPlacement();
    if (_debug)
        cout << "nodeids: " ;
    // for (auto chunkinfo: chunkinfos) {
    for (int i = 0; i < nodeList.size(); i++) {
        int chunkidx = i;
        node_id = nodeList[i];
        if (_debug)
            cout << node_id << " ";
        if (node_id == soon_to_fail_node)
            continue; 
        // set bipartite matrix
        _bipartite_matrix[rg_id*_num_stripes_per_group*_agents_num + cur_match_stripe*_agents_num + node_id]=1;
    }

    if (_debug)
        cout << endl;

    // 1. backup node_belong
    for(k=0; k<_agents_num; k++)
        bak_node_belong[k]=_node_belong[rg_id*_agents_num + k];

    // 2. use hungary algorithm
    for(chunk_id=0; chunk_id<_helpers_num; chunk_id++){
        if (_debug)
            cout << "check chunk" << chunk_id << endl;
            
        memset(_mark, 0, sizeof(int)*_agents_num);
        ret=hungary(rg_id, cur_match_stripe, _bipartite_matrix, rg_id*_num_stripes_per_group*_agents_num, _node_belong);
        if(ret==0)
            break;
    }

    // if the repair stripe cannot be inserted into the repair group
    if(chunk_id<=_helpers_num-1){
        // reset the bipartite matrix
        for(node_id=0; node_id<_agents_num; node_id++)
            _bipartite_matrix[rg_id*_num_stripes_per_group*_agents_num+cur_match_stripe*_agents_num+node_id]=0;
        // reset the node belong
        for(k=0; k<_agents_num; k++)
            _node_belong[rg_id*_agents_num+k]=bak_node_belong[k];
    }

    free(bak_node_belong);
    if(chunk_id<=_helpers_num-1)
        return 0;
    else
        return 1;
    return 1;
}

void ParallelSolution::update_bipartite_for_replace(int des_id, int stripe_id, int rg_id, int index_in_rg, string flag, int soon_to_fail_node) {
    // des_id is the index in _collection
    // stripe_id is the index in the overall stripes
    int k;
    int node_id;
    int bi_value;

    if(flag=="delete")
        bi_value=0;
    else
        bi_value=1;

    
    Stripe* repstripe = _stripe_list[des_id];
    vector<int> nodelist = repstripe->getPlacement();
    for(int i = 0;  i < nodelist.size(); i++)
    {
        int chunkidx = i;
        node_id = nodelist[i];
        if (node_id == soon_to_fail_node)
            continue;
        // set bipartite matrix
        _bipartite_matrix[rg_id*_num_stripes_per_group*_agents_num+index_in_rg*_agents_num+node_id]=bi_value;
    }
}

int ParallelSolution::replace(int src_id, int des_id, int rg_id, int* addi_id, int num_related_stripes, string flag, int soon_to_fail_node) {
    if (_debug)
        cout << "replace src_id = " << src_id << ", des_id = " << des_id << ", rg_id = " << rg_id << endl;

    int src_stripe_id, des_stripe_id;
    int stripe_id;
    int i;
    int j;
    int index;

    int benefit_cnt;
    int* bakp_node_belong=NULL;

    // establish the index of des_id in the _RepairGroup
    for(i=0; i<_cur_matching_stripe[rg_id]; i++)
        if(_RepairGroup[rg_id*_num_stripes_per_group+i] == des_id)
            break; 

    // delete the information of the des_id-th stripe in _bipartite_matrix
    index=i;
    des_stripe_id=_stripes_to_repair[des_id]; // 得到最初的stripe_idx
    update_bipartite_for_replace(des_id, des_stripe_id, rg_id, index, "delete", soon_to_fail_node); 
    
    if(flag=="test_replace"){
        bakp_node_belong=(int*)malloc(sizeof(int)*_agents_num);
        memcpy(bakp_node_belong, _node_belong+rg_id*_agents_num, sizeof(int)*_agents_num);                       
    }

        // update the _node_belong information
        for (i=0; i<_agents_num; i++)
            if (_node_belong[rg_id*_agents_num+i]==index)
                _node_belong[rg_id*_agents_num+i]=-1;

    // add the information of the src_id-th stripes in the _bipartite_matrix
    src_stripe_id = _stripes_to_repair[src_id];
    Stripe* src_repair_stripe = _stripe_list[src_stripe_id];
    
    // check if the stripe can be inserted into the stripe
    int ret=if_insert(src_repair_stripe, rg_id, index, soon_to_fail_node);

    if (flag=="test_replace") {
        if(ret == 0 ){
            update_bipartite_for_replace(des_id, des_stripe_id, rg_id, index, "add", soon_to_fail_node);
            memcpy(_node_belong+rg_id*_agents_num, bakp_node_belong, sizeof(int)*_agents_num);
            free(bakp_node_belong);
            return 0;
        }

        // calculate the benefit of the replacement 
        benefit_cnt=0;
        int cur_stripe_num;
        cur_stripe_num = _cur_matching_stripe[rg_id];

        // try if other stripes that are not selected can be inserted into the RG 
        for(i=src_id; i<num_related_stripes; i++){

            if(_ifselect[i]==1) continue;
            if(i==src_id) continue;
        
            stripe_id = _stripes_to_repair[i];
            Stripe* cur_repair_stripe = _stripe_list[stripe_id];
            ret=if_insert(cur_repair_stripe, rg_id, cur_stripe_num, soon_to_fail_node); 
            if(ret == 1){

                benefit_cnt++; 
                cur_stripe_num++;
                // record the additional stripe id that can be inserted
                j=0;
                while(addi_id[j]!=-1) j++;
                addi_id[j]=i;

            }

            if(cur_stripe_num==_num_stripes_per_group){
                break;
            }
        }

        // reset the _bipartite_matrix and _node_belong
        for(i=rg_id*_num_stripes_per_group*_agents_num; i<(rg_id+1)*_num_stripes_per_group*_agents_num; i++)
            _bipartite_matrix[i]=0;

        for(i=0; i<_cur_matching_stripe[rg_id]; i++){
            stripe_id=_RepairGroup[rg_id*_num_stripes_per_group+i];
            update_bipartite_for_replace(stripe_id, _stripes_to_repair[stripe_id], rg_id, i, "add", soon_to_fail_node);
        }

        memcpy(_node_belong+rg_id*_agents_num, bakp_node_belong, sizeof(int)*_agents_num);
        free(bakp_node_belong);
        return benefit_cnt;
    } else if(flag=="perform_replace"){

        _ifselect[src_id]=1;
        _ifselect[des_id]=0;

        // update _RepairGroup
        i=0;
        while(_RepairGroup[rg_id*_num_stripes_per_group+i]!=des_id) i++;
        _RepairGroup[rg_id*_num_stripes_per_group+i]=src_id;

        i=0;
        while(_record_stripe_id[i]!=-1 && i<_num_stripes_per_group){
            Stripe* repstripe = _stripe_list[_record_stripe_id[i]];
            ret=if_insert(repstripe, rg_id, _cur_matching_stripe[rg_id], soon_to_fail_node);
             
            if(ret==0){                
                printf("ERR-2: if_insert\n");
                exit(1);                                                                                      
            }
            // perform update
            _RepairGroup[rg_id*_num_stripes_per_group + _cur_matching_stripe[rg_id]] = _record_stripe_id[i];
            _cur_matching_stripe[rg_id]++;
            _ifselect[_record_stripe_id[i]]=1;
            i++;
        }
    }

    return 1;
}

int ParallelSolution::greedy_replacement(int num_related_stripes, int soon_to_fail_node, int rg_id) {
    int i;
    int best_src_id, best_des_id;
    int ret;
    int src_id;   
    int des_id; 
    int max_benefit;
    int if_benefit;

    int* addi_id=(int*)malloc(sizeof(int)*_num_stripes_per_group); 
    best_src_id=-1;
    best_des_id=-1;
    if_benefit = 1;
    max_benefit=-1;

    memset(_record_stripe_id, -1, sizeof(int)*_num_stripes_per_group); 


    if(_cur_matching_stripe[rg_id]==_num_stripes_per_group)
        return 0;

    for(src_id=0; src_id<num_related_stripes; src_id++){
        if(_ifselect[src_id]==1)
            continue;

        for(i=0; i<_cur_matching_stripe[rg_id]; i++){
            memset(addi_id, -1, sizeof(int)*_num_stripes_per_group); 
            des_id=_RepairGroup[rg_id*_num_stripes_per_group+i]; 
            string flag = "test_replace"; 
            ret = replace(src_id, des_id, rg_id, addi_id, num_related_stripes, flag, soon_to_fail_node);
            if(ret == 0) 
                continue;
            if(ret > 0){  
                best_src_id=src_id;
                best_des_id=des_id;
                max_benefit=ret;
                memcpy(_record_stripe_id, addi_id, sizeof(int)*_num_stripes_per_group);
                break;
            }
            if(max_benefit == _num_stripes_per_group - _cur_matching_stripe[rg_id])
                break;
        }
        if(max_benefit == _num_stripes_per_group - _cur_matching_stripe[rg_id])
            break;
    }

    // perform replacement
    if(max_benefit !=-1) {
        ret=replace(best_src_id, best_des_id, rg_id, addi_id, num_related_stripes, "perform_replace", soon_to_fail_node);
    } else
        if_benefit = 0;

    free(addi_id);
    return if_benefit;
}

vector<RepairBatch*> ParallelSolution::formatReconstructionSets() {
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
    return toret;
}


vector<RepairBatch*> ParallelSolution::findRepairBatchs(int soon_to_fail_node, string scenario) {
    if(DEBUG_ENABLE)
        cout << "ParallelSolution::findRepairBatchs start" << endl;

    int i;
    int stripe_id;
    int ret;
    int num_related_stripes = _stripes_to_repair.size();
    _helpers_num = _ec->_n -1;
    int _expand_ratio = 3;
    int rg_index=0;
    int flag;

    cout <<  "_agents_num " << _agents_num << endl;
    if (scenario == "scatter") {
        _num_stripes_per_group = (_agents_num-1)/_ec->_n;                      
    } else{
        // TODO
        _num_stripes_per_group = (_agents_num-1)/_ec->_n;
    }
    _num_rebuilt_chunks = num_related_stripes;
    _rg_num = (int)(ceil(_num_rebuilt_chunks*1.0/_num_stripes_per_group))*3;
    _RepairGroup=(int*)malloc(sizeof(int)*_rg_num*_num_stripes_per_group);
    _ifselect=(int*)malloc(sizeof(int)*_num_rebuilt_chunks);
    _record_stripe_id=(int*)malloc(sizeof(int)*_num_stripes_per_group);
    _bipartite_matrix=(int*)malloc(sizeof(int)*_rg_num*_num_stripes_per_group*_agents_num);
    _node_belong=(int*)malloc(sizeof(int)*_rg_num*_agents_num);
    _mark=(int*)malloc(sizeof(int)*_agents_num);
    _cur_matching_stripe=(int*)malloc(sizeof(int)*_rg_num);


    cout << "_num_rebuilt_chunks = " << _num_rebuilt_chunks << endl;
    cout << "_num_stripes_per_group = " << _num_stripes_per_group << endl;
    cout << "_rg_num = " << _rg_num << endl;


    // initialization
    int* select_stripe=(int*)malloc(sizeof(int)*_num_stripes_per_group);
    memset(_RepairGroup, -1, sizeof(int)*_rg_num*_num_stripes_per_group);
    memset(_ifselect, 0, sizeof(int)*num_related_stripes);
    memset(_bipartite_matrix, 0, sizeof(int)*_num_stripes_per_group * _agents_num * _rg_num);
    memset(_node_belong, -1, sizeof(int)*_rg_num*_agents_num);
    memset(select_stripe, -1, sizeof(int)*_num_stripes_per_group);
    memset(_cur_matching_stripe, 0, sizeof(int)*_rg_num);

    

    while(true){
        flag = 0;
        // generate an solution for a new repair group
        for(i=0; i<num_related_stripes; i++){
            if(_ifselect[i] == 1)
                continue;

            stripe_id = _stripes_to_repair[i]; 
            if (_debug)
                cout << "findReconstructionSets.stripe_id = " << stripe_id << endl;
            Stripe* repstripe = _stripe_list[stripe_id];
            ret = if_insert(repstripe, rg_index, _cur_matching_stripe[rg_index], soon_to_fail_node);
            if(ret){
                _RepairGroup[rg_index*_num_stripes_per_group + _cur_matching_stripe[rg_index]] = i;
                _ifselect[i]=1;
                _cur_matching_stripe[rg_index]++;
                flag=1;
                if(_cur_matching_stripe[rg_index] == _num_stripes_per_group)
                    break;
            }
        }

        if(!flag){
            break;
        } 
    
        // optimize that solution
        ret = 1;
        while(ret)
            ret = greedy_replacement(num_related_stripes, soon_to_fail_node, rg_index);

        rg_index++;
        assert(rg_index!=_rg_num);
    }

    free(select_stripe);
    vector<RepairBatch*> toret = formatReconstructionSets();
    return toret;
}


// if st1 is better than st2, then return ture;
bool ParallelSolution::isBetter(State st1, State st2)
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
bool ParallelSolution::isBetter(State st1,int color1, State st2, int color2,const vector<vector<int>> & table)
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

int ParallelSolution::chooseColor_single(Stripe* stripe, vector<int> childColors, unordered_map<int, int> coloring, int idx)
{
    // choose the color which has the minimum local load among the childColors node.
    int bestColor = -1;  
    State bestState(INT_MAX, INT_MAX);
    for(auto newColor : childColors)
    {   
        // try new color, and evaluate state
        if(newColor == -1) continue;
        vector<vector<int>> testTable = stripe->evaluateChange(_agents_num, idx, newColor);
        State state = evalTable(testTable, childColors);
        if(isBetter(state, bestState)){
            bestState = state;
            bestColor = newColor;
        }
    }
    return bestColor;
}

int ParallelSolution::chooseColor_fullnode(Stripe* stripe,const vector<int> & childColors, const vector<vector<int>> & loadTable, int idx)
{
    // choose the color which leads to minimum global_fulldag load
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

void ParallelSolution:: SingleMLP(Stripe* stripe, const vector<int> & itm_idx, const vector<int> & candidates,ECDAG * ecdag, unordered_map<int, int> & coloring)
{

    // 1. init information and blank solution
    vector<int> topoIdxs = ecdag->genTopoIdxs();
    for(auto it : itm_idx)
    {
        coloring.insert(make_pair(it, -1));
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
        
        if (bestColor == oldColor)
            continue;
        coloring[idx] = bestColor;
        stripe->changeColor(idx, bestColor);
    }
    stripe->evaluateColoring(); 
    stripe->dumpLoad(_agents_num);
    vector<int> ret = stripe->getsolution();
    // stripe->dumpTrans(_agents_num);

    return;
}

void ParallelSolution::GloballyMLP(Stripe* stripe, const vector<int> & itm_idx, const vector<int> & candidates,ECDAG * ecdag, unordered_map<int, int> & coloring, vector<vector<int>> & 
loadTable)
{
    if(DEBUG_ENABLE)
        cout << "GloballyMLP for stripe " << stripe->getStripeId() << endl;

    // 1. init information and blank solution
    vector<int> topoIdxs = ecdag->genTopoIdxs();
    for(auto it : itm_idx)
    {
        coloring.insert(make_pair(it, -1));
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
        int bestColor = chooseColor_fullnode(stripe, childColors, loadTable, idx);
        if (bestColor == oldColor)
            continue;
        coloring[idx] = bestColor;
        stripe->changeColor(idx, bestColor);
    }
    // 3. add into loadtable
    loadTable = stripe->evalColoringGlobal(loadTable);
    return;
}

void ParallelSolution::genParallelColoringForSingleFailure(Stripe* stripe, 
        int fail_node_id, string scenario,  vector<vector<int>> & loadTable) {
    // map a sub-packet idx to a real physical node id
    if(DEBUG_ENABLE)
        cout << "ParallelSolution::genParallelColoringForSingleFailure" << endl;
    ECDAG* ecdag = stripe->getECDAG();
    unordered_map<int, int> res;
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
    //cout << "number of intermediate vertices: " << intermediate_num << endl;

    // 1. color leave vertices 
    // If a leave vertex is part of a real block, we first figure out its block index, and then find corresponding node id
    // If a leave vertex is part of a virtual block (shortening), we mark nodeid as -1
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


    // choose new node after coloring
    int repair_node_id;
    vector<int> candidates;
    if (scenario == "standby")
        repair_node_id = fail_node_id;
    else {
        repair_node_id = -1;

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
    }
    stripe->_new_node = repair_node_id;
    res.insert(make_pair(ecHeaders[0], repair_node_id));

    // intermediate node idxs
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
    struct timeval time1, time2;
    GloballyMLP(stripe ,itm_idx, candidates, ecdag, res, loadTable);
}

void ParallelSolution::genOfflineColoringForSingleFailure(Stripe* stripe, 
        int fail_node_id, string scenario,vector<vector<int>> & loadTable, vector<int> & placement) {
    // choose a new node base on placement
    // add stripe_load on loadTable
    if(DEBUG_ENABLE)
        cout << "ParallelSolution::genOfflineColoringForSingleFailure()" << endl;
    ECDAG* ecdag = stripe->getECDAG();
    unordered_map<int, int> res;
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
    //cout << "number of intermediate vertices: " << intermediate_num << endl;

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
        res.insert(make_pair(dagidx, nodeid));
    }
    //cout << "realLeaves: " << realLeaves << endl;

    // 2. color header
    int repair_node_id = -1;
    vector<int> candidates;
    if (scenario == "standby")
        repair_node_id = fail_node_id;
    else{
        // choose a random node apart from the batch placement node
        for (int i=0; i<_agents_num; i++) {
            if (find(placement.begin(), placement.end(), i) == placement.end())
            candidates.push_back(i);
        }                        
        if(candidates.size() == 0)
        {
            // choose the minimum inputLoad node as new_node
            int minLoad = INT_MAX;
            for (int i=0; i<_agents_num; i++) 
            {
                if(find(avoid_node_ids.begin(), avoid_node_ids.end(), i) != avoid_node_ids.end())
                    continue;
                int inload = loadTable[i][1];
                if(inload < minLoad)
                {
                    minLoad = inload;
                    repair_node_id = i;
                }
            }
        }else {
            int tmpidx = rand() % candidates.size();
            repair_node_id = candidates[tmpidx];
            placement.push_back(repair_node_id);
        }
        if(DEBUG_ENABLE)
            cout << "stripe" << stripe->getStripeId() << " choose the new_node: " << repair_node_id << endl;   
    }
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

        res.insert(make_pair(dagidx, nodeid));
    }

    // add load
    stripe->setColoring(res);
    stripe->evaluateColoring();
    loadTable = stripe->evalColoringGlobal(loadTable);
}


void ParallelSolution::genColoringForSingleFailure(Stripe* stripe, unordered_map<int, int>& res, 
        int fail_node_id,  string scenario, vector<int> & placement) {
    if(DEBUG_ENABLE)
        cout << "ParallelSolution::genColoringForSingleFailure" << endl;


    _agents_num = _agents_num;
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

    //cout << "number of intermediate vertices: " << intermediate_num << endl;
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
        res.insert(make_pair(dagidx, nodeid));
    }
    // choose new node after coloring
    int repair_node_id;
    vector<int> candidates;
    if (scenario == "standby")
        repair_node_id = fail_node_id;
    else {
        repair_node_id = -1;
        // choose a random node apart from the batch placement node
        for (int i=0; i<_agents_num; i++) {
            if (find(placement.begin(), placement.end(), i) == placement.end())
            candidates.push_back(i);
        }            
        int tmpidx = rand() % candidates.size();
        repair_node_id = candidates[tmpidx];
        placement.push_back(repair_node_id);
    }
    
    res.insert(make_pair(ecHeaders[0], repair_node_id));
    // intermediate node idxs
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
    SingleMLP(stripe ,itm_idx, candidates, ecdag, res);
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
void ParallelSolution::improve(int fail_node_id, string scenario){

    struct timeval time1, time2;

    // 1. color all batchs
    gettimeofday(&time1, NULL);
    for(int i = 0; i < _batch_list.size(); i++)
    {
        RepairBatch* batch = _batch_list[i];
        vector<vector<int>> loadTable(_agents_num, {0,0}); // {out,in}
        
        // gen placement
        vector<int> placement;
        for(auto it : batch->getStripeList())
        {
            for(auto idx : it->getPlacement())
            {
                if(find(placement.begin(), placement.end(), idx) == placement.end())
                    placement.push_back(idx);
            }
        }

        // gen head batch coloring solution
        vector<Stripe*> full_stripe_list = batch->getStripeList();   
        for (int i=0; i<full_stripe_list.size(); i++) {
            Stripe* stripe = full_stripe_list[i];
            ECDAG* ecdag = stripe->genRepairECDAG(_ec, fail_node_id);
            unordered_map<int, int> coloring;
            // genColoringForSingleFailure(stripe, coloring, fail_node_id, _agents_num, scenario, placement);
            genOfflineColoringForSingleFailure(stripe, fail_node_id, scenario, loadTable, placement);  
        }
        // record information
        batch->evaluateBatch(_agents_num);
    }
    gettimeofday(&time2, NULL);
    double color_all_batch = DistUtil::duration(time1, time2);

    struct timeval improve_start, curr_improve;
    gettimeofday(&improve_start, NULL);
    vector<RepairBatch*> ret;

    // 2. insert stripe into batch
    while(true)
    {
        
        // check if has merged all batch
        if(_batch_list.size() == 0)
            break;
        sort(_batch_list.begin(), _batch_list.end(), [](RepairBatch* batch1, RepairBatch* batch2){
            return batch1->getLoad()/batch1->getStripeList().size() < batch2->getLoad()/batch2->getStripeList().size();
        });
        RepairBatch* head_batch = _batch_list[0];
        int tail_batch_idx = _batch_list.size()-1;
        int stripe_num = head_batch->getStripeList().size();
        float best_avg = evalTable(head_batch->getLoadTable(_agents_num))._load*1.0/stripe_num;
        
        vector<pair<RepairBatch*, Stripe*>> batch_stripe;
        vector<int> stripeIdx_to_insert;
        vector<vector<int>> loadTable = head_batch->getLoadTable(_agents_num); 

        // improve curr batch
        struct timeval improve1, improve2;
        gettimeofday(&improve1, NULL);

        while(true)
        {
            // insert one stripe into one head_batch
            Stripe* stripe;
            RepairBatch* batch;
            bool find_stripe = false; // find a candidate
            stripe_num = head_batch->getStripeList().size() + batch_stripe.size();

            // select a candidate stripe to insert
            struct timeval newnode1, newnode2;
            gettimeofday(&newnode1, NULL);
            for(int i = tail_batch_idx; i > 0; i--)
            {
                RepairBatch* tail_batch = _batch_list[i];
                for(auto tail_stripe : tail_batch->getStripeList())
                {
                    if(find(stripeIdx_to_insert.begin(), stripeIdx_to_insert.end(), tail_stripe->getStripeId()) != stripeIdx_to_insert.end())
                        continue;

                    // remove source nodes that we should avoid 
                    vector<int> avoid_node_ids = tail_stripe->getPlacement();
                    vector<int> candidates;
                    for (int i=0; i<_agents_num; i++) {
                        if (find(avoid_node_ids.begin(), avoid_node_ids.end(), i) == avoid_node_ids.end())
                            candidates.push_back(i);
                    }

                    // choose the minimum inputload apart from source node 
                    int min_input_load = INT_MAX;
                    int min_input_load_color = -1;
                    for(auto it : candidates)
                    {
                        int inload = loadTable[it][1];
                        if(inload < min_input_load)
                        {
                            min_input_load = inload;
                            min_input_load_color = it;
                        }
                    }

                    // aovoid the new_node input_load produce new max input_load
                    int new_node_load = loadTable[min_input_load_color][1] + _ec->_w;
                    if(new_node_load*1.0/(stripe_num+1) > best_avg){
                        if(DEBUG_ENABLE)
                            cout << "stripe" << tail_stripe->getStripeId() <<  " new_node input load is to large, = " << new_node_load << " avg=" << new_node_load*1.0/(stripe_num+1) << endl;
                        continue;
                    }
                    
                    // choose the maximum outputload amoung stripe source_node
                    int max_output_load = 0;
                    int max_output_load_color = -1;
                    vector<int> source_node_ids = tail_stripe->getPlacement();
                    source_node_ids.erase(find(source_node_ids.begin(), source_node_ids.end(), fail_node_id));
                    for(auto it : source_node_ids)
                    {
                        int outLoad = loadTable[it][0] + tail_stripe->getMinOutput(it);
                        if(outLoad > max_output_load)
                        {
                            max_output_load = outLoad;
                            max_output_load_color = it;
                        }
                    }
                    int max_gloabl_output = max_output_load;
                    double avg_output = max_gloabl_output*1.0/(stripe_num+1);
                    cout << "output avg = " << max_gloabl_output << "/" << (stripe_num+1) << "=" << avg_output << endl;
                    if(avg_output > best_avg){
                        if(DEBUG_ENABLE)
                            cout << "soucenode output load is to large, avoid" << endl;
                        continue;
                    }
                    find_stripe = true;
                    stripe = tail_stripe;
                    break;
                }
                if(find_stripe){
                    batch = tail_batch;
                    break;
                }
            }
            if(!find_stripe){
                if(DEBUG_ENABLE)
                    cout << "search all stripe, curr batch size = " << head_batch->getStripeList().size() << endl;
                break;
            }
            gettimeofday(&newnode2, NULL);

            // coloring the solution of choose stripe
            gettimeofday(&time1, NULL);
            batch_stripe.push_back(make_pair(batch,stripe));
            genParallelColoringForSingleFailure(stripe, fail_node_id, scenario, loadTable);

            // calculate average load after insert tail stripe
            int loadafter = evalTable(loadTable)._load;
            int bdwtafter = evalTable(loadTable)._bdwt;
            float avgafter = loadafter*1.0 / (stripe_num + 1);
            gettimeofday(&time2, NULL);
            gettimeofday(&curr_improve, NULL);

            // dump 
            double curr_time = DistUtil::duration(improve_start, curr_improve);
            if(DEBUG_ENABLE){
                cout << "insert stripe " << stripe->getStripeId() << " into batch " << head_batch->getBatchId() << " coloring duration = " << DistUtil::duration(time1, time2) << endl;
                cout << "best avg load = " << evalTable(head_batch->getLoadTable(_agents_num))._load << "/" << head_batch->getStripeList().size() << " = " << best_avg << endl;
                cout << "batch.load = " << head_batch->getLoad() << endl;
                cout << "avg load after = " << loadafter << "/" << stripe_num+1 << " = " << avgafter << endl;
                cout << curr_time/1000 << " " << stripe_num  << " " << loadafter/_ec->_w  << " " << bdwtafter/_ec->_w << " " <<  avgafter << endl; 
            }
            
            if(avgafter > best_avg){
                // cache
                stripeIdx_to_insert.push_back(stripe->getStripeId());
            }else{
                // merge
                for(auto it : batch_stripe)
                {
                    RepairBatch* batch_to_delete = it.first;
                    Stripe* stripe_to_insert = it.second;
                    head_batch->push(stripe_to_insert);
                    batch_to_delete->erase(stripe_to_insert);
                    if(batch_to_delete->getStripeList().size() == 0)
                    {
                        _batch_list.erase(find(_batch_list.begin(), _batch_list.end(), batch_to_delete));
                    }
                }
                batch_stripe.clear();
                stripeIdx_to_insert.clear();
                best_avg = avgafter;
            }
        }

        if(DEBUG_ENABLE)
        {
            cout << "curr batch finish" << endl;
            head_batch->dump();
        }
        
        // _batch_queue.push(head_batch);
        ret.push_back(head_batch);
        _batch_list.erase(_batch_list.begin());

        gettimeofday(&improve2, NULL);

        if(DEBUG_ENABLE){
            cout << "Improve batch" << head_batch->getBatchId() << " for " << DistUtil::duration(improve1, improve2) << endl;
            int avgload = head_batch->getLoad() / head_batch->getStripeList().size();
            cout << "avg load = " << head_batch->getLoad() << "/" << head_batch->getStripeList().size()  << " = " << avgload << endl;
        }
    }
    _batch_list = ret;
    for(auto it : _batch_list){
        cout << "batch" << it->getBatchId() << " load = " << it->getLoad() <<endl;
    }
    return;
}

void ParallelSolution::improve_enqueue(int fail_node_id, string scenario){

    struct timeval time1, time2;

    // 1. color all batchs
    gettimeofday(&time1, NULL);
    for(int i = 0; i < _batch_list.size(); i++)
    {
        RepairBatch* batch = _batch_list[i];
        vector<vector<int>> loadTable(_agents_num, {0,0}); // {out,in}
        
        // gen placement
        vector<int> placement;
        for(auto it : batch->getStripeList())
        {
            for(auto idx : it->getPlacement())
            {
                if(find(placement.begin(), placement.end(), idx) == placement.end())
                    placement.push_back(idx);
            }
        }

        // gen head batch coloring solution
        vector<Stripe*> full_stripe_list = batch->getStripeList();   
        for (int i=0; i<full_stripe_list.size(); i++) {
            Stripe* stripe = full_stripe_list[i];
            ECDAG* ecdag = stripe->genRepairECDAG(_ec, fail_node_id);
            unordered_map<int, int> coloring;
            // genColoringForSingleFailure(stripe, coloring, fail_node_id, _agents_num, scenario, placement);
            genOfflineColoringForSingleFailure(stripe, fail_node_id, scenario, loadTable, placement);  
        }
        // record information
        batch->evaluateBatch(_agents_num);
    }
    gettimeofday(&time2, NULL);
    double color_all_batch = DistUtil::duration(time1, time2);

    struct timeval improve_start, curr_improve;
    gettimeofday(&improve_start, NULL);
    
    // 2. insert stripe into batch
    while(true)
    {
        
        // check if has merged all batch
        if(_batch_list.size() == 0)
            break;
        sort(_batch_list.begin(), _batch_list.end(), [](RepairBatch* batch1, RepairBatch* batch2){
            return batch1->getLoad()/batch1->getStripeList().size() < batch2->getLoad()/batch2->getStripeList().size();
        });
        RepairBatch* head_batch = _batch_list[0];
        int tail_batch_idx = _batch_list.size()-1;
        int stripe_num = head_batch->getStripeList().size();
        float best_avg = evalTable(head_batch->getLoadTable(_agents_num))._load*1.0/stripe_num;
        
        vector<pair<RepairBatch*, Stripe*>> batch_stripe;
        vector<int> stripeIdx_to_insert;
        vector<vector<int>> loadTable = head_batch->getLoadTable(_agents_num); 

        // improve curr batch
        struct timeval improve1, improve2;
        gettimeofday(&improve1, NULL);

        while(true)
        {
            if(_batch_request){
            break;
            }
            // insert one stripe into one head_batch
            Stripe* stripe;
            RepairBatch* batch;
            bool find_stripe = false; // find a candidate
            stripe_num = head_batch->getStripeList().size() + batch_stripe.size();

            // select a candidate stripe to insert
            struct timeval newnode1, newnode2;
            gettimeofday(&newnode1, NULL);
            for(int i = tail_batch_idx; i > 0; i--)
            {
                RepairBatch* tail_batch = _batch_list[i];
                for(auto tail_stripe : tail_batch->getStripeList())
                {
                    if(find(stripeIdx_to_insert.begin(), stripeIdx_to_insert.end(), tail_stripe->getStripeId()) != stripeIdx_to_insert.end())
                        continue;

                    // remove source nodes that we should avoid 
                    vector<int> avoid_node_ids = tail_stripe->getPlacement();
                    vector<int> candidates;
                    for (int i=0; i<_agents_num; i++) {
                        if (find(avoid_node_ids.begin(), avoid_node_ids.end(), i) == avoid_node_ids.end())
                            candidates.push_back(i);
                    }

                    // choose the minimum inputload apart from source node 
                    int min_input_load = INT_MAX;
                    int min_input_load_color = -1;
                    for(auto it : candidates)
                    {
                        int inload = loadTable[it][1];
                        if(inload < min_input_load)
                        {
                            min_input_load = inload;
                            min_input_load_color = it;
                        }
                    }

                    // aovoid the new_node input_load produce new max input_load
                    int new_node_load = loadTable[min_input_load_color][1] + _ec->_w;
                    if(new_node_load*1.0/(stripe_num+1) > best_avg){
                        if(DEBUG_ENABLE)
                            cout << "stripe" << tail_stripe->getStripeId() <<  " new_node input load is to large, = " << new_node_load << " avg=" << new_node_load*1.0/(stripe_num+1) << endl;
                        continue;
                    }
                    
                    // choose the maximum outputload amoung stripe source_node
                    int max_output_load = 0;
                    int max_output_load_color = -1;
                    vector<int> source_node_ids = tail_stripe->getPlacement();
                    source_node_ids.erase(find(source_node_ids.begin(), source_node_ids.end(), fail_node_id));
                    for(auto it : source_node_ids)
                    {
                        int outLoad = loadTable[it][0] + tail_stripe->getMinOutput(it);
                        if(outLoad > max_output_load)
                        {
                            max_output_load = outLoad;
                            max_output_load_color = it;
                        }
                    }
                    int max_gloabl_output = max_output_load;
                    double avg_output = max_gloabl_output*1.0/(stripe_num+1);
                    cout << "output avg = " << max_gloabl_output << "/" << (stripe_num+1) << "=" << avg_output << endl;
                    if(avg_output > best_avg){
                        if(DEBUG_ENABLE)
                            cout << "soucenode output load is to large, avoid" << endl;
                        continue;
                    }
                    find_stripe = true;
                    stripe = tail_stripe;
                    break;
                }
                if(find_stripe){
                    batch = tail_batch;
                    break;
                }
            }
            if(!find_stripe){
                if(DEBUG_ENABLE)
                    cout << "search all stripe, curr batch size = " << head_batch->getStripeList().size() << endl;
                break;
            }
            gettimeofday(&newnode2, NULL);

            // coloring the solution of choose stripe
            gettimeofday(&time1, NULL);
            batch_stripe.push_back(make_pair(batch,stripe));
            genParallelColoringForSingleFailure(stripe, fail_node_id, scenario, loadTable);

            // calculate average load after insert tail stripe
            int loadafter = evalTable(loadTable)._load;
            int bdwtafter = evalTable(loadTable)._bdwt;
            float avgafter = loadafter*1.0 / (stripe_num + 1);
            gettimeofday(&time2, NULL);
            gettimeofday(&curr_improve, NULL);

            // dump 
            double curr_time = DistUtil::duration(improve_start, curr_improve);
            if(DEBUG_ENABLE){
                cout << "insert stripe " << stripe->getStripeId() << " into batch " << head_batch->getBatchId() << " coloring duration = " << DistUtil::duration(time1, time2) << endl;
                cout << "best avg load = " << evalTable(head_batch->getLoadTable(_agents_num))._load << "/" << head_batch->getStripeList().size() << " = " << best_avg << endl;
                cout << "batch.load = " << head_batch->getLoad() << endl;
                cout << "avg load after = " << loadafter << "/" << stripe_num+1 << " = " << avgafter << endl;
                cout << curr_time/1000 << " " << stripe_num  << " " << loadafter/_ec->_w  << " " << bdwtafter/_ec->_w << " " <<  avgafter << endl; 
            }
            
            if(avgafter > best_avg){
                // cache
                stripeIdx_to_insert.push_back(stripe->getStripeId());
            }else{
                // merge
                for(auto it : batch_stripe)
                {
                    RepairBatch* batch_to_delete = it.first;
                    Stripe* stripe_to_insert = it.second;
                    head_batch->push(stripe_to_insert);
                    batch_to_delete->erase(stripe_to_insert);
                    if(batch_to_delete->getStripeList().size() == 0)
                    {
                        _batch_list.erase(find(_batch_list.begin(), _batch_list.end(), batch_to_delete));
                    }
                }
                batch_stripe.clear();
                stripeIdx_to_insert.clear();
                best_avg = avgafter;
            }
        }

        if(DEBUG_ENABLE)
        {
            cout << "curr batch finish" << endl;
            head_batch->dump();
        }
        
        _batch_queue.push(head_batch);
        _batch_list.erase(_batch_list.begin());

        _lock.lock();
        _batch_request = false;
        _lock.unlock();

        gettimeofday(&improve2, NULL);

        if(DEBUG_ENABLE){
            cout << "Improve batch" << head_batch->getBatchId() << " for " << DistUtil::duration(improve1, improve2) << endl;
            int avgload = head_batch->getLoad() / head_batch->getStripeList().size();
            cout << "avg load = " << head_batch->getLoad() << "/" << head_batch->getStripeList().size()  << " = " << avgload << endl;
        }
    }
    return;
}

void ParallelSolution::improve_hybrid(int fail_node_id, string scenario){
    if(DEBUG_ENABLE)
        cout << "ParallelSolution::improve_hybrid" << endl;
    // init
    int debug_break;
    struct timeval time1, time2, time3, time4, time5, time6;
    
    // 1. color all batchs
    gettimeofday(&time1, NULL);
    for(int i = 0; i < _batch_list.size(); i++)
    {
        // cout << "[DEBUG] coloring batch: " << i << "/" << _batch_list.size() << endl;
        RepairBatch* batch = _batch_list[i];
        vector<vector<int>> loadTable(_agents_num, {0,0}); // {out,in}
        
        // gen placement
        vector<int> placement;
        for(auto it : batch->getStripeList())
        {
            for(auto idx : it->getPlacement())
            {
                if(find(placement.begin(), placement.end(), idx) == placement.end())
                    placement.push_back(idx);
            }
        }

        // gen head batch coloring solution
        vector<Stripe*> full_stripe_list = batch->getStripeList();   
        for (int i=0; i<full_stripe_list.size(); i++) {
            Stripe* stripe = full_stripe_list[i];
            ECDAG* ecdag = stripe->genRepairECDAG(_ec, fail_node_id);
            // genColoringForSingleFailure(stripe, coloring, fail_node_id, _agents_num, scenario, placement);
            genOfflineColoringForSingleFailure(stripe, fail_node_id, scenario, loadTable, placement);  
        }

        // record information
        batch->evaluateBatch(_agents_num);
    }
    gettimeofday(&time2, NULL);
    double color_all_batch = DistUtil::duration(time1, time2);

    // 2. insert stripe into batch
    while(true)
    {
        // check if has merged all batch
        if(_batch_list.size() == 0)
            break;
        sort(_batch_list.begin(), _batch_list.end(), [](RepairBatch* batch1, RepairBatch* batch2){
            return batch1->getLoad()/batch1->getStripeList().size() < batch2->getLoad()/batch2->getStripeList().size();
        });

        RepairBatch* head_batch = _batch_list[0];
        if(DEBUG_ENABLE)
            cout << endl << "imporove for batch" << head_batch->getBatchId() << endl;
        int tail_batch_idx = _batch_list.size()-1;
        int stripe_num = head_batch->getStripeList().size();
        float best_avg = evalTable(head_batch->getLoadTable(_agents_num))._load*1.0/stripe_num;
        
        vector<pair<RepairBatch*, Stripe*>> batch_stripe;
        vector<int> stripeIdx_to_insert;
        vector<vector<int>> loadTable = head_batch->getLoadTable(_agents_num); // contain the cache stripe load

        // improve curr batch until the signal turn true
        struct timeval improve1, improve2;
        gettimeofday(&improve1, NULL);

        bool offline = false;
        while(true)
        {
            if(_batch_request && batch_stripe.size() + head_batch->getStripeList().size() >= _batch_size / 2){
                break;
            }
            // insert one stripe into one head_batch
            Stripe* stripe;
            RepairBatch* batch;
            bool find_stripe = false; // find a candidate
            
            // 1. select a candidate stripe to insert
            int debug_pass_input = 0, debug_pass_output = 0;
            gettimeofday(&time3, NULL);
            for(int i = tail_batch_idx; i > 0; i--)
            {
                RepairBatch* tail_batch = _batch_list[i];
                for(auto tail_stripe : tail_batch->getStripeList())
                {
                    if(find(stripeIdx_to_insert.begin(), stripeIdx_to_insert.end(), tail_stripe->getStripeId()) != stripeIdx_to_insert.end())
                        continue;

                    // remove source nodes that we should avoid 
                    vector<int> avoid_node_ids = tail_stripe->getPlacement();
                    cout << "source node" << endl;
                    for(auto it : avoid_node_ids)
                    {
                        cout << " " << it;
                    }
                    cout << endl;
                    dumpTable(loadTable);
                    vector<int> candidates;
                    for (int i=0; i<_agents_num; i++) {
                        if (find(avoid_node_ids.begin(), avoid_node_ids.end(), i) == avoid_node_ids.end())
                            candidates.push_back(i);
                    }

                    // choose the minimum inputload apart from source node 
                    int min_load = INT_MAX;
                    int min_load_color = -1;
                    for(auto it : candidates)
                    {
                        int inload = loadTable[it][1];
                        if(inload < min_load)
                        {
                            min_load = inload;
                            min_load_color = it;
                        }
                    }

                    // aovoid the new_node input_load produce new max input_load
                    int new_node_load = loadTable[min_load_color][1] + _ec->_w;
                    if(new_node_load*1.0/(stripe_num+1) > best_avg){
                        if(DEBUG_ENABLE)
                            cout << "new_node input load is to large pass" << endl;
                        debug_pass_input++;
                        continue;
                    }
                    
                    // choose the maximum outputload amoung stripe source_node
                    int max_output = 0;
                    int max_output_color = -1;
                    vector<vector<int>> tempTable = deepCopyTable(loadTable);
                    for(auto leaf : tail_stripe->getECDAG()->getECLeaves()){
                        int blkid = leaf/_ec->_w;
                        int nodeid = tail_stripe->getPlacement()[blkid];
                        if(blkid < _ec->_n){
                            tempTable[nodeid][0]++;
                        }
                    }
                    cout << "debug output load" << endl;
                    dumpTable(tempTable);
                    
                    for(int i = 0; i < tempTable.size(); i++)
                    {
                        int outLoad = tempTable[i][0];
                        if(outLoad > max_output)
                        {
                            max_output = outLoad;
                            max_output_color = i;
                        }
                    }
                    float avg_output = max_output*1.0/(stripe_num+1);
                    if(avg_output > best_avg){
                        cout << "source node output load is to large" << max_output << "/" << stripe_num+1 << "=" << avg_output << ", pass" << endl;
                        debug_pass_output++;
                        continue;
                    }
                    find_stripe = true;
                    stripe = tail_stripe;
                    break;
                }
                if(find_stripe){
                    batch = tail_batch;
                    break;
                }
            }
            if(!find_stripe){ // search all stripe
                if(DEBUG_ENABLE)
                    cout << "search all stripe, curr batch size = " << head_batch->getStripeList().size() << endl;
                break;
            }
            gettimeofday(&time4, NULL);
            double select_candidate = DistUtil::duration(time3, time4);
            // 2. try coloring the solution
            gettimeofday(&time5, NULL);
            batch_stripe.push_back(make_pair(batch,stripe));

            vector<int> placement;

            if(batch_stripe.size() + head_batch->getStripeList().size() < _batch_size / 2){
                genOfflineColoringForSingleFailure(stripe,fail_node_id, scenario, loadTable, placement);
            }else{
                genParallelColoringForSingleFailure(stripe, fail_node_id, scenario, loadTable);
            }
            
            // calculate average load after insert tail stripe
            int loadafter = evalTable(loadTable)._load;
            float avgafter = loadafter*1.0 / (stripe_num + 1);
            gettimeofday(&time6, NULL);
            double try_merge = DistUtil::duration(time5,time6);

            if(DEBUG_ENABLE)
            {
                cout << "select candidate duration: " << select_candidate 
                    << " , and pass intput:" << debug_pass_input << ", pass output:" << debug_pass_output << endl;
                cout << "insert stripe " << stripe->getStripeId() << " into batch " << head_batch->getBatchId() 
                    << "  duration: " << try_merge << endl;
                cout << "best avg load = " << evalTable(head_batch->getLoadTable(_agents_num))._load << "/" << head_batch->getStripeList().size() << " = " << best_avg << endl;
                cout << "batch.load = " << head_batch->getLoad() << endl;
                cout << "avg load after = " << loadafter << "/" << stripe_num+1 << " = " << avgafter << endl;
            }
            // 3. cache or perform merge
            stripe_num++; 
            if(avgafter > best_avg){
                // cache
                stripeIdx_to_insert.push_back(stripe->getStripeId());
            }else{
                // perform merge
                if(DEBUG_ENABLE)
                    cout << "perform insert " << batch_stripe.size() << " stripes" << endl;
                for(auto it : batch_stripe)
                {
                    RepairBatch* batch_to_delete = it.first;
                    Stripe* stripe_to_insert = it.second;
                    head_batch->push(stripe_to_insert);
                    batch_to_delete->erase(stripe_to_insert);
                    if(batch_to_delete->getStripeList().size() == 0)
                    {
                        _batch_list.erase(find(_batch_list.begin(), _batch_list.end(), batch_to_delete));
                    }
                }
                batch_stripe.clear();
                stripeIdx_to_insert.clear();
                best_avg = avgafter;
            }
        }
        gettimeofday(&improve2, NULL);

        _batch_queue.push(head_batch);
        _batch_list.erase(_batch_list.begin());

        _lock.lock();
        _batch_request = false;
        _lock.unlock();


        // dump
        if(DEBUG_ENABLE){
            cout << "Improve batch" << head_batch->getBatchId() << " for " << DistUtil::duration(improve1, improve2) << endl;
            float avgload = head_batch->getLoad() * 1.0 / head_batch->getStripeList().size();
            cout << "avg load = " << head_batch->getLoad() << "/" << head_batch->getStripeList().size()  << " = " << avgload << endl;
        }
    }
    return;
}

void ParallelSolution::improve_hungary(int fail_node_id, string scenario){
    // init
    int debug_break;
    struct timeval time1, time2;

    bool signal = 0;

    int min_output_add = 0;
    if(_codename == "Clay")
    {
        // for single clay code
        int q = _ec->_n - _ec->_k;
        int t = _ec->_n / q;
        min_output_add = pow(q,t-1);
    }
    // cout << "debug gamma = " << min_output_add;

    // 1. color all batchs
    gettimeofday(&time1, NULL);
    for(int i = 0; i < _batch_list.size(); i++)
    {
        // cout << "[DEBUG] coloring batch: " << i << "/" << _batch_list.size() << endl;
        RepairBatch* batch = _batch_list[i];
        vector<vector<int>> loadTable(_agents_num, {0,0}); // {out,in}
        
        // gen placement
        vector<int> placement;
        for(auto it : batch->getStripeList())
        {
            for(auto idx : it->getPlacement())
            {
                if(find(placement.begin(), placement.end(), idx) == placement.end())
                    placement.push_back(idx);
            }
        }

        // gen head batch coloring solution
        vector<Stripe*> full_stripe_list = batch->getStripeList();   
        for (int i=0; i<full_stripe_list.size(); i++) {
            Stripe* stripe = full_stripe_list[i];
            ECDAG* ecdag = stripe->genRepairECDAG(_ec, fail_node_id);
            // genColoringForSingleFailure(stripe, coloring, fail_node_id, _agents_num, scenario, placement);
            genOfflineColoringForSingleFailure(stripe, fail_node_id, scenario, loadTable, placement);  
        }
        // record information
        batch->evaluateBatch(_agents_num);
    }
    gettimeofday(&time2, NULL);
    double color_all_batch = DistUtil::duration(time1, time2);

    // 2. insert stripe into batch
    while(true)
    {
        // check if has merged all batch
        if(_batch_list.size() == 0)
            break;
        // sort(_batch_list.begin(), _batch_list.end(), [](RepairBatch* batch1, RepairBatch* batch2){
        //     return batch1->getLoad()/batch1->getStripeList().size() < batch2->getLoad()/batch2->getStripeList().size();
        // });
        RepairBatch* head_batch = _batch_list[0];
        int tail_batch_idx = _batch_list.size()-1;
        int stripe_num = head_batch->getStripeList().size();
        float best_avg = evalTable(head_batch->getLoadTable(_agents_num))._load*1.0/stripe_num;
        
        vector<pair<RepairBatch*, Stripe*>> batch_stripe;
        vector<int> stripeIdx_to_insert;
        vector<vector<int>> loadTable = head_batch->getLoadTable(_agents_num); // contain the cache stripe load

        // improve curr batch until the signal turn true
        struct timeval improve1, improve2;
        gettimeofday(&improve1, NULL);

        _batch_queue.push(head_batch);
        _batch_list.erase(_batch_list.begin());

        _lock.lock();
        _batch_request = false;
        _lock.unlock();

        gettimeofday(&improve2, NULL);


        // dump
        cout << "Improve batch" << head_batch->getBatchId() << " for " << DistUtil::duration(improve1, improve2) << endl;
        int avgload = head_batch->getLoad() / head_batch->getStripeList().size();
        cout << "avg load = " << head_batch->getLoad() << "/" << head_batch->getStripeList().size()  << " = " << avgload << endl;
    }
    return;
}

State ParallelSolution::evalTable(const vector<vector<int>> & table)
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

State ParallelSolution::evalTable(vector<vector<int>> table, vector<int> colors)
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

vector<int> genBitVec(vector<int> locList, int clusterSize)
{
    vector<int> bitVec(clusterSize, 0);
    for(auto it : locList)
    {
        bitVec[it] = 1;
    }
    return bitVec;
}

Stripe* ParallelSolution::choose(vector<bool> flags, vector<Stripe*> _stripe_list)
{
    Stripe* ret = nullptr;
    vector<int> bitVec(_agents_num, 0);
    for(auto it : _stripe_list){
        for(auto nodeId : it->getPlacement()){
            bitVec[nodeId] = 1;
        }
    }
    auto genBitVec = [&](Stripe* currstripe){
        vector<int> demobitVec(_agents_num,0);
        for(auto nodeId: currstripe->getPlacement()){
            demobitVec[nodeId] = 1;
        }
        return demobitVec;
    };

    auto OR = [&](vector<int> vec_a, vector<int> vec_b){
        int ret = 0;
        for(int i = 0; i < vec_a.size(); i++)
        {
            if(vec_a[i] != 0 || vec_b[i] != 0)
            {
                ret++;
            }
        }
        return ret;
    };
    
    int max_num_cover = OR(bitVec, vector<int>(_agents_num,0));
    for(int i = _batch_list.size()-1; i >= 0; i--)
    {
        RepairBatch* currBatch = _batch_list[i];
        vector<Stripe*> stripe_list = currBatch->getStripeList();
        for(int j = 0; j < stripe_list.size(); j++)
        {
            Stripe* stripe = stripe_list[j];
            if(flags[stripe->getStripeId()])
            {
                // cout << "has choosen" << endl;
                continue;
            }
                
            vector<int> currBitVec = genBitVec(stripe);
            int orVal = OR(bitVec, currBitVec);
            if(orVal > max_num_cover){
                max_num_cover = orVal;
                ret = stripe;
            }
        }
    }
    if(!ret) return ret;
    cout << "choose " << ret->getStripeId() << endl;

    
    cout << "debug choose" << endl;
    for(auto it : bitVec)
    {
        // cout << " " << it;
    }
    // cout << endl;
    auto retBitVec = genBitVec(ret);
    for(auto it : retBitVec){
        // cout << " " << it;
    }
    // cout << endl;
    for(int i = 0; i < _agents_num; i++)
    {
        // cout << " " << bitVec[i] + retBitVec[i];
    }
    // cout << endl;

    return ret;
}

void ParallelSolution::genRepairBatchesForSingleFailure(int fail_node_id, string scenario) {

    if(DEBUG_ENABLE)
        cout << "ParallelSolution::genRepairBatchesForSingleFailure.fail_node_id = " << fail_node_id << endl;

    // 0. we first figure out stripes that stores a block in $fail_node_id
    filterFailedStripes({fail_node_id});
    vector<Stripe*> stripes_to_repair_vec;
    for(auto idx : _stripes_to_repair){
        stripes_to_repair_vec.push_back(_stripe_list[idx]);
    }

    if(DEBUG_ENABLE)
        cout << "ParallelSolution::genRepairBatchesForSingleFailure.stripes to repair: " << _stripes_to_repair.size() << endl;
   
    // 1. We first perform Hungarian algorithm to divide stripes into batches
    // we also sort batches based on the number of stripes in a batch in descending order
    struct timeval time1, time2, time3;

    gettimeofday(&time1, NULL);
    _batch_list = findRepairBatchs(fail_node_id, scenario);
    sort(_batch_list.begin(),_batch_list.end(),[](RepairBatch* batch1, RepairBatch* batch2){
        return batch1->getStripeList().size() > batch2->getStripeList().size();
    });

    if(DEBUG_ENABLE){
        cout << "Hungary result" << endl;
        for(auto batch: _batch_list)
        {
            cout << "batch:" << batch->getBatchId();
            for(auto stripe: batch->getStripeList())
            {
                cout << " " << stripe->getStripeId();
            }
            cout << endl;
        }
    }
    int hungary_num = _batch_list.size();

    // 2. We try to insert stripes of latter batches into previous batches to improve the repair efficiency
    gettimeofday(&time2, NULL);
    if(_enqueue){
        // use blocking queue and signal for online scenario
        improve_enqueue(fail_node_id, scenario);
    }else{
        // generate batch list for offline scenario
        improve(fail_node_id, scenario);
    }
    gettimeofday(&time3, NULL);

    if(DEBUG_ENABLE){
        cout << "[DEBUG] hungary_num = " << hungary_num <<endl;
        cout << "duration find batch = " << DistUtil::duration(time1,time2)  << endl;
        cout << "duration merge batch = " << DistUtil::duration(time2,time3) << endl;
    }
}
