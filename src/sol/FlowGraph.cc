#include "FlowGraph.hh"

// source
FlowGraph::FlowGraph(unordered_map<int, Stripe*> stripemap, int d, int failnid) {

    _p = stripemap.size();
    _d = d;
    _failnid = failnid;

    for (auto item: stripemap) {
        int stripeid = item.first;
        _stripe_id_list.push_back(stripeid);
        Stripe* curstripe = item.second;
        vector<int> nidlist = curstripe->getPlacement();

        for (auto nid: nidlist) {
            if (nid == failnid)
                continue;
            if (find(_node_id_list.begin(), _node_id_list.end(), nid) == _node_id_list.end())
                _node_id_list.push_back(nid);
        }
    }

    _q = _node_id_list.size();

    sort(_stripe_id_list.begin(), _stripe_id_list.end());
    sort(_node_id_list.begin(), _node_id_list.end());

    // construct the mapping between stripeid and vid
    int start = 1;
    for (int i=0; i<_stripe_id_list.size(); i++) {
        int sid = _stripe_id_list[i];
        int vid = start + i;
        sid2vid.insert(make_pair(sid, vid));
        vid2sid.insert(make_pair(vid, sid));
    }

    // construct the mapping between nodeid and vid
    start = _p+1;
    for (int i=0; i<_node_id_list.size(); i++) {
        int nid = _node_id_list[i];
        int vid = start + i;
        nid2vid.insert(make_pair(nid, vid));
        vid2nid.insert(make_pair(vid, nid));
    }

    // construct flowgraph
    _vnum = _p+_q+2;
    int size = _vnum * _vnum;
    _flow_graph = (int*)calloc(size, sizeof(int));
    _res_matrix = (int*)calloc(size, sizeof(int));
    memset(_flow_graph, 0, size*sizeof(int));

    for (int i=0; i<_stripe_id_list.size(); i++) {
        int stripeid = _stripe_id_list[i];
        Stripe* curstripe = stripemap[stripeid];
        int vsid = sid2vid[stripeid];

        // set the capacity between s and sv to d 
        _flow_graph[vsid] = _d;

        // set the capacity between sv and nv to 1
        for (int nid: curstripe->getPlacement()) {
            if (nid == failnid)
                continue;
            int vnid = nid2vid[nid];
            _flow_graph[vsid*(_p+_q+2)+vnid] = 1;
        }
    }

    // set the capacity between nv to t to d
    for (int i=0; i<_node_id_list.size(); i++) {
        int nid = _node_id_list[i];
        int vid = nid2vid[nid];

        _flow_graph[vid*(_p+_q+2)+_p+_q+1] = _d;
    }
    memcpy(_res_matrix, _flow_graph, _vnum * _vnum * sizeof(int));
}

// replacement
FlowGraph::FlowGraph(vector<Stripe*> stripelist, int failnid, int agentnum) {

    _p = stripelist.size();
    _d = 1;
    _failnid = failnid;
    unordered_map<int, vector<int>> candidatemap;

    for (Stripe* curstripe: stripelist) {
        int stripeid = curstripe->getStripeId();
        _stripe_id_list.push_back(stripeid);
        vector<int> nidlist = curstripe->getPlacement();
        vector<int> placelist;

        for (int i=0; i<agentnum; i++) {
            if (find(nidlist.begin(), nidlist.end(), i) == nidlist.end()) {
                // i is a candidate for stripeid
                placelist.push_back(i);
                if (find(_node_id_list.begin(), _node_id_list.end(), i) == _node_id_list.end())
                    _node_id_list.push_back(i);
            }
        }

        candidatemap.insert(make_pair(stripeid, placelist));
    }

    _q = _node_id_list.size();
    sort(_stripe_id_list.begin(), _stripe_id_list.end());
    sort(_node_id_list.begin(), _node_id_list.end());

    // construct the mapping between stripeid and vid
    int start = 1;
    for (int i=0; i<_stripe_id_list.size(); i++) {
        int sid = _stripe_id_list[i];
        int vid = start + i;
        sid2vid.insert(make_pair(sid, vid));
        vid2sid.insert(make_pair(vid, sid));
    }

    // construct the mapping between nodeid and vid
    start = _p+1;
    for (int i=0; i<_node_id_list.size(); i++) {
        int nid = _node_id_list[i];
        int vid = start + i;
        nid2vid.insert(make_pair(nid, vid));
        vid2nid.insert(make_pair(vid, nid));
    }

    // construct flowgraph
    _vnum = _p+_q+2;
    int size = _vnum * _vnum;
    _flow_graph = (int*)calloc(size, sizeof(int));
    _res_matrix = (int*)calloc(size, sizeof(int));
    memset(_flow_graph, 0, size*sizeof(int));

    for (Stripe* curstripe: stripelist) {
        int stripeid = curstripe->getStripeId();
        int vsid = sid2vid[stripeid];

        // set the capacity between s and sv to 1 
        _flow_graph[vsid] = 1;

        // set the capacity between sv and nv to 1
        for (int nid: candidatemap[stripeid]) {
            int vnid = nid2vid[nid];
            _flow_graph[vsid*(_p+_q+2)+vnid] = 1;
        }
    }

    // set the capacity between nv to t to 1
    for (int i=0; i<_node_id_list.size(); i++) {
        int nid = _node_id_list[i];
        int vid = nid2vid[nid];

        _flow_graph[vid*(_p+_q+2)+_p+_q+1] = 1;
    }
    memcpy(_res_matrix, _flow_graph, _vnum * _vnum * sizeof(int));

    //cout << "_flow_graph:" << endl;
    //dumpMatrix(_flow_graph, _p+_q+2);
    //cout << "_res_matrix: " << endl;
    //dumpMatrix(_res_matrix, _vnum);
}

int FlowGraph::findMaxFlow() {

    int u, v;

    // Create a residual graph and fill the residual graph
    // with given capacities in the original graph as
    // residual capacities in residual graph

    //cout << "_res_matrix:" << endl;
    //dumpMatrix(_res_matrix, _vnum);

    // This array is filled by BFS and to store path
    int parent[_vnum];
    int max_flow = 0;

    // Augment the flow while there is path from source to
    // sink

    int s=0, t=_vnum-1;
    while(bfs(_res_matrix, _vnum, s, t, parent)) {

        // Find minimum residual capacity of the edges along
        // the path filled by BFS. Or we can say find the
        // maximum flow through the path found.

        int path_flow = INT_MAX;
        for (v = t; v!=s; v = parent[v]) {
            u = parent[v];
            path_flow = min(path_flow, _res_matrix[u*_vnum+v]);
        }

        // update residual capacities of the edges and
        // reverse edges along the path

        for (v = t; v != s; v = parent[v]) {
            u = parent[v];
            _res_matrix[u*_vnum+v] -= path_flow;
            _res_matrix[v*_vnum+u] += path_flow;
        }

        // Add path flow to overall flow
        max_flow += path_flow;
    }
    _max_flow = max_flow;

    return max_flow;
}

bool FlowGraph::bfs(int* resgraph, int vnum, int s, int t, int* parent) {
    bool find = false;
    // Create a visited array and mark all vertices as not
    // visited

    int* visited = (int*)calloc(vnum, sizeof(int));
    memset(visited, 0, vnum*sizeof(int));

    // Create a queue, enqueue source vertex and mark source
    // vertex as visited

    queue<int> q;
    q.push(s);

    visited[s] = 1;
    parent[s] = -1;

    // Standard BFS Loop
    while (!q.empty()) { 
        int u = q.front();
        q.pop();

        for (int v = 0; v < vnum; v++) {
            if (resgraph[u*vnum+v] > 0 && visited[v] == 0) {
                // If we find a connection to the sink node,
                // then there is no point in BFS anymore We
                // just have to set its parent and can return
                // true
                if (v == t) {
                    parent[v] = u;
                    find = true;
                    break;
                }
                q.push(v);
                parent[v] = u;
                visited[v] = true;
            }
        }
        if (find)
            break;
    }
    free(visited);
    return find;
}

void FlowGraph::dumpMatrix(int* matrix, int vnum) {
    for (int i=0; i<vnum; i++) {
        for (int j=0; j<vnum; j++) {
            cout << matrix[i*vnum+j] << " ";
        }
        cout << endl;
    }
}

bool FlowGraph::isSaturated() {
    if (_max_flow == _stripe_id_list.size() * _d)
        return true;
    else
        return false;
}

int FlowGraph::getStripeIdWithMinFlow() {
    int max=-1;
    int id=-1;
    for (int i=0; i<_p; i++) {
        int vid = 1+i;
        if (_res_matrix[vid] > max) {
            max = _res_matrix[vid];
            id = vid;
        }
    }
    return vid2sid[id];
}

int FlowGraph::getFlowForNode(int nodeid) {
    int vid = nid2vid[nodeid];
    int f = _res_matrix[(_vnum-1)*_vnum+vid];
    return f;
}

int FlowGraph::getVidFromNode(int nodeid) {
    return nid2vid[nodeid];
}


int FlowGraph::getFullNodeNumForStripe(Stripe* stripe, int capacity, int fail_node_id) {
    int toret = 0;
    for (auto nid: stripe->getPlacement()) {
        if (nid == fail_node_id)
            continue;
        int vid = nid2vid[nid];
        int f = _res_matrix[(_vnum-1)*_vnum+vid];
        if (f == capacity)
            toret++;
    }
    return toret;
}

vector<int> FlowGraph::chooseStripes(int capacity) {

    vector<int> toret;
    for (int vid=1; vid<= _p; vid++) {
        int used = _res_matrix[vid * _vnum];
        int stripeid = vid2sid[vid];
        if (used == capacity) {
            toret.push_back(stripeid);
        }
    }

    return toret;
}

unordered_map<int, int> FlowGraph::getRepairNodes() {
    unordered_map<int, int> toret;

    for (int svid=1; svid<=_p; svid++) {
        for (int nvid=_p+1; nvid<=_p+_q; nvid++) {
            if (_flow_graph[svid*_vnum+nvid] == 1 && _res_matrix[svid*_vnum+nvid] == 0) {
                int sid = vid2sid[svid];
                int nid = vid2nid[nvid];

                toret.insert(make_pair(sid, nid));
            }
        }
    }

    return toret;
}

FlowGraph::~FlowGraph() {
    free(_flow_graph);
    free(_res_matrix);
}
