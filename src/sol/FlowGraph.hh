#ifndef _FLOWGRAPH_HH_
#define _FLOWGRAPH_HH_

#include "../inc/include.hh"
#include "../common/Stripe.hh"
#include <queue>
#include <climits>

using namespace std;

/* 
 * Case 1:
 *   We construct a FlowGraph of p stripes connected to q storage nodes
 *   Vertex:
 *       sink vertex (s): 0
 *       stripe vertex (sv): 1, 2, ..., p
 *       storage node vertex (nv): p+1, p+2, ..., p+q
 *       t vertex (t): p+q+1
 *   Edge:
 *       s-sv: the capacity of an edge between s and a sv is t, t is configured
 *       with d=n-1 for a (n,k) MSR code
 *       sv-nv: the capacity of an edge between sv and nv is 1
 *       nv-t: the capacity of an edge between nv and t is d=n-1 for a (n,k) MSR
 *       code
 *
 * Case 2:
 *   We construct a FlowGraph of p stripes connected to q storage nodes
 *   Vertex:
 *      sink vertex (s): 0
 *      stripe vertex (sv): 1, 2, ..., p
 *      storage node vertex (nv): p+1, p+2, ..., p+q
 *      t vertex (t): p+q+1
 *   Edge
 *      s-sv: the capacity of an edge between s and a sv is d=1
 *      sv-nv: the capacity of an edge between sv and nv is 1
 *      nv-t: the capacity of an edge between a nv and t is d=1
 */
class FlowGraph {
    private:
        int* _flow_graph;
        int* _res_matrix;

        int _p, _q, _d, _failnid, _vnum;
        int _max_flow;

        vector<int> _stripe_id_list;
        vector<int> _node_id_list;

        unordered_map<int, int> sid2vid;
        unordered_map<int, int> vid2sid;

        unordered_map<int, int> nid2vid;
        unordered_map<int, int> vid2nid;

        void dumpMatrix(int* matrix, int vnum);
        bool bfs(int* resgraph, int vnum, int s, int t, int* parent);

    public:
        // for finding stripes in a batch
        FlowGraph(unordered_map<int, Stripe*> stripemap, int d, int failnid);
        // for finding repair node for each block in a batch
        FlowGraph(vector<Stripe*> stripelist, int failnid, int agentnum);
        int findMaxFlow();
        bool isSaturated();
        int getStripeIdWithMinFlow();
        int getFlowForNode(int nodeid);
        int getVidFromNode(int nodeid);
        int getFullNodeNumForStripe(Stripe* stripe, int capacity, int failnodeid);
        vector<int> chooseStripes(int capacity);
        unordered_map<int, int> getRepairNodes();
        ~FlowGraph();
};

#endif

