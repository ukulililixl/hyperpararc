#include "common/Config.hh"
#include "common/Stripe.hh"
#include "common/StripeStore.hh"
#include "ec/ECDAG.hh"
#include "ec/ECNode.hh"
#include "protocol/AGCommand.hh"
#include "sol/ParallelSolution.hh"
#include "util/DistUtil.hh"

#include "ec/Clay.hh"
#include "ec/BUTTERFLY.hh"
#include "ec/HHXORPlus.hh"
#include "ec/RDP.hh"

#include "sol/BalanceSolution.hh"
#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <netinet/in.h>
#include <string>
#include <unistd.h>
#include <vector>
#include <list>
#include <deque>
#include <unordered_map>

using namespace std;

// const bool DEBUG_ENABLE = true;

int ID = 0;
int TotalSize; 
vector<int> candidates;
set<string> load_bdwt = {}; 
Stripe* stripe;
unordered_map<int, int> headerLevaesColring; 
unordered_map<string, int> overlapmap;

class Point
{
public:
    Point(int load, int bdwt, double affinity)
        :_load(load)
        ,_bdwt(bdwt)
        ,_affinity(affinity)
    {}
    int _bdwt;
    int _load;
    double _affinity;

};

list<Point> paratoList = {};

void genParato()
{

}

void filter(list<Point>& list, const Point & p)
{

    if (list.empty()) {
        list.emplace_back(p);
        return;
    }


    if (p._load > list.back()._load && p._bdwt < list.back()._bdwt) {
        list.emplace_back(p);
        return;
    }

    
    bool hasInsert = false;
    for (std::list<Point>::iterator it = list.begin(); it != list.end();) {
        // cout << "[TEST] it : " << it->_load << endl;
        if (it->_load < p._load) {

            if (it->_bdwt <= p._bdwt) {
                return;
            }
            
            it++;
            continue;
        }
        
        if (it->_load == p._load) {
            if (it->_bdwt < p._bdwt) {
                return;
            }
            if (it->_bdwt == p._bdwt ) {
                if (it->_affinity != p._affinity)
                    cout << "[INFO] same load and bdwt, but with diff affinity" << it->_affinity << " " << p._affinity;
                return;
            }  
            if (it->_bdwt > p._bdwt) {
                it = list.erase(it);
                if (!hasInsert) {
                    list.insert(it,p);
                    hasInsert = true;
                }
                continue;
            }
        }

        if (it->_load > p._load) {
            if (!hasInsert) {
                list.insert(it,p);
                hasInsert = true;
            }
            if (it->_bdwt >= p._bdwt) {
                it = list.erase(it);
                continue;
            }
            if (it->_bdwt < p._bdwt) {
                return;    
            }
        }
    }
}

void printList(list<Point> list)
{
    for(auto it : list)
    {
        cout << "{" << it._load << ", " << it._bdwt << ", " << it._affinity <<"}" << endl;
    }
    cout << endl;  
}

void usage()
{
     // ./LimitedGen code n k w reapirIdx loadRatio digits initStr 
     cout << "limitedGen usage: " << endl;
     cout << "  1. code" << endl;
     cout << "  2. n" << endl;
     cout << "  3. k" << endl;
     cout << "  4. w" << endl;
     cout << "  5. reapirIdx" << endl;
}

// coloring: full dag vetex coloring
double Affinity(ECDAG* ecdag, vector<int> itm_idx, unordered_map<int, int>coloring)
{
    // 1. full dag coloring res
    double localityCount = 0;
    double totalCount = 0;
    for(auto it : ecdag->getECNodeMap()) 
    {
        if (find(itm_idx.begin(), itm_idx.end(), it.first) == itm_idx.end()) {
            continue;
        }

        totalCount++;
        int flag = 0;
        int nodeId = it.first;
        ECNode* node = it.second;
        int nodeColor = coloring[nodeId];
        // 2. polling childs and parents
        vector<int> childs = node->getChildIndices();
        vector<int> parents = node->getParentIndices();
        for (auto it : childs)
        {
            if(coloring[it] == nodeColor){
                flag = 1;
                break;
            }
        }
        if(flag ==1){
            localityCount ++;
            continue;
        }
        for (auto it : parents)
        {
            if(coloring[it] == nodeColor){
                flag = 1;
                break;
            }
        }
        if(flag ==1){
            localityCount ++;
            continue;
        }
        
    }
    // cout << "locality node count = " << localityCount << endl;
    // cout << "total node count = " << totalCount << endl;
    return localityCount * 1.0 / totalCount;
}


void affinity_search(int depth, unordered_map<int, int> itm_coloring,  vector<int> itm_idx, ECDAG* ecdag)
{
    if (depth == TotalSize) {
        auto coloring = itm_coloring;
        stripe->setColoring(coloring);
        stripe->evaluateColoring();

        // cout << "Inset: " <<  " " << stripe->getLoad() << " "<< stripe->getBdwt() << " "<< affinity << endl;
        // printList(paratoList);
        int debug;
        // cin >> debug;
        int load = stripe->getLoad();
        int bdwt = stripe->getBdwt(); 
        string key = to_string(load) + "_"+  to_string(bdwt);
        if(load_bdwt.find(key) == load_bdwt.end()) {
            cout << ID++ << " " << load << " " << bdwt << endl;
            load_bdwt.insert(key);
        }
        // for (auto it : itm_idx) {
        //     cout << " " << itm_coloring[it];
        // }
        // cout << endl;

        return;
    }


    int idx = itm_idx[depth];
    ECNode* node = ecdag->getECNodeMap()[idx];
    vector<int> childs = node->getChildIndices();
    // vector<int> colors1 = node->getChildColors(itm_coloring);

    vector<int> colors;
    for (auto it : childs) {
        int color = itm_coloring[it];
        LOG << "parent color of "<< idx << ": " << it << "["<< color <<"]"<< endl;
        if (find(colors.begin(), colors.end(), color) == colors.end()) {
            colors.push_back(color);
        }
    }

    // cout << endl;
    for (auto color : colors) {
        LOG << "[DEBUG] " << idx << " color si " << color << endl;
        itm_coloring[idx] = color;
        affinity_search(depth+1, itm_coloring, itm_idx, ecdag);
    }
}

void search(int depth, unordered_map<int, int> itm_coloring,  vector<int> itm_idx, ECDAG* ecdag)
{
    if (depth == TotalSize) {   
        auto coloring = itm_coloring;
        // for(auto it : headerLevaesColring)
        // {
        //     coloring[it.first] = it.second;
        // }

        stripe->setColoring(coloring);
        stripe->evaluateColoring();

        // cout << "Inset: " <<  " " << stripe->getLoad() << " "<< stripe->getBdwt() << " "<< affinity << endl;
        // printList(paratoList);
        int debug;
        // cin >> debug;
        cout << ID++ << " " << stripe->getBdwt() <<  " " << stripe->getLoad() << endl; 

        // for (auto it : itm_idx) {
        //     cout << " " << itm_coloring[it];
        // }
        // cout << endl;

        return;
    }


    int idx = itm_idx[depth];
    ECNode* node = ecdag->getECNodeMap()[idx];
    vector<int> childs = node->getChildIndices();
    // vector<int> colors1 = node->getChildColors(itm_coloring);

    vector<int> colors;
    for (auto it : childs) {
        int color = itm_coloring[it];
        LOG << "parent color of "<< idx << ": " << it << "["<< color <<"]"<< endl;
        if (find(colors.begin(), colors.end(), color) == colors.end()) {
            colors.push_back(color);
        }
    }

    // cout << endl;
    for (auto color : colors) {
        LOG << "[DEBUG] " << idx << " color si " << color << endl;
        itm_coloring[idx] = color;
        search(depth+1, itm_coloring, itm_idx, ecdag);
    }
}

int main(int argc, char **argv)
{
    // ./SingleMLP code n k w reapirIdx curveSize loadRatio digits initStr 
    struct timeval time1, time2;
    gettimeofday(&time1, NULL);
    
    if (argc != 6)
    {
        usage();
        return -1;
    }
    string code(argv[1]);
    int n = atoi(argv[2]);
    int k = atoi(argv[3]);
    int w = atoi(argv[4]);
    int repairIdx = atoi(argv[5]);
    srand(time(NULL));
    vector<unsigned int> loclist;
    vector<int> nodelist;
    for (int i=0; i<n; i++) {
        nodelist.push_back(i);
    }
    vector<string> blklist;
    for (int i=0; i<n; i++) {
        string blkname = "blk"+to_string(i);
        blklist.push_back(blkname);              
    }
    stripe = new Stripe(0, "stripe0", blklist, nodelist);
    
    ECBase* ec;
    vector<string> param;
    if (code == "Clay") {
        ec = new Clay(n, k, w, {to_string(n-1)});
    } else if (code == "RDP") {
        ec = new RDP(n, k, w, param);
    } else if (code == "HHXORPlus") {
        ec = new HHXORPlus(n, k, w, param);
    } else if (code == "BUTTERFLY") {
        ec = new BUTTERFLY(n, k, w, param);
    } else {
        cout << "Non-supported code!" << endl;
        return 0;
    }

    // headers, leaves
    stripe->genRepairECDAG(ec,repairIdx);
    ECDAG* ecdag = stripe->getECDAG();
    unordered_map<int, ECNode*> ecNodeMap = ecdag->getECNodeMap();
    vector<int> ecHeaders = ecdag->getECHeaders(); 
    vector<int> ecLeaves = ecdag->getECLeaves();

    // intermediate
    vector<int> itm_idx = ecdag->genTopoIdxs();
    unordered_map<int, int> coloring;
    for (auto item: ecNodeMap) {
        int sidx = item.first;
        if (find(ecHeaders.begin(), ecHeaders.end(), sidx) != ecHeaders.end())
        continue;
        if (find(ecLeaves.begin(), ecLeaves.end(), sidx) != ecLeaves.end())
        continue;
        // itm_idx.push_back(sidx);
        coloring.insert(make_pair(sidx, -1));
    }
    TotalSize = itm_idx.size();

    // candidates: 
    for (int i=0; i<n; i++)
        candidates.push_back(i);


    int bidx = repairIdx;
    // headers
    for (auto sidx: ecHeaders) { 
        coloring.insert(make_pair(sidx, bidx));
    }

    // leaves
    for (auto sidx: ecLeaves) {
        int bidx = sidx / w;
        if (bidx < n) {
            coloring.insert(make_pair(sidx, bidx));
        } else {
            coloring.insert(make_pair(sidx, -1));
        }
    }

    string configpath = "conf/sysSetting.xml";
    Config* conf = new Config(configpath);
    BalanceSolution* sol = new BalanceSolution(1,1,n+1);
    sol->init({stripe}, ec, code, conf);
    auto placement = stripe->getPlacement();
    placement.erase(std::remove(placement.begin(), placement.end(), repairIdx), placement.end());


    double spacesize = pow(candidates.size(), itm_idx.size());
    cout << "Spacesize: " << spacesize << endl;
    affinity_search(0, coloring, itm_idx, ecdag);
    printList(paratoList);

    gettimeofday(&time2, NULL);
    double singleTime = DistUtil::duration(time1, time2);
    cout << "Coloring for " << singleTime << endl;

    vector<int> ret = stripe->getsolution();
    for(auto it : ret)
    {
        cout << " " << it;
    }
    cout << endl;
    // stripe->getECDAG()->dumpTOPO();
    return 0;
}
