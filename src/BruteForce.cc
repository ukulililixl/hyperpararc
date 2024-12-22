#include "common/Config.hh"
#include "common/Stripe.hh"
#include "common/StripeStore.hh"
#include "ec/ECDAG.hh"
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
#include <set>
#include <unistd.h>
#include <string>
#include <vector>
#include <list>
#include <deque>
#include <unordered_map>

using namespace std;

// const bool DEBUG_ENABLE = true;

int ID = 0;
int TotalSize; 
vector<int> candidates; 
Stripe* stripe;
unordered_map<int, int> headerLevaesColring; // headers 和leaves的coloring


// 唯一标识,  便于画图
set<string> load_bdwt_ar = {};
set<string> load_bdwt = {};
set<string> load_ar = {};
set<string> bdwt_ar = {};

vector<string> load_bdwt_ar_parato = {};
vector<string> load_ar_parato = {};
vector<string> bdwt_ar_parato = {};

int n, k, w;

class Point
{
public:
    Point(double load, double bdwt, double affinity)
        :_load(load)
        ,_bdwt(bdwt)
        ,_affinity(affinity)
    {}
    double _bdwt;
    double _load;
    double _affinity;

};

list<Point> paratoList = {};


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
                
                if (it->_affinity != p._affinity){
                    // cout << "[INFO] same load and bdwt, but with diff affinity" << it->_affinity << " " << p._affinity;
                    it->_affinity = max(it->_affinity, p._affinity);
                    if (it->_bdwt == 2.5) {
                        int debug;
                        cin >>debug;
                    } 
                }

                
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

void test1()
{
    list<Point> list = {};
    list.emplace_back(Point(1, 10, 1.1));
    list.emplace_back(Point(5, 5, 1.1));
    list.emplace_back(Point(10, 1, 1.1));
    printList(list);

    cout << endl;


    // filter(list, Point(2, 10, 1.1)); // 起点右上
    // filter(list, Point(0,11,1.1)); // 起点左上
    // filter(list, Point(2, 8, 1.1)); // 起点右下
    // filter(list, Point(0, 4, 1.1)); // 起点左下
    // filter(list, Point(10, 2, 1.1)); // 终点右上
    // filter(list, Point(11, 0, 1.1)); // 终点右下
    // filter(list, Point(9, 2, 1.1)); // 终点左上
    // filter(list, Point(9, 0, 1.1)); // 终点左下
    printList(list);
}

void test2()
{
    list<Point> list = {};
    filter(list, Point(1, 10, 1.1));
    printList(list);

    filter(list, Point(2, 9, 1.1));
    printList(list);
 
    filter(list, Point(1, 9, 1.1));
    printList(list);

    filter(list, Point(2, 8, 1.1));
    filter(list, Point(3, 7, 1.1));
    filter(list, Point(2, 8, 1.2));
    filter(list, Point(3, 6, 1.2));
    filter(list, Point(2, 6, 1.2));
    printList(list);


    filter(list, Point(0, 0, 1.2));
    printList(list);
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
    auto leaves = ecdag->getECLeaves();
    auto headers = ecdag->getECHeaders();
    for(auto it : ecdag->getECNodeMap()) 
    {
        // 只考虑itm_node和root
        if (find(leaves.begin(), leaves.end(), it.first) != leaves.end()) {
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
    }
    // cout << "locality node count = " << localityCount << endl;
    // cout << "total node count = " << totalCount << endl;
    return localityCount * 1.0 / totalCount;
}

void simple_search(int depth, unordered_map<int, int> itm_coloring,  vector<int> itm_idx)
{
    if (depth == TotalSize) {
        auto coloring = itm_coloring;
        for(auto it : headerLevaesColring)
        {
            coloring[it.first] = it.second;
        }

        stripe->setColoring(coloring);
        stripe->evaluateColoring();
   
        double load = stripe->getLoad() * 1.0 / w;
        double bdwt = stripe->getBdwt() * 1.0 / w;
        if (load == 2.5 && bdwt ==2.5) {
            stripe->dumpTrans();
            int debug;
            cin >> debug;
        }
        double affinity = Affinity(stripe->getECDAG(), itm_idx, coloring);

        // load+bdwt+ar 
        string key = to_string(load) + "_"+  to_string(bdwt) + "_" + to_string(affinity);
        load_bdwt_ar.insert(key);

        string load_ar_key = to_string(load) + "_" + to_string(affinity);
        load_ar.insert(load_ar_key);

        string bdwt_ar_key = to_string(bdwt) + "_" + to_string(affinity);
        bdwt_ar.insert(bdwt_ar_key);

        // parato曲线
        Point p = Point(load, bdwt, affinity);
        // cout << "Inset: " <<  " " << stripe->getLoad() << " "<< stripe->getBdwt() << " "<< affinity << endl;
        
        filter(paratoList, p);
        printList(paratoList);
        int debug;
        // cin >> debug;
        // LOG << ID++ << " " << stripe->getBdwt() <<  " " << stripe->getLoad() << " " << affinity << endl; 
        return;
    }


    int idx = itm_idx[depth];
    int color = itm_coloring[idx]; 
    for (int i = 0; i < candidates.size(); i++) {
        itm_coloring[idx] = candidates[i];
        simple_search(depth+1, itm_coloring, itm_idx);
    }
}

void search(int depth, unordered_map<int, int> itm_coloring,  vector<int> itm_idx)
{
    if (depth == TotalSize) {
        auto coloring = itm_coloring;
        for(auto it : headerLevaesColring)
        {
            coloring[it.first] = it.second;
        }

        stripe->setColoring(coloring);
        stripe->evaluateColoring();
   
        double load = stripe->getLoad() * 1.0 / w;
        double bdwt = stripe->getBdwt() * 1.0 / w;
        if (load == 2.5 && bdwt ==2.5) {
            stripe->dumpTrans();
            int debug;
            cin >> debug;
        }

        // load+bdwt+ar 
        string key = to_string(load) + "_"+  to_string(bdwt);
        if(load_bdwt.find(key) == load_bdwt.end()) {
            cout << ID++ << " " << load << " " << bdwt << endl;
            load_bdwt.insert(key);
        }
        return;
    }


    int idx = itm_idx[depth];
    int color = itm_coloring[idx]; 
    for (int i = 0; i < candidates.size(); i++) {
        itm_coloring[idx] = candidates[i];
        search(depth+1, itm_coloring, itm_idx);
    }
}

// 皆用于去重画图
void log_load_ar()
{
    for (auto it : load_ar) {
        string flag = "common";
        if (find(load_ar_parato.begin(), load_ar_parato.end(), it) != load_ar_parato.end()) {
            flag = "parato";
        }
        size_t start = 0;
        size_t end = it.find("_", start);
        double load =  stod(it.substr(start,end-start));
        start = end + 1;
        double ar = stod(it.substr(start));
        LOG  << flag << " " << load << " " << ar << endl;
    }
}

void log_bdwt_ar()
{
    for (auto it : bdwt_ar) {
        string flag = "common";
        if (find(bdwt_ar_parato.begin(), bdwt_ar_parato.end(), it) != bdwt_ar_parato.end()) {
            flag = "parato";
        }
        size_t start = 0;
        size_t end = it.find("_", start);
        double bdwt = stod(it.substr(start,end-start));
        start = end + 1;
        double ar = stod(it.substr(start));
        LOG  << flag << " " << bdwt << " " << ar << endl;
    }
}

void log_load_bdwt_ar()
{
    for (auto it : load_bdwt_ar) {
        string flag = "common";
        if (find(load_bdwt_ar_parato.begin(), load_bdwt_ar_parato.end(), it) != load_bdwt_ar_parato.end()) {
            flag = "parato";
        }
        size_t start = 0;
        size_t end = it.find("_", start);
        double load =  stod(it.substr(start,end-start));
        start = end + 1;
        end = it.find("_", start);
        double bdwt = stod(it.substr(start,end-start));
        start = end + 1;
        double ar = stod(it.substr(start));
        LOG  << flag << " " << load << " " << bdwt << " " << ar << endl;
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
    n = atoi(argv[2]);
    k = atoi(argv[3]);
    w = atoi(argv[4]);
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
    stripe = new Stripe(0, "stripe0", blklist,  nodelist);
    
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
    vector<int> itm_idx;
    unordered_map<int, int> coloring;
    for (auto item: ecNodeMap) {
        int sidx = item.first;
        if (find(ecHeaders.begin(), ecHeaders.end(), sidx) != ecHeaders.end())
        continue;
        if (find(ecLeaves.begin(), ecLeaves.end(), sidx) != ecLeaves.end())
        continue;
        itm_idx.push_back(sidx);
        coloring.insert(make_pair(sidx, -1));
    }

    TotalSize = itm_idx.size();

    // candidates: 
    for (int i=0; i<n; i++)
        candidates.push_back(i);


    int bidx = repairIdx;
    // headers
    for (auto sidx: ecHeaders) { 
        headerLevaesColring.insert(make_pair(sidx, bidx));
    }

    // leaves
    for (auto sidx: ecLeaves) {
        int bidx = sidx / w;
        if (bidx < n) {
            headerLevaesColring.insert(make_pair(sidx, bidx));
        } else {
            headerLevaesColring.insert(make_pair(sidx, -1));
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

    // simple_search(0, coloring, itm_idx);

    search(0, coloring, itm_idx);
    return 0;

    printList(paratoList);

    gettimeofday(&time2, NULL);
    double singleTime = DistUtil::duration(time1, time2);
    cout << "Coloring for " << singleTime << endl;
    stripe->dumpLoad(n);
    vector<int> ret = stripe->getsolution();
    for(auto it : ret)
    {
        cout << " " << it;
    }
    cout << endl;
    // stripe->getECDAG()->dumpTOPO();
    for (auto it : paratoList) {
        load_bdwt_ar_parato.push_back(to_string(it._load) + "_" + to_string(it._bdwt) + "_" + to_string(it._affinity));
        load_ar_parato.push_back(to_string(it._load) + "_" + to_string(it._affinity));
        bdwt_ar_parato.push_back(to_string(it._bdwt) + "_" + to_string(it._affinity));
    }
    
    log_load_ar();
    // log_bdwt_ar();
    return 0;
}
