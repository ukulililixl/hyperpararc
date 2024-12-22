#include "common/Config.hh"
#include "common/Stripe.hh"
#include "common/StripeStore.hh"
#include "ec/ECDAG.hh"
#include "protocol/AGCommand.hh"
#include "sol/ParallelSolution.hh"
#include "util/DistUtil.hh"

#include "ec/RSPIPE.hh"
#include "ec/Clay.hh"
#include "ec/BUTTERFLY.hh"
#include "ec/HHXORPlus.hh"
#include "ec/RDP.hh"

#include "sol/BalanceSolution.hh"

using namespace std;

// const bool DEBUG_ENABLE = true;

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
    Stripe* stripe = new Stripe(0, "stripe0", blklist, nodelist);
    
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
    } else if (code == "RSPIPE") {
        ec = new RSPIPE(n, k, w, param);
    } else {
        cout << "Non-supported code!" << endl;
        return 0;
    }
    cout << "[DEBUG] gen repair ecdag: " << repairIdx << endl;
    stripe->genRepairECDAG(ec,repairIdx);
    stripe->getECDAG()->dumpTOPO();
    
    string configpath = "conf/sysSetting.xml";
    Config* conf = new Config(configpath);
    BalanceSolution* sol = new BalanceSolution(1,1,n);
    sol->init({stripe}, ec, code, conf);

    
    int cluster_size = n+1;
    auto loadtable = vector<vector<int>>(cluster_size, {0,0});
    auto placement = stripe->getPlacement();
    placement.erase(std::remove(placement.begin(), placement.end(), repairIdx), placement.end());    

    sol->genBalanceColoringForSingleFailure(stripe,repairIdx, "standby", loadtable);

    vector<int> ret = stripe->getsolution();

    // stripe->dumpLoad(cluster_size);
    vector<int> itm_idx = stripe->getECDAG()->genItmIdxs();
    unordered_map<int, int> coloring = stripe->getColoring();
    vector<int> itm_sol;
    for (int i = 0; i < itm_idx.size(); i++)
    {
        int color = coloring.at(itm_idx[i]);
        itm_sol.push_back(color);
    }


    cout << "ret: " << stripe->getStringAffinity(cluster_size, itm_sol) << endl;
    return 0;
}
