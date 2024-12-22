#include "inc/include.hh"
#include "util/DistUtil.hh"
#include "common/Config.hh"
#include "common/Stripe.hh"
#include "ec/ECBase.hh"
#include "ec/Clay.hh"
#include "ec/RDP.hh"
#include "ec/HHXORPlus.hh"
#include "ec/BUTTERFLY.hh"
#include "sol/SolutionBase.hh"
#include "sol/CentSolution.hh"

using namespace std;

void usage() {
    cout << "usage: ./Simulation" << endl;
    cout << "   1. placement file path" << endl;
    cout << "   2. number of agents" << endl;
    cout << "   3. number of stripes" << endl;
    cout << "   4. code [Clay]" << endl;
    cout << "   5. ecn" << endl;
    cout << "   6. eck" << endl;
    cout << "   7, ecw" << endl;
    cout << "   8. scenario [standby|scatter]" << endl;
    cout << "   9. batchsize [3]" << endl;
    cout << "   10. fail node id [0]" << endl;
    cout << "   11. standby size " << endl;
}

int main(int argc, char** argv) {
    
    if (argc != 12) {
        usage();
        return 0;
    }

    string filepath = argv[1];
    int num_agents = atoi(argv[2]);
    int num_stripes = atoi(argv[3]);
    string code = argv[4];
    int ecn = atoi(argv[5]);
    int eck = atoi(argv[6]);
    int ecw = atoi(argv[7]);
    string scenario = argv[8];
    int batchsize = atoi(argv[9]);
    int fnid = atoi(argv[10]);
    int standby_size = atoi(argv[11]);

    string config_path = "conf/sysSetting.xml";
    Config* conf = new Config(config_path);

    // 0. read block placement
    vector<Stripe*> stripelist;
    ifstream infile(filepath);
    for (int stripeid=0; stripeid<num_stripes; stripeid++) {
        string line;
        getline(infile, line);
        vector<string> items = DistUtil::splitStr(line, " ");
        vector<int> nodeidlist;
        for (int i=0; i<ecn; i++) {
            int nodeid = atoi(items[i].c_str());
            nodeidlist.push_back(nodeid);
        }
        Stripe* curstripe = new Stripe(stripeid, nodeidlist);
        stripelist.push_back(curstripe);
    }
    infile.close();

    // 1. init a solution
    vector<string> param;
    ECBase* ec;
    if (code == "Clay") {
        ec = new Clay(ecn, eck, ecw, {to_string(ecn-1)});
    } else if (code == "RDP") {
        ec = new RDP(ecn, eck, ecw, param);
    } else if (code == "HHXORPlus") {
        ec = new HHXORPlus(ecn, eck, ecw, param);
    } else if (code == "BUTTERFLY") {
        ec = new BUTTERFLY(ecn, eck, ecw, param);
    } else {
        cout << "Non-supported code!" << endl;
        return 0;
    }

    string sol_param = to_string(batchsize);
    SolutionBase* sol = new CentSolution(batchsize, standby_size, num_agents);
    sol->init(stripelist, ec, code, conf);

    // 2. create a thread to generate repair batches
    thread genthread = thread([=]{sol->genRepairBatches(1, {fnid}, scenario, true);});
    sleep(1);
    // 3. get repair batches
    int overall_load = 0;
    int overall_bdwt = 0;
    struct timeval time1, time2, time3, time4;
    double latency = 0;
    
    while (sol->hasNext()) {
        gettimeofday(&time1, NULL);
        RepairBatch* curbatch = sol->getRepairBatchFromQueue();
        gettimeofday(&time2, NULL);
        latency += DistUtil::duration(time1, time2);

        int load = curbatch->getLoad();
        overall_load += load;
        overall_bdwt += curbatch->getBdwt();
    }

    // join
    genthread.join();
    cout << "overall load: " << overall_load << "= " << overall_load/ecw << " blocks" << endl;
    cout << "overall bdwt: " << overall_bdwt << "= " << overall_bdwt/ecw << " blocks" << endl;
    cout << "get repair batch latency: " << latency << " ms" << endl;
}
