#include "inc/include.hh"
#include "common/Globaldata.cc"
#include "util/DistUtil.hh"
#include "common/Config.hh"
#include "common/Stripe.hh"
#include "ec/ECBase.hh"
#include "ec/Clay.hh"
#include "ec/RDP.hh"
#include "ec/HHXORPlus.hh"
#include "ec/BUTTERFLY.hh"
#include "sol/SolutionBase.hh"
#include "sol/BalanceSolution.hh"

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
    cout << "[DEBUG] Init Started"  <<endl;
    if (argc != 13) {
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

    GlobalData::getInstance()->testModel = atoi(argv[12]);

    string config_path = "conf/sysSetting.xml";
    cout << "[DEBUG] Init Config "  <<endl;
    Config* conf = new Config(config_path);
    // 0. read block placement
    vector<Stripe*> stripelist;
    ifstream infile(filepath);

    cout << "[DEBUG] Init Stripe Store "  <<endl;
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
    ECBase* ec;
    vector<string> param;
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
    SolutionBase* sol = new BalanceSolution(batchsize, standby_size, num_agents);
    sol->init(stripelist, ec, code, conf);
    struct timeval time1,time2,time3;
    // 2. generate repair batches
    cout << "[DEBUG] Init Finished"  <<endl;
    gettimeofday(&time1,NULL);
    sol->genRepairBatches(1, {fnid}, scenario, false);
    gettimeofday(&time2,NULL);

    // 3. get repair batches
    vector<RepairBatch*> repairbatches = sol->getRepairBatches();
    cout << "repairbatches.size: " << repairbatches.size() << endl;

    // 4. get statistics for each batch
    int overall_load = 0;
    int overall_bdwt = 0;
    for (int batchid=0; batchid<repairbatches.size(); batchid++) {
        RepairBatch* curbatch = repairbatches[batchid];
        int load = curbatch->getLoad();
        overall_load += load;
        overall_bdwt += curbatch->getBdwt();
        
        for (auto stripe : curbatch->getStripeList()) {
            cout << "STRIPE " << stripe->getStripeId() << endl;
            stripe->dumpTrans();
            stripe->dumpLoad(num_agents + standby_size);
        }

        cout << curbatch->dumpLoad(num_agents+standby_size);
    }
    double duration = DistUtil::duration(time1, time2);
    cout << "duration = " << duration << endl;
    cout << "[RET] overall load: "  << 1.0*overall_load/ecw << " blocks" << endl;
    cout << "[RET] overall bdwt: "  << 1.0*overall_bdwt/ecw << " blocks" << endl;
    LOG << "overall load: "  << 1.0*overall_load/ecw << " blocks" << endl;
    LOG << "overall bdwt: "  << 1.0*overall_bdwt/ecw << " blocks" << endl;
}
