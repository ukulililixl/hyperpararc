//#include "common/CmdDistributor.hh"
#include "common/Config.hh"
#include "common/Coordinator.hh"
#include "common/Globaldata.hh"
#include "common/StripeStore.hh"
#include "inc/include.hh"
#include "sol/RepairBatch.hh"

using namespace std;

void usage() {
    cout << "Usage: ./ParaCoordinator " << endl;
    cout << "   1. method [centralize|offline|balance]" << endl;
    cout << "   2. failnodeid [0]" << endl;
    cout << "   3. scenario [scatter|standby]" << endl;
}

int main(int argc, char** argv) {

    if (argc != 4) {
        usage();
        return 0;
    }

    string method = string(argv[1]);
    int failnodeid = atoi(argv[2]);
    string scenario = string(argv[3]);
    string configpath = "conf/sysSetting.xml";
    Config* conf = new Config(configpath);
    GlobalData::getInstance()->testModel = PARALLEL_TUNING;
    int ecn = conf->_ecn;
    int eck = conf->_eck;
    int ecw = conf->_ecw;
    
    StripeStore* ss = new StripeStore(conf); 
    Coordinator* coor = new Coordinator(conf, ss);
    coor->initRepair(method, scenario, failnodeid);
    struct timeval time1, time2, time3, time4, time5;

    // parallel
    double latency = 0;
    vector<double> batchtime;
    thread genThread = thread([=]{coor->genRepairSolutionAsync();});
    coor->repair();
    genThread.join();

    // clean
    if (coor)
        delete coor;
    if (ss)
        delete ss;
    if (conf)
        delete conf;
    return 0;
}
