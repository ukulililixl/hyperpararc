#ifndef _ECDAG_HH_
#define _ECDAG_HH_

#include "../inc/include.hh"

#include "ECNode.hh"

using namespace std;

#define ECDAG_DEBUG_ENABLE false

#define REQUESTOR 32767
#define SGSTART 0
#define USTART 0
#define CSTART 0

class ECDAG {
  private:
    unordered_map<int, ECNode*> _ecNodeMap;
    vector<int> _ecHeaders;
    vector<int> _ecLeaves;
    vector<int> _itm_idx;
    
  public:
    ECDAG(); 
    ~ECDAG();

    void Join(int pidx, vector<int> cidx, vector<int> coefs);
    void Concact(vector<int> cidx);
    vector<int> genItmIdxs();
    unordered_map<int, ECNode*> getECNodeMap();
    vector<int> getECHeaders();
    vector<int> getECLeaves();
    vector<int> getAllNodeIds();

    // for debug
    void dump();
    void dumpTOPO();
    vector<vector<int>> genLeveledTopologicalSorting();
    vector<int> genTopoIdxs();
};
#endif
