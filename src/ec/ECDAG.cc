#include "ECDAG.hh"

ECDAG::ECDAG() {
}

ECDAG::~ECDAG() {
    //for (auto item: _ecClusterMap)
    //    delete item.second;
    //for (auto item: _ecUnitMap)
    //    delete item.second;
    
    for (auto it: _ecNodeMap) {
        if (it.second) {
            delete it.second;
            it.second = nullptr;
        }
    }
    _ecNodeMap.clear();
}


void ECDAG::Join(int pidx, vector<int> cidx, vector<int> coefs) {
  // debug start
  string msg = "ECDAG::Join(" + to_string(pidx) + ",";
  for (int i=0; i<cidx.size(); i++) msg += " "+to_string(cidx[i]);
  msg += ",";
  for (int i=0; i<coefs.size(); i++) msg += " "+to_string(coefs[i]);
  msg += ")";
  if (ECDAG_DEBUG_ENABLE) cout << msg << endl;
  // debug end

  // 0. deal with childs
  vector<ECNode*> targetChilds;
  for (int i=0; i<cidx.size(); i++) { 
    int curId = cidx[i];
    // 0.0 check whether child exists in our ecNodeMap
    unordered_map<int, ECNode*>::const_iterator findNode = _ecNodeMap.find(curId);
    ECNode* curNode;
    if (findNode == _ecNodeMap.end()) {
      // child does not exists, we need to create a new one
      curNode = new ECNode(curId);
      // a new child is set to leaf on creation
      curNode->setType("leaf");
      // insert into ecNodeMap
      _ecNodeMap.insert(make_pair(curId, curNode));
      _ecLeaves.push_back(curId);
    } else {
      // child exists
      curNode = _ecNodeMap[curId];
      // NOTE: if this node is marked with root before, it should be marked as intermediate now
      vector<int>::iterator findpos = find(_ecHeaders.begin(), _ecHeaders.end(), curId);
      if (findpos != _ecHeaders.end()) {
        // delete from headers
        _ecHeaders.erase(findpos);
        curNode->setType("intermediate");
      }
    }
    // 0.1 add curNode into targetChilds
    targetChilds.push_back(curNode);
//    // 0.3 increase refNo for curNode
//    curNode->incRefNumFor(curId);
  }

  // 1. deal with parent
  ECNode* rNode;
  assert(_ecNodeMap.find(pidx) == _ecNodeMap.end());
  // pidx does not exists, create new one and add to headers
  rNode = new ECNode(pidx);
  // parent node is set to root on creation
  rNode->setType("root");
  _ecNodeMap.insert(make_pair(pidx, rNode));
  _ecHeaders.push_back(pidx);

  // set child nodes for the root node, as well as the coefs
  rNode->setChilds(targetChilds);
  rNode->setCoefs(coefs);
//  rNode->addCoefs(pidx, coefs);

  // set parent node for each child node
  for (auto cnode: targetChilds) {
    cnode->addParentNode(rNode);
  }
}

void ECDAG::Concact(vector<int> cidx) {
  vector<int> coefs;
  for (int i=0; i<cidx.size(); i++) {
    coefs.push_back(-1);
  }

  Join(REQUESTOR, cidx, coefs);
}


unordered_map<int, ECNode*> ECDAG::getECNodeMap() {
  return _ecNodeMap;
}

vector<int> ECDAG::getECHeaders() {
  return _ecHeaders;
}

vector<int> ECDAG::getECLeaves() {
  return _ecLeaves;
}

vector<int> ECDAG::getAllNodeIds() {
    vector<int> toret;
    for (auto item: _ecNodeMap) {
        int idx = item.first;
        toret.push_back(idx);
    }
    return toret;
}

vector<int> ECDAG::genItmIdxs() {
    vector<int> ret;
    for (auto item : _ecNodeMap)
    {
        int sidx = item.first;
        if (find(_ecHeaders.begin(), _ecHeaders.end(), sidx) != _ecHeaders.end())
            continue;
        if (find(_ecLeaves.begin(), _ecLeaves.end(), sidx) != _ecLeaves.end())
            continue;
        ret.push_back(sidx);
    }
    sort(ret.begin(), ret.end());
    return ret;
}

void ECDAG::dump() {
  for (auto id : _ecHeaders) {
    _ecNodeMap[id] ->dump(-1);
    cout << endl;
  }
}


void ECDAG::dumpTOPO() {
  cout << "ECDAG::dumpTOPO" << endl;
  vector<int> topoidx = genTopoIdxs();
  for(auto it : topoidx){
      auto node = _ecNodeMap[it];
      cout << it << ": ";
      for(auto child: node->getChildIndices()){
        cout << child << " ";
      }
      cout<< endl;
  }
}


vector<vector<int>> ECDAG::genLeveledTopologicalSorting() {
    vector<vector<int>> toret;

    unordered_map<int, int> inref;
    unordered_map<int, int> outref;
    for (auto item: _ecNodeMap) {
        int dagidx = item.first;
        ECNode* dagnode = item.second;
        vector<ECNode*> childlist = dagnode->getChildNodes();

        // increase outref for each child by 1
        for (auto cnode: childlist) {
            int child_dagidx = cnode->getNodeId();
            if (outref.find(child_dagidx) == outref.end()) {
                outref.insert(make_pair(child_dagidx, 1));
            } else
                outref[child_dagidx]++;
        }

        // increase inref for current node by childlist.size()
        inref.insert(make_pair(dagidx, childlist.size()));
    }

    // 1. Each time we search for leave vertices at the same level
    while (true) {
        vector<int> leaves;
        //cout << "inref.size: " << inref.size() << endl;

        if (inref.size() == 0)
            break;

        // check inref and find vertices that has inref=0
        for (auto item: inref) {
            if (item.second == 0)
                leaves.push_back(item.first);
        }

        toret.push_back(leaves);

        // remove the current level from inref
        for (auto idx: leaves) {
            // find the leave vertex
            ECNode* node = _ecNodeMap[idx];

            // this is not the root
            if (outref[idx] != 0) {
                // get parent vertices
                vector<ECNode*> parentNodes = node->getParentNodes();
                for(auto it: parentNodes) {
                    int pidx = it->getNodeId();

                    // decrease inref for each parent by 1
                    inref[pidx]--;
                }
            }

            // remove idx from inref map such that in the next round we will not count it
            inref.erase(inref.find(idx));
        }
    }

    return toret;
}

vector<int> ECDAG::genTopoIdxs() {
  // 0. generate inrefs and outrefs for each node
  vector<int> topoIdxs;
  unordered_map<int, int> inref;
  unordered_map<int, int> outref;
  for (auto item: _ecNodeMap) {
    int nodeId = item.first;
    ECNode* node = item.second;
    vector<ECNode*> childlist = node->getChildNodes();
    // increase outref for each child by 1
    for (auto cnode: childlist) {
      int cidx = cnode->getNodeId();
      if (outref.find(cidx) == outref.end())
        outref.insert(make_pair(cidx, 1));
      else
        outref[cidx]++;
    }
    // increate inref for current node by childlist.size();
    inref.insert(make_pair(nodeId, childlist.size()));
  }

  // 1. Each time we search for leaf nodes
  deque<int> leaves;
  for (auto item: inref) {
    if (item.second == 0)
      leaves.push_back(item.first);
  }
  while (leaves.size() > 0) {
    int idx = leaves.front();
    leaves.pop_front();
    // cout << "leaves idx = " << idx << endl;
    if(idx != REQUESTOR && find(_ecLeaves.begin(), _ecLeaves.end(), idx) == _ecLeaves.end()){
        topoIdxs.push_back(idx);
    }
    ECNode* node = _ecNodeMap[idx];
    if (outref[idx] == 0) {
      continue;
    }
    vector<ECNode*> parentNodes = node->getParentNodes();
    for (auto it: parentNodes) {
        int pidx = it->getNodeId();
        inref[pidx]--;
        if(inref[pidx] == 0){
            leaves.push_back(pidx);
        }
    } 
  }
  return topoIdxs;
}

