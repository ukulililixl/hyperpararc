#ifndef __GLOBAL_DATA_H__
#define __GLOBAL_DATA_H__
#include <execinfo.h>
#include <iostream>  
#include <fstream>
#include <string>
#include <sstream>
using namespace std;

enum MODEL {
    OFFLINE = 0,
    OFFLINE_TUNING,
    PARALLEL,
    PARALLEL_TUNING
};

class GlobalData
{
    public:
        static GlobalData* getInstance();
        int testModel; // tuning-true, not tuning-false
        void static printStackTrace();
        size_t static getMemoryUsage();
    protected:
        GlobalData();
};

#endif
