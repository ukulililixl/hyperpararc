#include "Globaldata.hh"

GlobalData::GlobalData()
{

}

GlobalData* GlobalData::getInstance()
{
    static GlobalData instance;
    static GlobalData* pInstance = nullptr;
    if (pInstance == nullptr)
    {
        pInstance = &instance;
    }
    return pInstance;
}


void GlobalData::printStackTrace() {
    const int max_frames = 128;
    void* addrlist[max_frames];
    int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

    if (addrlen == 0) {
        std::cerr << "No stack trace available\n";
        return;
    }
    char** symbol_list = backtrace_symbols(addrlist, addrlen);
    std::cout << "Stack trace:\n";
    for (int i = 0; i < addrlen; i++) {
        std::cout << symbol_list[i] << std::endl;
    }
    free(symbol_list);
}

size_t GlobalData::getMemoryUsage() {
    std::ifstream file("/proc/self/status");
    std::string line;
    while (std::getline(file, line)) {
        if (line.substr(0, 6) == "VmRSS:") {
            std::istringstream iss(line);
            std::string key;
            size_t memory;
            std::string unit;
            iss >> key >> memory >> unit;
            return memory; 
        }
    }
    return 0;
}