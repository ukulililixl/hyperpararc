import os
import sys
import subprocess
import time

# home dir
cmd = r'echo ~'
home_dir_str, stderr = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
home_dir = home_dir_str.decode().strip()
system="hyperpararc"
proj_dir="{}/{}".format(home_dir, system)
print(proj_dir)
script_dir="{}/scripts".format(proj_dir)
script="{}/pretest/bw.py".format(script_dir)
cmd="cd {}".format(proj_dir)
os.system(cmd)
scenario="standby"
blkMB=256
pktKB=64
batchsize=4
stripesnum=1000
#agentsnum=20
standbysize=4

# RSCONV RSCONV 14 10 1 centralize
# RSPIPE RSPIPE 14 10 1 offline
# Clay Clay 14 10 256 centralize
# Butterfly BUTTERFLY 12 10 512 centralize
# ParaRC Clay 14 10 256 offline
# HyperParaRC-base Clay 14 10 256 balance
# HyperParaRC Clay 14 10 256 balance

tests=["RSCONV", "RSPIPE", "Clay", "Butterfly", "ParaRC", "HyperParaRC-base", "HyperParaRC"]
codes=["RSCONV", "RSPIPE", "Clay", "BUTTERFLY", "Clay", "Clay", "Clay"]
ecns=[14, 14, 14, 12, 14, 14, 14]
ecks=[10, 10, 10, 10, 10, 10, 10]
ecws=[1, 1, 256, 512, 256, 256, 256]
methods=["Sim_offline", "Sim_offline", "Sim_centralize", "Sim_centralize", "Sim_offline", "Sim_balance", "Sim_balance"]
agentsnums=[20,40,60,80,100]


for agentsnum in agentsnums:
    cmd = "python scripts/data/gen_simulation_data.py {} 100 14 10 0".format(agentsnum)
    os.system(cmd)
    cmd = "python scripts/data/gen_simulation_data.py {} 100 12 10 0".format(agentsnum)
    os.system(cmd)

gendata="true"
for time in range(1,6):
    for agentsnum in agentsnums:
        for idx in range(0,7):
            test=tests[idx]
            method=methods[idx]
            code=codes[idx]
            ecn=ecns[idx]
            eck=ecks[idx]
            ecw=ecws[idx]
            program="./build/"+method
            stripeStore="stripeStore/simulation_{}_{}_{}_{}".format(agentsnum, stripesnum, ecn, eck)
            cmd="{} {} {} {} {} {} {} {} standby {} 0 {} > output ".format(program, stripeStore, agentsnum, stripesnum, code, ecn, eck, ecw, batchsize, standbysize)
            if test=="HyperParaRC-base":
                cmd += " 0"
            elif test=="HyperParaRC":                  
                cmd += " 3"
            print("Test for: ", test, agentsnum, "  ", cmd)
            os.system("rm -rf log.txt")
            os.system(cmd)

            print("load")
            os.system("grep -a -oP 'overall load: \K\d+' log.txt ")
            print("bdwt")
            os.system("grep -a -oP 'overall bdwt: \K\d+' log.txt ")
