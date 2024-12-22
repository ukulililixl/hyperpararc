import os
import sys
import subprocess
import time

# home dir

# varying code
cmd = r'echo ~'
home_dir_str, stderr = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
home_dir = home_dir_str.decode().strip()
system="hyperpararc"
proj_dir="{}/{}".format(home_dir, system)
print(proj_dir)
script_dir="{}/scripts".format(proj_dir)
script="{}/testbed/fullnode_bw.py".format(script_dir)
cmd="cd {}".format(proj_dir)
os.system(cmd)
cluster="aliyun"
#code="Clay"
#ecn=14
#eck=10
#ecw=256
scenario="standby"
blkMB=256
pktKB=64
batchsize=4
numstripes=20
bdwt=10000

tmplog="{}/log.txt".format(proj_dir)
logdir="{}/log/standby_node_10G".format(proj_dir)
retdir="{}/ret/standby_node_10G".format(proj_dir)
print(logdir)
print(retdir)
os.system("mkdir -p {}".format(logdir))
os.system("mkdir -p {}".format(retdir))
os.system("rm -rf {}/*".format(logdir))
os.system("rm -rf {}/*".format(retdir))


# varying method
# 1. intra only
# 2. intra&inter
methods=['balance', 'balance']
tunings=['false', 'true']

# varying code
# 1. Clay 12
# 2. Clay 14
# 3. Clay 16
# 4. Butt 12
codes=['Clay', 'Clay', 'Clay', 'BUTTERFLY']
ecns=[12, 14, 16, 12]
ecks=[8, 10, 12, 10]
ecws=[64, 256, 256, 512]


gendata="true"
for time in range(1,6):
    for standbysize in range(1,5):
        for idx in range(0,2):	
            method=methods[idx]
            tuning=tunings[idx]
            for i in range(0,6)
                code=codes[i]
                ecn=ecns[i]
                enk=ecks[i]
                ecw=ecws[i]
                key="{}_{}_{}_{}".format(code,ecn,method,tuning)
                logfile="{}/{}_{}".format(logdir,key,time)
                retfile="{}/{}".format(logdir,key)
                cmd="timeout 10m python {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} 1 {} &> coor_output".format(script, cluster, code, ecn, eck, ecw, method, scenario, blkMB, pktKB, batchsize, numstripes, gendata, bdwt, standbysize, tuning )
                print(cmd)
                os.system(cmd)


                os.system("date")
                os.system("cp {} {}".format(tmplog, logfile))
                print(logfile)    

                cmd = "grep -a -oP 'Coordinator::repair. repairbatch = \K\d+' {} ".format(logfile)
                print(cmd)
                os.system(cmd)
                os.system("grep -a -oP 'Coordinator::repair. repairbatch = \K\d+' {} >> {}".format(logfile, retfile))
                
            if gendata == "true":
                gendata = "false"





