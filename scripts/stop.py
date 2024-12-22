import os
import subprocess

# home dir
cmd = r'echo ~'
home_dir_str, stderr = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).communicate()
home_dir = home_dir_str.decode().strip()
# proj dir
proj_dir="{}/hyperpararc".format(home_dir)
config_dir="{}/conf".format(proj_dir)
CONF = config_dir+"/sysSetting.xml"

f = open(CONF)
start = False
concactstr = ""
for line in f:
    if line.find("setting") == -1:
        line = line[:-1]
    concactstr += line
res=concactstr.split("<attribute>")

slavelist=[]
for attr in res:
    if attr.find("agents.addr") != -1:
        valuestart=attr.find("<value>")
        valueend=attr.find("</attribute>")
        attrtmp=attr[valuestart:valueend]
        slavestmp=attrtmp.split("<value>")
        for slaveentry in slavestmp:
            if slaveentry.find("</value>") != -1:
                entrysplit=slaveentry.split("<")
                slave=entrysplit[0]
                #slave=entrysplit[2][0:-1]
                slavelist.append(slave)
    if attr.find("repairnodes.addr") != -1:
        valuestart=attr.find("<value>")
        valueend=attr.find("</attribute>")
        attrtmp=attr[valuestart:valueend]
        slavestmp=attrtmp.split("<value>")
        for slaveentry in slavestmp:
            if slaveentry.find("</value>") != -1:
                #entrysplit=slaveentry.split("/")
                #slave=entrysplit[2][0:-1]
                entrysplit=slaveentry.split("<")
                slave=entrysplit[0]
                slavelist.append(slave)

# stop
print("stop coordinator")
os.system("redis-cli flushall")

for slave in slavelist:
    print("stop slave on ", slave)
    cmd="ssh "+ slave +" \"ps aux|grep redis\""
    res=subprocess.Popen(['/bin/bash','-c',cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = res.communicate()

    pid=-1
    out = out.split("\n")
    for line in out:
       if line.find("redis-server") == -1:
           continue
       item = line.split(" ")

       for i in range(1,7):
           if (item[i] != ''):
               pid = item[i]
               break

    #cmd="ssh "+slave+" \"sudo kill -9 "+str(pid)+"\""
    #print(cmd)
    #os.system(cmd)

    #cmd="ssh "+slave+" \"sudo rm /var/lib/redis/dump.rdb\""
    #print(cmd)
    #os.system(cmd)

    #cmd="ssh "+slave+" \" ulimit -c unlimited  \""
    #print(cmd)
    #os.system(cmd)

    cmd="ssh "+slave+" \"sudo service redis-server restart\""
    os.system(cmd)

    print(cmd)
    os.system("ssh " + slave + " \"killall ParaAgent \"")

