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
script_dir = "{}/scripts".format(proj_dir)
stripestore_dir="{}/stripeStore".format(proj_dir)


f=open(CONF)
start = False
concactstr = ""
for line in f:
    if line.find("setting") == -1:
        line = line[:-1]
    concactstr += line
res=concactstr.split("<attribute>")

slavelist=[]
fstype=""
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
                slavelist.append(slave)
    if attr.find("repairnodes.addr") != -1:
        valuestart=attr.find("<value>")
        valueend=attr.find("</attribute>")
        attrtmp=attr[valuestart:valueend]
        slavestmp=attrtmp.split("<value>")
        for slaveentry in slavestmp:
            if slaveentry.find("</value>") != -1:
                entrysplit=slaveentry.split("<")
                slave=entrysplit[0]
                slavelist.append(slave)

print(slavelist)


os.system("redis-cli flushall")
os.system("killall ParaCoordinator")
os.system("sudo service redis_6379 restart")


for slave in slavelist:
    cmd="ssh "+slave+" \"sudo service redis_6379 restart\""
    os.system(cmd)
    cmd="ssh "+slave+" \" rm -rf "+proj_dir+ "/log.txt \""
    os.system(cmd)
    cmd="ssh "+slave+" \" rm -rf "+proj_dir+ "/blkDir/*/*.repair\""
    os.system(cmd)
    os.system("ssh " + slave + " \"redis-cli flushall \"")
    os.system("ssh " + slave + " \"killall ParaAgent \"")

    command="ssh "+slave+" \"cd "+proj_dir+"; ./build/ParaAgent &> "+proj_dir+"/agent_output &\""
    print(command)
    os.system(command)
