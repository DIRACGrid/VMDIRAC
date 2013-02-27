#!/bin/bash
########################################################################
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################
# alternative to python script (remote run con Cernvm seems to fail on python call)
# contextualization script to be run on the VM, after init.d proccess 


if [ $# -ne 10 ]
then
	echo "bash contextualize-script.bash  certfile keyfile runjobagent runvmmonitoragent runlogjobagent runlogvmmonitoragent cvmfscontextscript diraccontextscript cvmfshttpproxy sitename>"
	exit 1
fi

vmCertPath=$1
vmKeyPath=$2
vmRunJobAgent=$3
vmRunVmMonitorAgent=$4
vmRunLogJobAgent=$5
vmRunLogVmMonitorAgent=$6
cvmfsContextPath=$7
diracContextPath=$8
cvmfs_http_proxy=$9
siteName=$10

localVmRunJobAgent=/root/run.job-agent
localVmRunVmMonitorAgent=/root/run.vm-monitor-agent
localVmRunLogJobAgent=/root/run.log.job-agent 
localVmRunLogVmMonitorAgent=/root/run.log.vm-monitor-agent
localCvmfsContextPath=/root/cvmfs-context.sh
localDiracContextPath=/root/dirac-context.sh

# vmcert and key have been previoslly copy to VM, these paths are local, the rest of files are on some repo... 
# 1) download the necesary files:
wget --no-check-certificate -O ${localVmRunJobAgent} ${vmRunJobAgent}
wget --no-check-certificate -O ${localVmRunVmMonitorAgent} ${vmRunVmMonitorAgent} 
wget --no-check-certificate -O ${localVmRunLogJobAgent} ${vmRunLogJobAgent} 
wget --no-check-certificate -O ${localVmRunLogVmMonitorAgent} ${vmRunLogVmMonitorAgent} 
wget --no-check-certificate -O ${localCvmfsContextPath} ${cvmfsContextPath} 
wget --no-check-certificate -O ${localDiracContextPath} ${diracContextPath} 

#2) Run the cvmvfs contextualization script:    
if [ ${cvmfsContextPath} != 'NONE' ]
then
    chmod u+x ${localCvmfsContextPath}
    bash ${localCvmfsContextPath} ${cvmfs_http_proxy}
fi

#3) Run the dirac contextualization script:    
chmod u+x ${localDiracContextPath}
bash ${localDiracContextPath} ${siteName} ${vmCertPath} ${vmKeyPath} ${localVmRunJobAgent} ${localVmRunVmMonitorAgent} ${localVmRunLogJobAgent} ${localVmRunLogVmMonitorAgent}

exit 0
