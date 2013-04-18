#!/bin/bash
########################################################################
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################
# alternative to python script (remote run con Cernvm seems to fail on python call)
# contextualization script to be run on the VM, after init.d proccess 


if [ $# -ne 12 ]
then
	echo "bash contextualize-script.bash  <uniqueId> <certfile> <keyfile> <runjobagent> <runvmmonitoragent> <runlogjobagent> <runlogvmmonitoragent> <cvmfscontextscript> <diraccontextscript> <cvmfshttpproxy> <sitename> <clouddriver> <cpuTime>"
	exit 1
fi

uniqueId=$1
vmCertPath=$2
vmKeyPath=$3
vmRunJobAgent=$4
vmRunVmMonitorAgent=$5
vmRunLogJobAgent=$6
vmRunLogVmMonitorAgent=$7
cvmfsContextPath=$8
diracContextPath=$9
cvmfs_http_proxy=${10}
siteName=${11}
cloudDriver=${12}
cpuTime=${13}

localVmRunJobAgent=/root/run.job-agent
localVmRunVmMonitorAgent=/root/run.vm-monitor-agent
localVmRunLogJobAgent=/root/run.log.job-agent 
localVmRunLogVmMonitorAgent=/root/run.log.vm-monitor-agent
localCvmfsContextPath=/root/cvmfs-context.sh
localDiracContextPath=/root/dirac-context.sh

# parameters log:

echo "1 $uniqueID" >> /var/log/contextualize-script.log 2>&1
echo "2 $vmCertPath" >> /var/log/contextualize-script.log 2>&1
echo "3 $vmKeyPath" >> /var/log/contextualize-script.log 2>&1
echo "4 $vmRunJobAgent" >> /var/log/contextualize-script.log 2>&1
echo "5 $vmRunVmMonitorAgent" >> /var/log/contextualize-script.log 2>&1
echo "6 $vmRunLogJobAgent" >> /var/log/contextualize-script.log 2>&1
echo "7 $vmRunLogVmMonitorAgent" >> /var/log/contextualize-script.log 2>&1
echo "8 $cvmfsContextPath" >> /var/log/contextualize-script.log 2>&1
echo "9 $diracContextPath" >> /var/log/contextualize-script.log 2>&1
echo "10 $cvmfs_http_proxy" >> /var/log/contextualize-script.log 2>&1
echo "11 $siteName" >> /var/log/contextualize-script.log 2>&1
echo "12 $cloudDriver" >> /var/log/contextualize-script.log 2>&1
echo "13 $cpuTime" >> /var/log/contextualize-script.log 2>&1

#recording the uniqueId of the VM to be used by VM agents:
echo ${uniqueId} > /etc/VMID

# vmcert and key have been previoslly copy to VM, these paths are local, the rest of files are on some repo... 
# 1) download the necesary files:
wget --no-check-certificate -O ${localVmRunJobAgent} ${vmRunJobAgent} >> /var/log/contextualize-script.log 2>&1
wget --no-check-certificate -O ${localVmRunVmMonitorAgent} ${vmRunVmMonitorAgent} >> /var/log/contextualize-script.log 2>&1
wget --no-check-certificate -O ${localVmRunLogJobAgent} ${vmRunLogJobAgent} >> /var/log/contextualize-script.log 2>&1
wget --no-check-certificate -O ${localVmRunLogVmMonitorAgent} ${vmRunLogVmMonitorAgent} >> /var/log/contextualize-script.log 2>&1
wget --no-check-certificate -O ${localCvmfsContextPath} ${cvmfsContextPath} >> /var/log/contextualize-script.log 2>&1
wget --no-check-certificate -O ${localDiracContextPath} ${diracContextPath} >> /var/log/contextualize-script.log 2>&1

#2) Run the cvmvfs contextualization script:    
if [ ${cvmfsContextPath} != 'NONE' ]
then
    chmod u+x ${localCvmfsContextPath} >> /var/log/contextualize-script.log 2>&1
    bash ${localCvmfsContextPath} "${cvmfs_http_proxy}" >> /var/log/contextualize-script.log 2>&1
fi

#3) Run the dirac contextualization script:    
chmod u+x ${localDiracContextPath} >> /var/log/contextualize-script.log 2>&1
bash ${localDiracContextPath} "${siteName}" "${vmCertPath}" "${vmKeyPath}" "${localVmRunJobAgent}" "${localVmRunVmMonitorAgent}" "${localVmRunLogJobAgent}" "${localVmRunLogVmMonitorAgent}" "${cloudDriver}" "${cpuTime}">> /var/log/contextualize-script.log 2>&1

exit 0
