#!/bin/bash
########################################################################
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################
# alternative to python script (remote run con Cernvm seems to fail on python call)
# contextualization script to be run on the VM, after init.d proccess 


if [ $# -ne 12 ]
then
	echo "bash contextualize-script.bash  <certfile> <keyfile> <runjobagent> <runvmmonitoragent> <runlogjobagent> <runlogvmmonitoragent> <cvmfscontextscript> <diraccontextscript> <cvmfshttpproxy> <sitename> <clouddriver> <uniqueid>"
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
siteName=${10}
cloudDriver=${11}
uniqueId=${12}

localVmRunJobAgent=/root/run.job-agent
localVmRunVmMonitorAgent=/root/run.vm-monitor-agent
localVmRunLogJobAgent=/root/run.log.job-agent 
localVmRunLogVmMonitorAgent=/root/run.log.vm-monitor-agent
localCvmfsContextPath=/root/cvmfs-context.sh
localDiracContextPath=/root/dirac-context.sh

# parameters log:

echo "1 $vmCertPath" >> /var/log/contextualize-script.log 2>&1
echo "2 $vmKeyPath" >> /var/log/contextualize-script.log 2>&1
echo "3 $vmRunJobAgent" >> /var/log/contextualize-script.log 2>&1
echo "4 $vmRunVmMonitorAgent" >> /var/log/contextualize-script.log 2>&1
echo "5 $vmRunLogJobAgent" >> /var/log/contextualize-script.log 2>&1
echo "6 $vmRunLogVmMonitorAgent" >> /var/log/contextualize-script.log 2>&1
echo "7 $cvmfsContextPath" >> /var/log/contextualize-script.log 2>&1
echo "8 $diracContextPath" >> /var/log/contextualize-script.log 2>&1
echo "9 $cvmfs_http_proxy" >> /var/log/contextualize-script.log 2>&1
echo "10 $siteName" >> /var/log/contextualize-script.log 2>&1
echo "11 $cloudDriver" >> /var/log/contextualize-script.log 2>&1
echo "12 $uniqueId" >> /var/log/contextualize-script.log 2>&1

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
bash ${localDiracContextPath} "${siteName}" "${vmCertPath}" "${vmKeyPath}" "${localVmRunJobAgent}" "${localVmRunVmMonitorAgent}" "${localVmRunLogJobAgent}" "${localVmRunLogVmMonitorAgent}" "${cloudDriver}">> /var/log/contextualize-script.log 2>&1

exit 0
