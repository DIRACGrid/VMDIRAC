#!/bin/bash
########################################################################
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################
# alternative to python script (remote run con Cernvm seems to fail on python call)
# contextualization script to be run on the VM, after init.d proccess 

echo "Starting /root/contextualize-script.bash" > /var/log/contextualize-script.log

if [ $# -ne 15 ]
then
        echo "Parameter ERROR: bash contextualize-script.bash  <uniqueId> <certfile> <keyfile> <runjobagent> <runvmmonitoragent> <runvmupdateragent> <runlogagent> <cvmfscontextscript> <diraccontextscript> <cvmfshttpproxy> <sitename> <clouddriver> <cpuTime> <vmStopPolicy> <submitPool>" >> /var/log/contextualize-script.log
        exit 1
fi

uniqueId=$1
vmCertPath=$2
vmKeyPath=$3
vmRunJobAgent=$4
vmRunVmMonitorAgent=$5
vmRunVmUpdaterAgent=$6
vmRunLogAgent=$7
cvmfsContextPath=$8
diracContextPath=$9
cvmfs_http_proxy=${10}
siteName=${11}
cloudDriver=${12}
cpuTime=${13}
vmStopPolicy=${14}
submitPool=${15}

if [ ${vmRunJobAgent} != 'nouse' ]
then
  localVmRunJobAgent=/root/run.job-agent
else
  localVmRunJobAgent=nouse
fi

localVmRunVmMonitorAgent=/root/run.vm-monitor-agent

if [ ${vmRunVmUpdaterAgent} != 'nouse' ]
then
  localVmRunVmUpdaterAgent=/root/run.vm-updater-agent 
else
  localVmRunVmUpdaterAgent=nouse
fi

localVmRunLogAgent=/root/run.log.agent

if [ ${cvmfsContextPath} != 'nouse' ]
then
  localCvmfsContextPath=/root/cvmfs-context.sh
else
  localCvmfsContextPath=nouse
fi

localDiracContextPath=/root/dirac-context.sh

# parameters log:

echo "1 $uniqueId" >> /var/log/contextualize-script.log 2>&1
echo "2 $vmCertPath" >> /var/log/contextualize-script.log 2>&1
echo "3 $vmKeyPath" >> /var/log/contextualize-script.log 2>&1
echo "4 $vmRunJobAgent" >> /var/log/contextualize-script.log 2>&1
echo "5 $vmRunVmMonitorAgent" >> /var/log/contextualize-script.log 2>&1
echo "6 $vmRunVmUpdaterAgent" >> /var/log/contextualize-script.log 2>&1
echo "7 $vmRunLogAgent" >> /var/log/contextualize-script.log 2>&1
echo "8 $cvmfsContextPath" >> /var/log/contextualize-script.log 2>&1
echo "9 $diracContextPath" >> /var/log/contextualize-script.log 2>&1
echo "10 $cvmfs_http_proxy" >> /var/log/contextualize-script.log 2>&1
echo "11 $siteName" >> /var/log/contextualize-script.log 2>&1
echo "12 $cloudDriver" >> /var/log/contextualize-script.log 2>&1
echo "13 $cpuTime" >> /var/log/contextualize-script.log 2>&1
echo "14 $vmStopPolicy" >> /var/log/contextualize-script.log 2>&1
echo "15 $submitPool" >> /var/log/contextualize-script.log 2>&1

#recording the uniqueId of the VM to be used by VM agents:
echo ${uniqueId} > /etc/VMID

# vmcert and key have been previoslly included in the VM, these paths are local, the rest of files are on some repo... 
# 1) download the necesary files:

if [ ${vmRunJobAgent} != 'nouse' ]
then
  wget --no-check-certificate -O ${localVmRunJobAgent} ${vmRunJobAgent} >> /var/log/contextualize-script.log 2>&1
fi

if [ ${vmRunVmMonitorAgent} != 'nouse' ]
then
  wget --no-check-certificate -O ${localVmRunVmMonitorAgent} ${vmRunVmMonitorAgent} >> /var/log/contextualize-script.log 2>&1
fi

if [ ${vmRunVmUpdaterAgent} != 'nouse' ]
then
  wget --no-check-certificate -O ${localVmRunVmUpdaterAgent} ${vmRunVmUpdaterAgent} >> /var/log/contextualize-script.log 2>&1
fi


if [ ${cvmfsContextPath} != 'nouse' ]
then
  wget --no-check-certificate -O ${localCvmfsContextPath} ${cvmfsContextPath} >> /var/log/contextualize-script.log 2>&1
fi

if [ ${vmRunLogAgent} != 'nouse' ]
then
  wget --no-check-certificate -O ${localVmRunLogAgent} ${vmRunLogAgent} >> /var/log/contextualize-script.log 2>&1
fi

if [ ${diracContextPath} != 'nouse' ]
then
  wget --no-check-certificate -O ${localDiracContextPath} ${diracContextPath} >> /var/log/contextualize-script.log 2>&1
fi

#2) Run the cvmvfs contextualization script:    
if [ ${cvmfsContextPath} != 'nouse' ]
then
    chmod u+x ${localCvmfsContextPath} >> /var/log/contextualize-script.log 2>&1
    bash ${localCvmfsContextPath} "${cvmfs_http_proxy}" >> /var/log/contextualize-script.log 2>&1
fi

#3) Run the dirac contextualization script:    

if [ ${diracContextPath} != 'nouse' ]
then
    echo "Ready for running dirac contextualize script: ${localDiracContextPath}" >> /var/log/contextualize-script.log 2>&1
    echo "    Parameters: ${siteName} ${vmStopPolicy} ${vmCertPath} ${vmKeyPath} ${localVmRunJobAgent} ${localVmRunVmMonitorAgent} ${localVmRunVmUpdaterAgent} ${localVmRunLogAgent} ${submitPool} ${cpuTime} ${cloudDriver}" >> /var/log/contextualize-script.log 2>&1

    chmod u+x ${localDiracContextPath} >> /var/log/contextualize-script.log 2>&1
    bash ${localDiracContextPath} ${siteName} ${vmStopPolicy} ${vmCertPath} ${vmKeyPath} ${localVmRunJobAgent} ${localVmRunVmMonitorAgent} ${localVmRunVmUpdaterAgent} ${localVmRunLogAgent} ${submitPool} ${cpuTime} ${cloudDriver}
else
    echo "Context configured with 'nouse' of dirac contextualize script" >> /var/log/contextualize-script.log 2>&1
fi

echo "END /root/contextualize-script.bash" >> /var/log/contextualize-script.log

exit 0
