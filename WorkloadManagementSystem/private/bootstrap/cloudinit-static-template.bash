# This is a base template with the static part of a bash script for cloudinit
# It is completed with dynamic clouinit part at BuildClouinitScript.py
########################################################################
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################

echo "Starting /root/contextualize-script.bash" > /var/log/contextualize-script.log

if [ ${instanceID} != None ]
then
  echo ${instanceID}>/root/VMDIRAC_instanceID
fi

if [ ${vmRunJobAgentURL} != 'nouse' ]
then
  localVmRunJobAgent=/root/run.job-agent
else
  localVmRunJobAgent=nouse
fi

localVmRunVmMonitorAgent=/root/run.vm-monitor-agent

if [ ${vmRunVmUpdaterAgentURL} != 'nouse' ]
then
  localVmRunVmUpdaterAgent=/root/run.vm-updater-agent 
else
  localVmRunVmUpdaterAgent=nouse
fi

localVmRunLogAgent=/root/run.log.agent

if [ ${vmCvmfsContextURL} != 'nouse' ]
then
  localCvmfsContextPath=/root/cvmfs-context.sh
else
  localCvmfsContextPath=nouse
fi

localDiracContextPath=/root/dirac-context.sh

# parameters log:

echo "putCertPath $putCertPath" >> /var/log/contextualize-script.log 2>&1
echo "putKeyPath $putKeyPath" >> /var/log/contextualize-script.log 2>&1
echo "vmRunJobAgentURL $vmRunJobAgentURL" >> /var/log/contextualize-script.log 2>&1
echo "vmRunVmMonitorAgentURL $vmRunVmMonitorAgentURL" >> /var/log/contextualize-script.log 2>&1
echo "vmRunVmUpdaterAgentURL $vmRunVmUpdaterAgentURL" >> /var/log/contextualize-script.log 2>&1
echo "vmRunLogAgentURL $vmRunLogAgentURL" >> /var/log/contextualize-script.log 2>&1
echo "vmCvmfsContextURL $vmCvmfsContextURL" >> /var/log/contextualize-script.log 2>&1
echo "vmDiracContextURL $vmDiracContextURL" >> /var/log/contextualize-script.log 2>&1
echo "cvmfs_http_proxy $cvmfs_http_proxy" >> /var/log/contextualize-script.log 2>&1
echo "siteName $siteName" >> /var/log/contextualize-script.log 2>&1
echo "cloudDriver $cloudDriver" >> /var/log/contextualize-script.log 2>&1
echo "vmStopPolicy $vmStopPolicy" >> /var/log/contextualize-script.log 2>&1

#TODO: remember the unique_id stuff !!

# vmcert and key have been previoslly copy to VM, these paths are local, the rest of files are on some repo... 
# 1) download the necesary files:

#<<<<<<< HEAD
#if [ ! `which wget` ]
#then
#  rpm --rebuilddb >> /var/log/contextualize-script.log 2>&1
#  yum clean all >> /var/log/contextualize-script.log 2>&1
#  yum -y update >> /var/log/contextualize-script.log 2>&1
#  yum -y install wget >> /var/log/contextualize-script.log 2>&1
#=======
get_packaging_system() {
    YUM_CMD=$(which yum)
    APT_GET_CMD=$(which apt-get)

    if [ ! -z $YUM_CMD ]
    then
        echo "RedHat based"
        PACKAGE_MANAGER="yum"
    elif [ ! -z $APT_GET_CMD ]
    then
        echo "Debian based"
        PACKAGE_MANAGER="apt-get"
    else
        echo "Package manager not implemented."
    fi
}

install_wget() {
    get_packaging_system
    [ ! -z $PACKAGE_MANAGER ] && $PACKAGE_MANAGER -y install wget

}

if [ ! `which wget` ]
then
  echo "Wget not installed. Installing"
  install_wget
#>>>>>>> upstream/integration
fi

if [ ${vmRunJobAgentURL} != 'nouse' ]
then
  wget --no-check-certificate -O ${localVmRunJobAgent} ${vmRunJobAgentURL} >> /var/log/contextualize-script.log 2>&1
fi

if [ ${vmRunVmMonitorAgentURL} != 'nouse' ]
then
  wget --no-check-certificate -O ${localVmRunVmMonitorAgent} ${vmRunVmMonitorAgentURL} >> /var/log/contextualize-script.log 2>&1
fi

if [ ${vmRunVmUpdaterAgentURL} != 'nouse' ]
then
  wget --no-check-certificate -O ${localVmRunVmUpdaterAgent} ${vmRunVmUpdaterAgentURL} >> /var/log/contextualize-script.log 2>&1
fi


if [ ${vmCvmfsContextURL} != 'nouse' ]
then
  wget --no-check-certificate -O ${localCvmfsContextPath} ${vmCvmfsContextURL} >> /var/log/contextualize-script.log 2>&1
fi

if [ ${vmRunLogAgentURL} != 'nouse' ]
then
  wget --no-check-certificate -O ${localVmRunLogAgent} ${vmRunLogAgentURL} >> /var/log/contextualize-script.log 2>&1
fi

if [ ${vmDiracContextURL} != 'nouse' ]
then
  wget --no-check-certificate -O ${localDiracContextPath} ${vmDiracContextURL} >> /var/log/contextualize-script.log 2>&1
fi

#2) Run the cvmvfs contextualization script:    
if [ ${vmCvmfsContextURL} != 'nouse' ]
then
    chmod u+x ${localCvmfsContextPath} >> /var/log/contextualize-script.log 2>&1
    bash ${localCvmfsContextPath} "${cvmfs_http_proxy}" >> /var/log/contextualize-script.log 2>&1
fi

#3) Run the dirac contextualization script:    

if [ ${vmDiracContextURL} != 'nouse' ]
then
    echo "Ready for running dirac contextualize script: ${localDiracContextPath}" >> /var/log/contextualize-script.log 2>&1
    echo "    Parameters: ${siteName} ${vmStopPolicy} ${putCertPath} ${putKeyPath} ${localVmRunJobAgent} ${localVmRunVmMonitorAgent} ${localVmRunVmUpdaterAgent} ${localVmRunLogAgent} ${cloudDriver}" >> /var/log/contextualize-script.log 2>&1

    chmod u+x ${localDiracContextPath} >> /var/log/contextualize-script.log 2>&1
    bash ${localDiracContextPath} ${siteName} ${vmStopPolicy} ${putCertPath} ${putKeyPath} ${localVmRunJobAgent} ${localVmRunVmMonitorAgent} ${localVmRunVmUpdaterAgent} ${localVmRunLogAgent} ${cloudDriver}
else
    echo "Context configured with 'nouse' of dirac contextualize script" >> /var/log/contextualize-script.log 2>&1
fi

echo "END /root/contextualize-script.bash" >> /var/log/contextualize-script.log

exit 0
