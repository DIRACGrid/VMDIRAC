#!/bin/bash
#
# dirac contextualization script 
# To be run as root on VM
#


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

install_unzip() {
    get_packaging_system
    [ ! -z $PACKAGE_MANAGER ] && $PACKAGE_MANAGER -y update
    [ ! -z $PACKAGE_MANAGER ] && $PACKAGE_MANAGER -y install unzip
}

install_easy_install() {
    get_packaging_system
    [ ! -z $PACKAGE_MANAGER ] && $PACKAGE_MANAGER -y install python-setuptools
}

check_python_dev() {
    get_packaging_system
    if [ $PACKAGE_MANAGER="yum" ]
    then
       pdev=`rpm -qa|grep python-dev|wc -l`
    elif [ $PACKAGE_MANAGER="apt-get" ]
    then
       pdev=`dpkg -l|grep python-dev|wc -l`
    else
        echo "Package manager not implemented."
    fi
    if [ $pdev -ne 0 ] 
    then
       [ ! -z $PACKAGE_MANAGER ] && $PACKAGE_MANAGER -y install python-dev
    fi
}


        echo "Starting dirac-context-script.sh" > /var/log/dirac-context-script.log 2>&1


if [ $# -ne 9 ]
then
    echo "ERROR: Given $# parameters" >> /var/log/dirac-context-script.log 2>&1
    echo "       Given parameters: $@" >> /var/log/dirac-context-script.log 2>&1
    echo "       Required parameters: general-DIRAC-context.sh '<siteName>' '<vmStopPolicy>' '<putCertPath>' '<putKeyPath>' '<localVmRunJobAgent>' '<localVmRunVmMonitorAgent>' '<localVmRunVmUpdaterAgent>' '<localVmRunLogAgent>' '<cloudDriver>'" >> /var/log/dirac-context-script.log 2>&1
    exit 1
fi

siteName=${1}
vmStopPolicy=${2}
putCertPath=${3}
putKeyPath=${4}
localVmRunJobAgent=${5}
localVmRunVmMonitorAgent=${6}
localVmRunVmUpdaterAgent=${7}
localVmRunLogAgent=${8}
cloudDriver=${9}

echo "Running dirac-contex.sh '<siteName>' '<vmStopPolicy>' '<putCertPath>' '<putKeyPath>' '<localVmRunJobAgent>' '<localVmRunVmMonitorAgent>' '<localVmRunVmUpdaterAgent>' '<localVmRunLogAgent>' '<cloudDriver>'" >> /var/log/dirac-context-script.log 2>&1
echo "1 $siteName" >> /var/log/dirac-context-script.log 2>&1
echo "2 $vmStopPolicy" >> /var/log/dirac-context-script.log 2>&1
echo "3 $putCertPath" >> /var/log/dirac-context-script.log 2>&1
echo "4 $putKeyPath" >> /var/log/dirac-context-script.log 2>&1
echo "5 $localVmRunJobAgent" >> /var/log/dirac-context-script.log 2>&1
echo "6 $localVmRunVmMonitorAgent" >> /var/log/dirac-context-script.log 2>&1
echo "7 $localVmRunVmUpdaterAgent" >> /var/log/dirac-context-script.log 2>&1
echo "8 $localVmRunLogAgent" >> /var/log/dirac-context-script.log 2>&1
echo "9 $cloudDriver" >> /var/log/dirac-context-script.log 2>&1

# dirac user:
# Ubuntu14-hg19 already has a dirac user
#        /usr/sbin/useradd -m -s /bin/bash -d /opt/dirac dirac >> /var/log/dirac-context-script.log 2>&1

# breakseq tgz software stack:
echo "mount disk and untargz breakseq-stack.tgz" >> /var/log/dirac-context-script.log 2>&1
echo "breakseq software stack requires 8GB free in /mnt + input and output data to process" >> /var/log/dirac-context-script.log 2>&1
mount /dev/xvdb /mnt >> /var/log/dirac-context-script.log 2>&1
#cd /
#tar xzvf /home/ubuntu/breakseq-stack.tgz >> /var/log/dirac-context-script.log 2>&1
#rm -f /home/ubuntu/breakseq-stack.tgz >> /var/log/dirac-context-script.log 2>&1
cd /mnt
tar xzvf /home/ubuntu/genome-opt.tgz >> /var/log/dirac-context-script.log 2>&1
# tar should be of dirac user, just in case:
chown dirac.dirac /mnt/dirac >> /var/log/dirac-context-script.log 2>&1
# here /opt/dirac and /opt/breakseq ad others are linked to /mnt
rm -f /home/ubuntu/genome-opt.tgz >> /var/log/dirac-context-script.log 2>&1

mkdir /mnt/tmp >> /var/log/dirac-context-script.log 2>&1
rm -rf /tmp >> /var/log/dirac-context-script.log 2>&1
ln -s /mnt/tmp /tmp >> /var/log/dirac-context-script.log 2>&1

# servercert/serverkey previouslly to this script copied 
#
	cd /opt/dirac
        echo "pwd should be /opt/dirac" >> /var/log/dirac-context-script.log 2>&1
	pwd >> /var/log/dirac-context-script.log 2>&1
	mkdir -p etc/grid-security >> /var/log/dirac-context-script.log 2>&1
	chmod -R 755 etc >> /var/log/dirac-context-script.log 2>&1
	mv ${putCertPath} etc/grid-security/servercert.pem >> /var/log/dirac-context-script.log 2>&1
	mv ${putKeyPath} etc/grid-security/serverkey.pem >> /var/log/dirac-context-script.log 2>&1

	sleep 1

	chmod 444 etc/grid-security/servercert.pem >> /var/log/dirac-context-script.log 2>&1
	chmod 400 etc/grid-security/serverkey.pem >> /var/log/dirac-context-script.log 2>&1

	chown -R dirac:dirac etc >> /var/log/dirac-context-script.log 2>&1
	
#
# Installing DIRAC
#
	cd /opt/dirac
        wget --no-check-certificate -O dirac-install 'http://dirac1.grid.cyfronet.pl:8088/repo/integration/DIRAC/Core/scripts/dirac-install.py' >> /var/log/dirac-context-script.log 2>&1
	su dirac -c'python dirac-install -V "VMEGI"' >> /var/log/dirac-context-script.log 2>&1

	# FOR DEBUGGIN PURPOSES overwriting with last released in the local vmendez folder: 
        rm -rf VMDIRAC
        wget --no-check-certificate -O vmdirac.zip 'http://dirac1.grid.cyfronet.pl:8088/repo/vmendez/master.zip' >> /var/log/dirac-context-script.log 2>&1
        # checking if unzip installed
        if [ ! `which unzip` ]
        then
          install_unzip
        fi
	unzip vmdirac.zip >> /var/log/dirac-context-script.log 2>&1
        mv VMDIRAC-master VMDIRAC
	chown -R dirac:dirac VMDIRAC
	cd VMDIRAC
	for i in `find . -name "*pyo"`
	do 
		chown root:root $i
	done
	cd /opt/dirac

        source bashrc >> /var/log/dirac-context-script.log 2>&1
        env >> /var/log/dirac-context-script.log 2>&1
        chmod ugo+w /var/log/dirac-context-script.log 

        # to the runsvdir stuff:
	export PATH
	export LD_LIBRARY_PATH
        platform=`dirac-platform`
        # for the VM Monitor
        # checking if python-dev installed
        check_python_dev
        # checking if easy_install installed
        if [ ! `which easy_install` ]
        then
                echo "easy_install not installed. Installing">> /var/log/dirac-context-script.log 2>&1
                install_easy_install
        fi
        echo "Installing easy_install simplejson for the VM Monitor" >> /var/log/dirac-context-script.log 2>&1
        `which python` `which easy_install` simplejson >> /var/log/dirac-context-script.log 2>&1
        # getting RunningPodRequirements
        requirements=''
        while read keyval           
        do           
            requirements=`echo "$requirements -o /LocalSite/$keyval"`
        done </root/LocalSiteRequirements
        # configure, if CAs are not download we retry
        for retry in 0 1 2 3 4 5 6 7 8 9
        do
		su dirac -c"source bashrc;dirac-configure -UHddd $requirements -o /LocalSite/CloudDriver=$cloudDriver -o /LocalSite/Site=$siteName  -o /LocalSite/VMStopPolicy=$vmStopPolicy  -o /LocalSite/CE=CE-nouse defaults-VMEGI.cfg"  >> /var/log/dirac-context-script.log 2>&1
		# options H: SkipCAChecks, dd: debug level 2, U: UseServerCertificate 
		# options only for debuging D: SkipCADownload
		# after UseServerCertificate = yes for the configuration with CS
		if [ `ls /opt/dirac/etc/grid-security/certificates | wc -l` -ne 0 ]
		then
			echo "certificates download in dirac-configure at retry: $retry"  >> /var/log/dirac-context-script.log 2>&1
			break
		fi
		echo "certificates was not download in dirac-configure at retry: $retry"  >> /var/log/dirac-context-script.log 2>&1
	done
	# we have to change to allow user proxy delegation for agents:
        sed "s/UseServerCertificate = yes/#UseServerCertificate = yes/" etc/dirac.cfg > dirac.cfg.aux
        cp etc/dirac.cfg dirac.cfg.postconfigure
	mv dirac.cfg.aux etc/dirac.cfg
	echo "etc/dirac.cfg content previous to agents run: "  >> /var/log/dirac-context-script.log 2>&1
	cat etc/dirac.cfg >> /var/log/dirac-context-script.log 2>&1
	echo >> /var/log/dirac-context-script.log 2>&1


# start the agents: VirtualMachineMonitor, JobAgent, VirtualMachineConfigUpdater

	cd /opt/dirac
        if [ ${localVmRunJobAgent} != 'nouse' ]
        then
	  mkdir -p startup/WorkloadManagement_JobAgent/log >> /var/log/dirac-context-script.log 2>&1
	  mv ${localVmRunJobAgent} startup/WorkloadManagement_JobAgent/run >> /var/log/dirac-context-script.log 2>&1
	  cp ${localVmRunLogAgent} startup/WorkloadManagement_JobAgent/log/run >> /var/log/dirac-context-script.log 2>&1
	  chmod 755 startup/WorkloadManagement_JobAgent/log/run 
          chmod 755 startup/WorkloadManagement_JobAgent/run 

	  echo "rights and permissions to control and work JobAgent dirs" >> /var/log/dirac-context-script.log 2>&1
	  mkdir -p /opt/dirac/control/WorkloadManagement/JobAgent >> /var/log/dirac-context-script.log 2>&1
	  mkdir -p /opt/dirac/work/WorkloadManagement/JobAgent >> /var/log/dirac-context-script.log 2>&1
	  chmod 775 /opt/dirac/control/WorkloadManagement/JobAgent >> /var/log/dirac-context-script.log 2>&1
	  chmod 775 /opt/dirac/work/WorkloadManagement/JobAgent >> /var/log/dirac-context-script.log 2>&1
	  chown root:dirac /opt/dirac/work/WorkloadManagement/JobAgent >> /var/log/dirac-context-script.log 2>&1
	  chown root:dirac /opt/dirac/control/WorkloadManagement/JobAgent >> /var/log/dirac-context-script.log 2>&1
	  echo "/opt/dirac/control/WorkloadManagement content" >> /var/log/dirac-context-script.log 2>&1
	  ls -l /opt/dirac/control/WorkloadManagement >> /var/log/dirac-context-script.log 2>&1
	  echo "/opt/dirac/work/WorkloadManagement content" >> /var/log/dirac-context-script.log 2>&1
	  ls -l /opt/dirac/work/WorkloadManagement >> /var/log/dirac-context-script.log 2>&1
	  echo >> /var/log/dirac-context-script.log 2>&1
        fi

        if [ ${localVmRunVmUpdaterAgent} != 'nouse' ]
        then
	  mkdir -p startup/WorkloadManagement_VirtualMachineConfigUpdater/log >> /var/log/dirac-context-script.log 2>&1
	  mv ${localVmRunVmUpdaterAgent} startup/WorkloadManagement_VirtualMachineConfigUpdater/run >> /var/log/dirac-context-script.log 2>&1
	  cp ${localVmRunLogAgent} startup/WorkloadManagement_VirtualMachineConfigUpdater/log/run >> /var/log/dirac-context-script.log 2>&1
	  chmod 755 startup/WorkloadManagement_VirtualMachineConfigUpdater/log/run 
	  chmod 755 startup/WorkloadManagement_VirtualMachineConfigUpdater/run 
        fi

	mkdir -p startup/WorkloadManagement_VirtualMachineMonitorAgent/log >> /var/log/dirac-context-script.log 2>&1
	mv ${localVmRunVmMonitorAgent} startup/WorkloadManagement_VirtualMachineMonitorAgent/run >> /var/log/dirac-context-script.log 2>&1
	mv ${localVmRunLogAgent} startup/WorkloadManagement_VirtualMachineMonitorAgent/log/run >> /var/log/dirac-context-script.log 2>&1
	chmod 755 startup/WorkloadManagement_VirtualMachineMonitorAgent/log/run 
	chmod 755 startup/WorkloadManagement_VirtualMachineMonitorAgent/run 

	echo "runsvdir startup, have a look to DIRAC JobAgent, VirtualMachineMonitorAgent and VirtualMachineConfigUpdater logs" >> /var/log/dirac-context-script.log 2>&1
	runsvdir -P /opt/dirac/startup 'log:  DIRAC runsv' &

#
# END installing DIRAC
#

# avoiding ssh conection refused:
#	echo "After DIRAC install:" >> /var/log/dirac-context-script.log 2>&1
#	ls -l /etc/ssh >> /var/log/dirac-context-script.log 2>&1
#	chmod 600 /etc/ssh/* >> /var/log/dirac-context-script.log 2>&1
#	chmod go+r /etc/ssh/ssh_config /etc/ssh/ssh_host_dsa_key.pub /etc/ssh/ssh_host_key.pub /etc/ssh/ssh_host_rsa_key.pub >> /var/log/dirac-context-script.log 2>&1
#	echo "After restoring rights:" >> /var/log/dirac-context-script.log 2>&1
#	ls -l /etc/ssh >> /var/log/dirac-context-script.log 2>&1

    #
    # STOPING DIRAC AGENTS:
    #
#    cd /opt/dirac
#    killall runsvdir
#    runsvctrl d startup/*
#    killall runsv

        echo "END dirac-context-script.sh" >> /var/log/dirac-context-script.log 2>&1

exit $RETVAL
