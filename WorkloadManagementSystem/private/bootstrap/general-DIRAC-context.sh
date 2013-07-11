#!/bin/bash
#
# VMDIRAC general dirac contextualization script 
# To be run as root on VM
#

        echo "Starting dirac-context-script.sh" > /var/log/dirac-context-script.log 2>&1

if [ $# -ne 9 ]
then
    echo "ERROR: general-DIRAC-context.bash <siteName> <vmStopPolicy> <putCertPath> <putKeyPath> <localVmRunJobAgent> <localVmRunVmMonitorAgent> <localVmRunVmUpdaterAgent> <localVmRunLogAgent> <cloudDriver> " > /var/log/dirac-context-script.log 2>&1
    exit 1
fi

siteName=$1
vmStopPolicy=$2
putCertPath=$3
putKeyPath=$4
localVmRunJobAgent=$5
localVmRunVmMonitorAgent=$6
localVmRunVmUpdaterAgent=$7
localVmRunLogAgent=$8
cloudDriver=$9

cpuTime=`cat /etc/CPU_TIME`

# dirac user:
        /usr/sbin/useradd -d /opt/dirac dirac
# To work wiht the cmvfs LB_LOGIN of LHCb:
        chmod g+w /root
        chown root:dirac /root

# servercert/serverkey previouslly to this script copied 
#
	cd /opt/dirac
	su dirac -c'mkdir -p etc/grid-security' >> /var/log/dirac-context-script.log 2>&1
	chmod -R 755 etc >> /var/log/dirac-context-script.log 2>&1
	mv ${putCertPath} etc/grid-security/servercert.pem >> /var/log/dirac-context-script.log 2>&1
	chmod 444 /root/servercert.pem >> /var/log/dirac-context-script.log 2>&1
	mv ${putKeyPath} etc/grid-security/serverkey.pem >> /var/log/dirac-context-script.log 2>&1
	chmod 400 /root/serverkey.pem >> /var/log/dirac-context-script.log 2>&1
	chown dirac:dirac etc >> /var/log/dirac-context-script.log 2>&1
	
#
# Installing DIRAC
# FOR DEBUGGIN PURPOSES installing debuggin github version instead of cvmfs repository released DIRAC:
#
	cd /opt/dirac
	wget --no-check-certificate -O dirac-install 'https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py' >> /var/log/dirac-context-script.log 2>&1
	# target: su dirac -c'python dirac-install -V "VMDIRAC"'
	# label VMDIRAC it is declared at cern central installation info, linked to:
	# have a look to: http://lhcweb.pic.es/~vmendez/dirac/vmdirac.cfg

	su dirac -c'python dirac-install -V "VMDIRAC"' >> /var/log/dirac-context-script.log 2>&1
	# FOR DEBUGGIN PURPOSES overwriting with last released in the local vmendez git folder: 
        rm -rf VMDIRAC
        wget --no-check-certificate -O vmdirac.zip 'https://github.com/vmendez/VMDIRAC/archive/multi-endpoint.zip'
	unzip vmdirac.zip >> /var/log/dirac-context-script.log 2>&1
        mv VMDIRAC-multi-endpoint VMDIRAC
	chown -R dirac:dirac VMDIRAC
	cd VMDIRAC
	for i in `find . -name "*pyo"`
	do 
		chown root:root $i
	done
	cd /opt/dirac

        source bashrc >> /var/log/dirac-context-script.log 2>&1
        # to the runsvdir stuff:
	export PATH
	export LD_LIBRARY_PATH
        # for the VM Monitor
        echo "Installing easy_install simplejson for the VM Monitor" >> /var/log/dirac-context-script.log 2>&1
        easy_install simplejson >> /var/log/dirac-context-script.log 2>&1
	# also the options for the agents: CPUTime, Occi SumbitPools, Site...
        # if CAs are not download we retry
        for retry in 0 1 2 3 4 5 6 7 8 9
        do
                # multi-endpoint:
		# FIX: CPUTime should be cloudenpoint parameter
		su dirac -c"dirac-configure -UHdd -o /LocalSite/SubmitPool=Cloud -o /LocalSite/CPUTime=${cpuTime} -o /LocalSite/CloudDriver=${cloudDriver} -o /LocalSite/Site=${siteName}  -o /LocalSite/VMStopPolicy=${vmStopPolicy}  -o /LocalSite/CE=CE-nouse defaults-VMDIRAC.cfg"  >> /var/log/dirac-context-script.log 2>&1
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
        su dirac -c'sed "s/UseServerCertificate = yes/#UseServerCertificate = yes/" etc/dirac.cfg > dirac.cfg.aux'
        su dirac -c'cp etc/dirac.cfg dirac.cfg.postconfigure'
	su dirac -c'mv dirac.cfg.aux etc/dirac.cfg'
	echo "etc/dirac.cfg content previous to agents run: "  >> /var/log/dirac-context-script.log 2>&1
	cat etc/dirac.cfg >> /var/log/dirac-context-script.log 2>&1
	echo >> /var/log/dirac-context-script.log 2>&1

# start the agents: VirtualMachineMonitor, JobAgent, VirtualMachineConfigUpdater

	cd /opt/dirac
	mkdir -p startup/WorkloadManagement_JobAgent/log >> /var/log/dirac-context-script.log 2>&1
	mkdir -p startup/WorkloadManagement_VirtualMachineMonitorAgent/log >> /var/log/dirac-context-script.log 2>&1
	mkdir -p startup/WorkloadManagement_VirtualMachineConfigUpdater/log >> /var/log/dirac-context-script.log 2>&1
	mv ${localVmRunJobAgent} startup/WorkloadManagement_JobAgent/run >> /var/log/dirac-context-script.log 2>&1
	cp ${localVmRunLogAgent} startup/WorkloadManagement_JobAgent/log/run >> /var/log/dirac-context-script.log 2>&1
	mv ${localVmRunVmMonitorAgent} startup/WorkloadManagement_VirtualMachineMonitorAgent/run >> /var/log/dirac-context-script.log 2>&1
	cp ${localVmRunLogAgent} startup/WorkloadManagement_VirtualMachineMonitorAgent/log/run >> /var/log/dirac-context-script.log 2>&1
	mv ${localVmRunVmUpdaterAgent} startup/WorkloadManagement_VirtualMachineConfigUpdater/run >> /var/log/dirac-context-script.log 2>&1
	cp ${localVmRunLogAgent} startup/WorkloadManagement_VirtualMachineConfigUpdater/log/run >> /var/log/dirac-context-script.log 2>&1

	chmod 755 startup/WorkloadManagement_JobAgent/log/run 
	chmod 755 startup/WorkloadManagement_JobAgent/run 
	chmod 755 startup/WorkloadManagement_VirtualMachineMonitorAgent/log/run 
	chmod 755 startup/WorkloadManagement_VirtualMachineMonitorAgent/run 
	chmod 755 startup/WorkloadManagement_VirtualMachineConfigUpdater/log/run 
	chmod 755 startup/WorkloadManagement_VirtualMachineConfigUpdater/run 

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

exit $RETVAL
