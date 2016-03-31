#!/bin/bash
#
# cvmfs configuration for lhcb repository: 
# to be run as root on the VM
#
#

if [ $# -ne 1 ]
then
	echo "cvmfs-LHCb-context.sh <cvmfs_http_proxy>"
fi	


mkdir /home/cache >> /var/log/cvmfs-context-script.log 2>&1
chown cvmfs:cvmfs /home/cache >> /var/log/cvmfs-context-script.log 2>&1

	cat<<EOF>/etc/cvmfs/default.local
CVMFS_CACHE_BASE=/home/cache
CVMFS_REPOSITORIES=enmr.eu
CVMFS_HTTP_PROXY=$1
EOF

	cat<<EOF>/etc/cvmfs/domain.d/gridpp.ac.uk.conf
CVMFS_SERVER_URL="http://cvmfs-egi.gridpp.rl.ac.uk:8000/cvmfs/@org@.gridpp.ac.uk;http://cvmfs01.nikhef.nl/cvmfs/@org@.gridpp.ac.uk"
CVMFS_PUBLIC_KEY=/etc/cvmfs/keys/gridpp.ac.uk.pub
EOF

#	cat <<EOF>/etc/cvmfs/config.d/lhcb.cern.ch.local 
#CVMFS_QUOTA_LIMIT=5500
#CVMFS_QUOTA_THRESHOLD=5000
#EOF

# reaload configuration to activate new setup:
/sbin/service cvmfs reload >> /var/log/cvmfs-context-script.log 2>&1
cvmfs_config showconfig >> /var/log/cvmfs-context-script.log 2>&1

exit 0
