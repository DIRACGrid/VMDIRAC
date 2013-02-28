#!/bin/bash
#
# cvmfs configuration for lhcb repository: 
# to be run as root on the VM
#
#

mkdir /home/cache >> /var/log/cvmfs-context-script.log 2>&1
chown cvmfs:cvmfs /home/cache >> /var/log/cvmfs-context-script.log 2>&1

	cat<<EOF>/etc/cvmfs/default.local
CVMFS_CACHE_BASE=/home/cache
CVMFS_REPOSITORIES=lhcb,lhcb-conddb
CVMFS_HTTP_PROXY=TEMPLATE_VALUE
EOF

	cat<<EOF>/etc/cvmfs/domain.d/cern.ch.local
CVMFS_SERVER_URL="http://cvmfs-stratum-one.cern.ch/opt/lhcb-conddb;http://cernvmfs.gridpp.rl.ac.uk/opt/lhcb-conddb;http://cvmfs02.grid.sinica.edu.tw/opt/lhcb-conddb"
CVMFS_PUBLIC_KEY=/etc/cvmfs/keys/cern.ch.pub:/etc/cvmfs/keys/cern-it1.cern.ch.pub:/etc/cvmfs/keys/cern-it3.cern.ch.pub
EOF

	cat <<EOF>/etc/cvmfs/config.d/lhcb.cern.ch.local 
CVMFS_QUOTA_LIMIT=5500
CVMFS_QUOTA_THRESHOLD=5000
EOF

	cat <<EOF>/etc/cvmfs/config.d/lhcb-conddb.cern.ch.local 
CVMFS_QUOTA_LIMIT=2000
CVMFS_QUOTA_THRESHOLD=1500
EOF

# reaload configuration to activate new setup:
/sbin/service cvmfs reload >> /var/log/cvmfs-context-script.log 2>&1
cvmfs_config showconfig >> /var/log/cvmfs-context-script.log 2>&1

exit 0
