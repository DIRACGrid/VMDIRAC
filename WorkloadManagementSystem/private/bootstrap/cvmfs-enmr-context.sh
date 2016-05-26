#!/bin/bash
#
# cvmfs configuration for lhcb repository: 
# to be run as root on the VM
#
#

if [ $# -ne 1 ]
then
	echo "cvmfs-enmr-context.sh <cvmfs_http_proxy>"
fi	

echo "received cvmfs_http_proxy parameter $1" >> /var/log/cvmfs-context-script.log 2>&1
rpm --import https://cvmrepo.web.cern.ch/cvmrepo/yum/RPM-GPG-KEY-CernVM >> /var/log/cvmfs-context-script.log 2>&1
yum -y install https://ecsft.cern.ch/dist/cvmfs/cvmfs-release/cvmfs-release-latest.noarch.rpm >> /var/log/cvmfs-context-script.log 2>&1
yum -y update >> /var/log/cvmfs-context-script.log 2>&1
echo "install cvmfs and cvmfs-config-default" >> /var/log/cvmfs-context-script.log 2>&1
yum -y install cvmfs cvmfs-config-default >> /var/log/cvmfs-context-script.log 2>&1



	cat<<EOF>/etc/cvmfs/default.local
CVMFS_SERVER_URL="http://cvmfs-egi.gridpp.rl.ac.uk:8000/cvmfs/@fqrn@;http://klei.nikhef.nl:8000/cvmfs/@fqrn@;http://cvmfsrepo.lcg.triumf.ca:8000/cvmfs/@fqrn@;http://cvmfsrep.grid.sinica.edu.tw:8000/cvmfs/@fqrn@"
CVMFS_KEYS_DIR=/etc/cvmfs/keys/egi.eu
CVMFS_CACHE_BASE=/home/cache
CVMFS_REPOSITORIES=wenmr.egi.eu
CVMFS_QUOTA_LIMIT=4000
CVMFS_HTTP_PROXY="$1"
EOF


#alternative setup
#CVMFS_SERVER_URL="http://cvmfs-egi.gridpp.rl.ac.uk:8000/cvmfs/@org@.gridpp.ac.uk;http://cvmfs01.nikhef.nl/cvmfs/@org@.gridpp.ac.uk"
#CVMFS_PUBLIC_KEY=/etc/cvmfs/keys/gridpp.ac.uk.pub

#alternative setup needs this:
#	cat<<EOF>/etc/cvmfs/keys/gridpp.ac.uk.pub
#-----BEGIN PUBLIC KEY-----
#MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAp7C4KDvOIEVJepuAHjxE
#EES1sDdohz0hiU6uvSqxVYjKVR4Y4/0I/D/zLijQI+MHR7859RN0/6fsZ3b3At3l
#UbvNfqq6DN1zVjjd0xagC6SMBhSfj/iQKQSsG8MXSyiNmM8YalVHJSPqoova6CPE
#EgLEjnHKTNEogTNjKBwbP2ELPLkfVoNoxxrXPSox7aln8JdgyZzZlBwm98gnFa1v
#JTVAl0HQnUJ6cjMwO31wIGVMdvZ+P962t+2bPGfOCm6Ly6BusXcLoIIeez5SBerB
#aHz//NSTZDbHVNPEqpoo1AQVVOo4XJmqo64jBa3G4Dr0zSda1bkZMVhsyUtjhfEB
#DwIDAQAB
#-----END PUBLIC KEY-----
#EOF

	cat<<EOF>/etc/fuse.conf
user_allow_other
EOF

# reaload configuration to activate new setup:
cvmfs_config reload >> /var/log/cvmfs-context-script.log 2>&1
cvmfs_config showconfig >> /var/log/cvmfs-context-script.log 2>&1
service autofs start >> /var/log/cvmfs-context-script.log 2>&1
chkconfig autofs on >> /var/log/cvmfs-context-script.log 2>&1
# just for testing:
#cvmfs_config chksetup >> /var/log/cvmfs-context-script.log 2>&1
mkdir -p /cvmfs/wenmr.egi.eu
mount -t cvmfs wenmr.egi.eu /cvmfs/wenmr.egi.eu >> /var/log/cvmfs-context-script.log 2>&1
export VO_ENMR_EU_SW_DIR=/cvmfs/wenmr.egi.eu


exit 0
