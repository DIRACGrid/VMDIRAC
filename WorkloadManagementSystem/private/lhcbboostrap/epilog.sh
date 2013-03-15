#!/bin/bash
set -o xtrace
echo Running epilog.sh
logger -t \$0 "epilog running"
    
service cvmfs probe
logger -t \$0 "cvmfs probed"
       
logger -t \$0 "installing runsvdir-start"

runStart=/sbin/runsvdir-start

echo '#!/bin/sh' > \$runStart 
echo 'PATH=/command:/usr/local/bin:/usr/local/sbin:/bin:/sbin:/usr/bin:/usr/sbin:/usr/X11R6/bin' >> \$runStart
#echo 'exec 2>&1 \' >> \$runStart
echo 'exec env - PATH=$PATH \' >> \$runStart
echo 'runsvdir -P /opt/dirac/startup "log: DIRAC"' >> \$runStart
chmod 750 \$runStart
    
echo 'SV:123456:respawn:/sbin/runsvdir-start' >> /etc/inittab
       
#Re-evaluating inittab
init q