#!/bin/bash
#
# lhcbdirac : LHCbDIRAC as a Service
#
# @ubeda
#

# Source function library
#. /etc/init.d/hepix_functions

STARTUP=/opt/dirac/startup

start(){

  set -o xtrace
  logger -t \$0 "lhcbdirac starting"
  
  echo Starting
  echo "\`ls \$STARTUP | tr ' ' '\n'\`"
  
  logger -t \$0 "Runsvctrl u"
  runsvctrl u \$STARTUP/*
  RETVAL=\$?
    
  logger -t \$0 "lhcbdirac started"
    
  return \$RETVAL
}

restart(){

  echo "Making sure cvmfs works is mounted"
  service cvmfs probe
 
  export DIRACSYSCONFIG=/opt/dirac/etc/dirac.cfg

  echo "Restarting agents..."
  echo "\`ls \$STARTUP | tr ' ' '\n'\`"
  runsvctrl t \$STARTUP/*
  
  RETVAL=\$?
  return \$RETVAL
}

stop(){

  echo "Stopping lhcbdirac ..."
  
  runsvctrl d \$STARTUP/*

  RETVAL=\$?
  return \$RETVAL
}

stopagent(){

  echo "Stopping lhcbdirac agents..."
  
  touch /opt/dirac/control/*/*/stop_agent

  RETVAL=\$?
  return \$RETVAL
}

case "\$1" in
  start)
    start
    RETVAL=\$?
    ;;
  restart)
    restart
    RETVAL=\$?
    ;;
  stop)
    stop
    RETVAL=\$?
    ;;
  stopagent)
    stopagent
    RETVAL=\$?
    ;;    
  *)
    echo $"Usage: \$0 {start|restart|stop|stopagent}"
    RETVAL=2
esac

exit \$RETVAL