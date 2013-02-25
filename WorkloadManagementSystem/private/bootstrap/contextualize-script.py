#!/usr/bin/env python
########################################################################
# $HeadURL$
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################
# contextualization script to be run on the VM, after init.d proccess 
# TODO: the same using the CernVM Contextualization method as init.d service

import os
import subprocess
import sys, getopt
import time

def main(argv):
    vmCertPath = ''
    vmKeyPath = ''
    vmRunJobAgent = ''
    localVmRunJobAgent = '/root/run.job-agent'
    vmRunVmMonitorAgent = ''
    localVmRunVmMonitorAgent = '/root/run.vm-monitor-agent'
    vmRunLogJobAgent = '' 
    localVmRunLogJobAgent = '/root/run.log.job-agent' 
    vmRunLogVmMonitorAgent = ''
    localVmRunLogVmMonitorAgent = '/root/run.log.vm-monitor-agent'
    cvmfsContextPath = ''
    localCvmfsContextPath = '/root/cvmfs-context.sh'
    diracContextPath = ''
    localDiracContextPath = '/root/dirac-context.sh'
    cvmfs_http_proxy = ''
    siteName = ''
    try:
      opts, args = getopt.getopt(argv,"hc:k:j:m:l:L:v:d:p",["certfile=","keyfile=","runjobagent=","runvmmonitoragent=","runlogagent=","cvmfscontextscript=","diraccontextscript=","cvmfshttpproxy="])
    except getopt.GetoptError:
      print 'python contextualize-script  -c <certfile> -k <keyfile> -j <runjobagent> -m <runvmmonitoragent> -l <runlogagent> -L <runlogvmmonitoragent>  -v <cvmfscontextscript> -d <diraccontextscript> -p <cvmfshttpproxy>'
      sys.exit(2)

    for opt, arg in opts:
      if opt == '-h':
        print 'python contextualize-script  -c <certfile> -k <keyfile> -j <runjobagent> -m <runvmmonitoragent> -l <runlogjobagent> -L <runlogvmmonitoragent> -v <cvmfscontextscript> -d <diraccontextscript> -p <cvmfshttpproxy> -s <sitename>'
        sys.exit()
      elif opt in ("-c", "--certfile"):
        vmCertPath = arg
      elif opt in ("-k", "--keyfile"):
        vmKeyPath = arg
      elif opt in ("-j", "--runjobagent"):
        vmRunJobAgent = arg
      elif opt in ("-m", "--runvmmonitoragent"):
        vmRunVmMonitorAgent = arg
      elif opt in ("-l", "--runlogjobagent"):
        vmRunLogJobAgent = arg
      elif opt in ("-L", "--runlogvmmonitoragent"):
        vmRunLogVmMonitorAgent = arg
      elif opt in ("-v", "--cvmfscontextscript"):
        cvmfsContextPath = arg
      elif opt in ("-d", "--diraccontextscript"):
        diracContextPath = arg
      elif opt in ("-p", "--cvmfshttpproxy"):
        cvmfs_http_proxy = arg
      elif opt in ("-s", "--sitename"):
        siteName = arg

    # vmcert and key have been previoslly copy to VM, these paths are local, the rest of files are on some repo... 
    # 1) download the necesary files:
    os.system("wget --no-check-certificate -O %s '%s'" %(localVmRunJobAgent, vmRunJobAgent) )
    os.system("wget --no-check-certificate -O %s '%s'" %(localVmRunVmMonitorAgent, vmRunVmMonitorAgent) )
    os.system("wget --no-check-certificate -O %s '%s'" %(localVmRunLogJobAgent, vmRunLogJobAgent) )
    os.system("wget --no-check-certificate -O %s '%s'" %(localVmRunLogVmMonitorAgent, vmRunLogVmMonitorAgent) )
    os.system("wget --no-check-certificate -O %s '%s'" %(localCvmfsContextPath, cvmfsContextPath) )
    os.system("wget --no-check-certificate -O %s '%s'" %(localDiracContextPath, diracContextPath) )

    #2) Run the cvmvfs contextualization script:    
    if not ( cvmfsContextPath == '' ):
      os.system("chmod u+x ./%s" %localCvmfsContextPath )
      os.system("./%s %s" %(localCvmfsContextPath, cvmfs_http_proxy) )

    #3) Run the dirac contextualization script:    
    os.system("chmod u+x ./%s" %localDiracContextPath )
    os.system("./%s %s %s %s %s %s %s %s" %(localDiracContextPath, siteName, vmCertPath, vmKeyPath, localVmRunJobAgent, localVmRunVmMonitorAgent, localVmRunLogJobAgent, localVmRunLogVmMonitorAgent) )

if __name__ == "__main__":
    main(sys.argv[1:])

sys.exit(0)
