########################################################################
# $HeadURL$
# File :   BuildCloudinitScript.py
# Author : Victor Mendez
########################################################################

"""
    This class construct a cloudinit script for a DIRAC image and IaaS endpoint
"""

import os
import sys


# DIRAC
from DIRAC                       import gLogger, S_OK, S_ERROR, gConfig

__RCSID__ = "$Id: $"

class BuildCloudinitScript:

  def buildCloudinitScript( self, imageConfig, endpointConfig, runningPodRequirements, instanceID = None ):

    # logger
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
   
    contextMethod = imageConfig[ 'contextMethod' ]
    if contextMethod == 'cloudinit':

      cvmfs_http_proxy = endpointConfig.get( 'cvmfs_http_proxy' )
      siteName         = endpointConfig.get( 'siteName' )
      cloudDriver      = endpointConfig.get( 'cloudDriver' )
      vmStopPolicy     = endpointConfig.get( 'vmStopPolicy' )

      imageName                     = imageConfig.get( 'DIRACImageName' )
      contextConfig                 = imageConfig.get( 'contextConfig' )
      vmKeyPath                     = contextConfig[ 'vmKeyPath' ]
      vmCertPath                    = contextConfig[ 'vmCertPath' ]
      vmContextualizeScriptPath     = contextConfig[ 'vmContextualizeScriptPath' ]
      vmRunJobAgentURL              = contextConfig[ 'vmRunJobAgentURL' ]
      vmRunVmMonitorAgentURL        = contextConfig[ 'vmRunVmMonitorAgentURL' ]
      vmRunVmUpdaterAgentURL        = contextConfig[ 'vmRunVmUpdaterAgentURL' ]
      vmRunLogAgentURL              = contextConfig[ 'vmRunLogAgentURL' ]
      vmCvmfsContextURL             = contextConfig[ 'vmCvmfsContextURL' ]
      vmDiracContextURL             = contextConfig[ 'vmDiracContextURL' ]

      result = self.__buildCloudinitScript( DIRACImageName = imageName,
                                        siteName = siteName,
                                        cloudDriver = cloudDriver,
                                        cvmfs_http_proxy = cvmfs_http_proxy,
                                        vmStopPolicy = vmStopPolicy,
                                        contextMethod = contextMethod,
                                        vmCertPath = vmCertPath,
                                        vmKeyPath = vmKeyPath,
                                        vmContextualizeScriptPath = vmContextualizeScriptPath,
                                        vmRunJobAgentURL = vmRunJobAgentURL,
                                        vmRunVmMonitorAgentURL = vmRunVmMonitorAgentURL,
                                        vmRunVmUpdaterAgentURL = vmRunVmUpdaterAgentURL,
                                        vmRunLogAgentURL = vmRunLogAgentURL,
                                        vmCvmfsContextURL = vmCvmfsContextURL,
                                        vmDiracContextURL = vmDiracContextURL,
                                        runningPodRequirements = runningPodRequirements,
					instanceID = instanceID
                                      )
    elif contextMethod == 'ssh':
      result = S_ERROR( 'ssh context method found instead of cloudinit method' )
    elif contextMethod == 'adhoc':
      result = S_ERROR( 'adhoc context method found instead of cloudinit method' )
    elif contextMethod == 'amiconfig':
      result = S_ERROR( 'amiconfig context method found instead of cloudinit method' )
    else:
      result = S_ERROR( '%s is not a known NovaContext method' % contextMethod )

    return result


  def __buildCloudinitScript( self,
                                        DIRACImageName,
                                        siteName,
                                        cloudDriver,
                                        cvmfs_http_proxy,
                                        vmStopPolicy,
                                        contextMethod,
                                        vmCertPath,
                                        vmKeyPath,
                                        vmContextualizeScriptPath,
                                        vmRunJobAgentURL,
                                        vmRunVmMonitorAgentURL,
                                        vmRunVmUpdaterAgentURL,
                                        vmRunLogAgentURL,
                                        vmCvmfsContextURL,
                                        vmDiracContextURL,
                                        runningPodRequirements,
					instanceID
                        ):
    # The function return S_OK with the name of the created cloudinit script
    # If the cloudinit context script was previously created, then overwriten
    cloudinitPath = '/tmp/cloudinit_' + DIRACImageName + '_' + siteName + '_' + str(instanceID) + '.sh'
    file=open(cloudinitPath, 'w')

    #start writing the script
    file.write('#!/bin/bash\n') 

    #buildin the necesary arguments
    putCertPath = "/root/vmservicecert.pem"
    file.write('putCertPath=%s\n' % (putCertPath))
    putKeyPath = "/root/vmservicekey.pem"
    file.write('putKeyPath=%s\n' % (putKeyPath))
    file.write('vmRunJobAgentURL=%s\n' % (vmRunJobAgentURL))
    file.write('vmRunVmMonitorAgentURL=%s\n' % (vmRunVmMonitorAgentURL))
    file.write('vmRunVmUpdaterAgentURL=%s\n' % (vmRunVmUpdaterAgentURL))
    file.write('vmRunLogAgentURL=%s\n' % (vmRunLogAgentURL))
    file.write('vmCvmfsContextURL=%s\n' % (vmCvmfsContextURL))
    file.write('vmDiracContextURL=%s\n' % (vmDiracContextURL))
    file.write('cvmfs_http_proxy=\"%s\"\n' % (cvmfs_http_proxy))
    file.write('siteName=%s\n' % (siteName))
    file.write('cloudDriver=%s\n' % (cloudDriver))
    file.write('vmStopPolicy=%s\n' % (vmStopPolicy))
    file.write('instanceID=%s\n' % (instanceID))

    # dynamic runningPod requirements for LocalSite
    file.write("cat << 'EOF' > /root/LocalSiteRequirements\n")
    for key, value in runningPodRequirements.items():
      if type(value) is list:
        file.write('%s=%s\n' % (key,','.join(value)))
      else:
        file.write('%s=%s\n' % (key,value))

    file.write("EOF\n")

    # 0) Previous copy of necessary files using build in cloudinit script
    # 0.1) DIRAC service public key

    pubkeyPath = os.path.expanduser( '~/.ssh/id_rsa.pub' )
    file.write("cat << 'EOF' > /root/.ssh/authorized_keys\n")

    try:
      with open(pubkeyPath) as fp:
        for line in fp:
          file.write(line)
    except Exception, errmsg:
      return S_ERROR( errmsg )

    file.write("EOF\n")

    # VM DIRAC service cert 
    file.write("cat << 'EOF' > %s\n" % (putCertPath))

    try:
      with open(vmCertPath) as fp:
        for line in fp:
          file.write(line)
    except Exception, errmsg:
      return S_ERROR( errmsg )

    file.write("EOF\n")

    # VM DIRAC service key 
    file.write("cat << 'EOF' > %s\n" % (putKeyPath))

    try:
      with open(vmKeyPath) as fp:
        for line in fp:
          file.write(line)
    except Exception, errmsg:
      return S_ERROR( errmsg )

    file.write("EOF\n")

    #now the static part of the cloudinit

    try:
      with open(vmContextualizeScriptPath) as fp:
        for line in fp:
          file.write(line)
    except Exception, errmsg:
      return S_ERROR( errmsg )

    file.close()
    return S_OK(cloudinitPath)

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
