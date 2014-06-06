########################################################################
# $HeadURL$
# File :   BuildCloudinitScript.py
# Author : Victor Mendez
########################################################################

"""
    This class construct a cloudinit script for a DIRAC image and IaaS endpoint
"""

import os


# DIRAC
from DIRAC                       import gLogger, S_OK, S_ERROR, gConfig

__RCSID__ = "$Id: $"

class BuildCloudinitScript:

  def buildCloudinitScript( self, imageConfig, endpointConfig, **kwargs ):

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

      #RunningPod requirements are passed as arguments
      cpuTime = kwargs.get( 'cpuTime' )
      submitPool = kwargs.get( 'submitPool' )

      result = self.__buildCloudinitScript( DIRACImageName = DIRACImageName,
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
                                        cpuTime = cpuTime,
                                        submitPool = submitPool
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
                                        cpuTime,
                                        submitPool
                        ):
    # The function return S_OK with the name of the created cloudinit script
    # If the cloudinit context script was previously created, then overwriten
    cloudinitPath = '/tmp/cloudinit_' + DIRACImageName + '_' + siteName + '.sh'
    file=open(cloudinitPath, 'w')

    #start writing the script
    file.write('#!/bin/bash\n') 

    #buildin the necesary arguments
    putCertPath = "/root/vmservicecert.pem"
    file.write('putCertPath=%s' % (putCertPath))
    putKeyPath = "/root/vmservicekey.pem"
    file.write('putKeyPath=%s' % (putKeyPath))
    file.write('vmRunJobAgent=%s' % (vmRunJobAgent))
    file.write('vmRunVmMonitorAgent=%s' % (vmRunVmMonitorAgent))
    file.write('vmRunVmUpdaterAgent=%s' % (vmRunVmUpdaterAgent))
    file.write('vmRunLogAgent=%s' % (vmRunLogAgent))
    file.write('vmCvmfsContextURL=%s' % (vmCvmfsContextURL))
    file.write('vmDiracContextURL=%s' % (vmDiracContextURL))
    file.write('cvmfs_http_proxy=%s' % (cvmfs_http_proxy))
    file.write('siteName=%s' % (siteName))
    file.write('cloudDriver=%s' % (cloudDriver))
    file.write('cpuTime=%s' % (cpuTime))
    file.write('vmStopPolicy=%s' % (vmStopPolicy))
    file.write('submitPool=%s' % (submitPool))

    # 0) Previous copy of necessary files using build in cloudinit script
    # 0.1) DIRAC service public key

    pubkeyPath = os.path.expanduser( '~/.ssh/id_rsa.pub' )
    file.write("cat << 'EOF' > /root/.ssh/authorized_keys")

    try:
      with open(pubkeyPath) as fp:
        for line in fp:
          file.write(line)
    except Exception, errmsg:
      return S_ERROR( errmsg )

    file.write("EOF")

    # VM DIRAC service cert 
    file.write("cat << 'EOF' > %s" % (putCertPath))

    try:
      with open(vmCertPath) as fp:
        for line in fp:
          file.write(line)
    except Exception, errmsg:
      return S_ERROR( errmsg )

    file.write("EOF")

    # VM DIRAC service key 
    file.write("cat << 'EOF' > %s" % (putKeyPath))

    try:
      with open(vmKeyPath) as fp:
        for line in fp:
          file.write(line)
    except Exception, errmsg:
      return S_ERROR( errmsg )

    file.write("EOF")

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
