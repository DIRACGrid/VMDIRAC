########################################################################
# $HeadURL$
# File :   SshContextualize.py
# Author : Victor Mendez
########################################################################

"""
    This is the common ssh contextualization class for all the cloud managers
"""

import random
import paramiko
import os
import time


# DIRAC
from DIRAC                       import gLogger, S_OK, S_ERROR, gConfig

__RCSID__ = "$Id: $"

class SshContextualize:

  def contextualise( self, imageConfig, endpointConfig, **kwargs ):

    # logger
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
   
    contextMethod = imageConfig[ 'contextMethod' ]
    if contextMethod == 'ssh':

      cvmfs_http_proxy = endpointConfig.get( 'cvmfs_http_proxy' )
      siteName         = endpointConfig.get( 'siteName' )
      cloudDriver      = endpointConfig.get( 'cloudDriver' )
      vmStopPolicy     = endpointConfig.get( 'vmStopPolicy' )

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

      uniqueId = kwargs.get( 'uniqueId' )
      publicIP = kwargs.get( 'publicIp' )
      cpuTime = kwargs.get( 'cpuTime' )
      submitPool = kwargs.get( 'submitPool' )

      result = self.__sshContextualise( uniqueId = uniqueId,
                                        publicIP = publicIP,
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
                                        siteName = siteName,
                                        cpuTime = cpuTime,
                                        submitPool = submitPool
                                      )
    elif contextMethod == 'adhoc':
      result = S_OK()
    elif contextMethod == 'amiconfig':
      result = S_OK()
    else:
      result = S_ERROR( '%s is not a known NovaContext method' % contextMethod )

    return result


  def __sshContextualise( self,
                                        uniqueId,
                                        publicIP,
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
                                        siteName,
                                        cpuTime,
                                        submitPool
                        ):
    # the contextualization using ssh needs the VM to be ACTIVE, so VirtualMachineContextualization
    # check status and launch contextualize_VMInstance

    self.log.info ( "Preparing sftp client" )
    # 1) copy the necesary files
    # prepare paramiko sftp client
    host = '%s' % publicIP
    if ( host[0] == ' ' ):
      last = len(host)
      host = host[1:last]

    port = 22
    try:
      privatekeyfile = os.path.expanduser( '~/.ssh/id_rsa' )
      mykey = paramiko.RSAKey.from_private_key_file( privatekeyfile )
      sshusername = 'root'
      transport = paramiko.Transport( ( host, port ) )
      transport.connect( username = sshusername, pkey = mykey )
      sftp = paramiko.SFTPClient.from_transport( transport )
    except Exception, errmsg:
      return S_ERROR( "Can't open sftp conection to %s errmsg: %s" % ( host, errmsg ) )

    self.log.info ( "Copy of VM cert keys and contextualize-script" )
    # scp VM cert/key
    putCertPath = "/root/vmservicecert.pem"
    putKeyPath = "/root/vmservicekey.pem"
    try:
      # while the ssh.exec_command is asyncronous request I need to put on the VM the contextualize-script to ensure the file existence before exec
      sftp.put(vmContextualizeScriptPath, '/root/contextualize-script.bash')
      sftp.put( vmCertPath, putCertPath )
      sftp.put( vmKeyPath, putKeyPath )
    except Exception, errmsg:
      return S_ERROR( errmsg )
    finally:
      sftp.close()
      transport.close()

    # giving time sleep asyncronous sftp
    time.sleep( 10 )

    self.log.info ( "Preparing ssh client" )
    #2)  prepare paramiko ssh client
    try:
      ssh = paramiko.SSHClient()
      ssh.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
      ssh.connect( host, username = sshusername, port = port, pkey = mykey )
    except Exception, errmsg:
      return S_ERROR( "Can't open ssh conection to %s errmsg: %s" % ( publicIP, errmsg ) )

    #3) Run the checker & DIRAC contextualization orchestator script:

    self.log.info('SshContextualize -> submitPool: %s' % submitPool)
    try:

      remotecmd = "/bin/bash /root/contextualize-script.bash \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\'"
      remotecmd = remotecmd % ( uniqueId, putCertPath, putKeyPath, vmRunJobAgentURL,
                                vmRunVmMonitorAgentURL, vmRunVmUpdaterAgentURL, vmRunLogAgentURL,
                                vmCvmfsContextURL, vmDiracContextURL, cvmfs_http_proxy, siteName, cloudDriver, cpuTime, vmStopPolicy, submitPool )
      self.log.info ( 'SshContextualize -> Remote Command: %s' % remotecmd )
      _stdin, _stdout, _stderr = ssh.exec_command( remotecmd )
    except Exception, errmsg:
      return S_ERROR( "Can't run remote ssh to %s errmsg: %s" % ( publicIP, errmsg ) )

    return S_OK()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
