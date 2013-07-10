########################################################################
# $HeadURL$
# File :   VirtualMachineContextualization.py
# Author : Victor Mendez
########################################################################

"""
	The VirtualMachineContextualization agent is used for the asyncronous vm contextualization methods.
        The first available method is ssh contextualization
        Such method needs the VM to be active, before the contextualization using ssh
        For this pupose the Image layer load Status=Wait_ssh_context on VirtualMachineDB.vm_Instances table
        This agent is lookint into this table, asking to the corresponding cloud endpoint to check the status,
        then requesting the VM contextualization and
        changing the status on the corresponding entry of VirtualMachineDB.vm_SshContextualize
"""

import random
import paramiko
import os
import time


# DIRAC
from DIRAC                       import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.NovaImage   import NovaImage
from VMDIRAC.WorkloadManagementSystem.Client.OcciImage   import OcciImage
from VMDIRAC.WorkloadManagementSystem.Client.ServerUtils import virtualMachineDB



__RCSID__ = "$Id: $"

#FIXME: why do we need the random seed ?
random.seed()

class VirtualMachineContextualization( AgentModule ):

  def initialize( self ):
    """ Standard constructor
    """

    self.am_setOption( "PollingTime", 60.0 )

    return S_OK()

  def execute( self ):
    """ Agent executor
    """
    result = virtualMachineDB.getInstancesInfoByStatus ( 'Wait_ssh_context' )
    if not result['OK']:
      return result

    for uniqueId, endpoint, publicIP in result['Value']:

      retDict = virtualMachineDB.getImageNameFromInstance ( uniqueId )
      if not retDict['OK']:
        return retDict

      diracImageName = retDict['Value']

      cloudDriver = gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( endpoint, "cloudDriver" ) )
      if ( cloudDriver == 'nova-1.1' ):
        nima     = NovaImage( diracImageName, endpoint )
        connection = nima.connectOcci()
      elif ( cloudDriver == 'occi-1.1' ):
        oima     = OcciImage( diracImageName, endpoint )
        connection = oima.connectNova()
      else:
        return S_ERROR( 'cloudDriver %s, has not ssh contextualization' % cloudDriver )

      if not connection[ 'OK' ]:
        return connection

      if ( cloudDriver == 'nova-1.1' ):
        result = nima.getInstanceStatus( uniqueId )
      elif ( cloudDriver == 'occi-1.1' ):
        result = oima.getInstanceStatus( uniqueId )
      else:
        return S_ERROR( 'cloudDriver %s, has not ssh contextualization' % cloudDriver )
      if not result[ 'OK' ]:
        return result

      if result['Value'] == 'RUNNING':
        if ( cloudDriver == 'nova-1.1' ):
          result = nima.contextualizeInstance( uniqueId, publicIP )
        elif ( cloudDriver == 'occi-1.1' ):
          result = oima.contextualizeInstance( uniqueId, publicIP )
        else:
          return S_ERROR( 'cloudDriver %s, has not ssh contextualization' % cloudDriver )
        self.log.info( "result of contextualize:" )
        self.log.info( result )
        if not result[ 'OK' ]:
          return result
        retDict = virtualMachineDB.declareInstanceContextualizing( uniqueId )
        if not retDict['OK']:
          return retDict

    return S_OK() 

class SshContextualise:

  def contextualise( self, imageConfig, endpointConfig, **kwargs ):
   
    contextMethod = imageConfig[ 'contextMethod' ]
    if contextMethod == 'ssh':

      cvmfs_http_proxy = endpointConfig.get( 'CVMFS_HTTP_PROXY' )
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
      cpuTime                       = contextConfig[ 'cpuTime' ]

      uniqueId = kwargs.get( 'uniqueId' )
      publicIP = kwargs.get( 'publicIp' )

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
                                        cpuTime = cpuTime
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
                                        cpuTime
                        ):
    # the contextualization using ssh needs the VM to be ACTIVE, so VirtualMachineContextualization
    # check status and launch contextualize_VMInstance

    # 1) copy the necesary files

    # prepare paramiko sftp client
    try:
      privatekeyfile = os.path.expanduser( '~/.ssh/id_rsa' )
      mykey = paramiko.RSAKey.from_private_key_file( privatekeyfile )
      sshusername = 'root'
      transport = paramiko.Transport( ( publicIP, 22 ) )
      transport.connect( username = sshusername, pkey = mykey )
      sftp = paramiko.SFTPClient.from_transport( transport )
    except Exception, errmsg:
      return S_ERROR( "Can't open sftp conection to %s: %s" % ( publicIP, errmsg ) )

    # scp VM cert/key
    putCertPath = "/root/vmservicecert.pem"
    putKeyPath = "/root/vmservicekey.pem"
    try:
      sftp.put( vmCertPath, putCertPath )
      sftp.put( vmKeyPath, putKeyPath )
      # while the ssh.exec_command is asyncronous request I need to put on the VM the contextualize-script to ensure the file existence before exec
      sftp.put(vmContextualizeScriptPath, '/root/contextualize-script.bash')
    except Exception, errmsg:
      return S_ERROR( errmsg )
    finally:
      sftp.close()
      transport.close()

    # giving time sleep asyncronous sftp
    time.sleep( 5 )


    #2)  prepare paramiko ssh client
    try:
      ssh = paramiko.SSHClient()
      ssh.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
      ssh.connect( publicIP, username = sshusername, port = 22, pkey = mykey )
    except Exception, errmsg:
      return S_ERROR( "Can't open ssh conection to %s: %s" % ( publicIP, errmsg ) )

    #3) Run the DIRAC contextualization orchestator script:

    try:
      remotecmd = "/bin/bash /root/contextualize-script.bash \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\'"
      remotecmd = remotecmd % ( uniqueId, putCertPath, putKeyPath, vmRunJobAgentURL,
                                vmRunVmMonitorAgentURL, vmRunVmUpdaterAgentURL, vmRunLogAgentURL,
                                vmCvmfsContextURL, vmDiracContextURL, cvmfs_http_proxy, siteName, cloudDriver, cpuTime, vmStopPolicy )
      print "remotecmd"
      print remotecmd
      _stdin, _stdout, _stderr = ssh.exec_command( remotecmd )
    except Exception, errmsg:
      return S_ERROR( "Can't run remote ssh to %s: %s" % ( publicIP, errmsg ) )

    return S_OK()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

