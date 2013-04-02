########################################################################
# $HeadURL$
# File :   Nova11.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################
# DIRAC driver to nova v_1.1 endpoint using libcloud and pyton-novaclient
# TODO: frist release only with user/passd, to implement proxy auth with VOMS

import getpass
import os
import paramiko
import time 

# libcloud, novaclient
from libcloud                   import security
from libcloud.compute.types     import Provider
from libcloud.compute.providers import get_driver
from novaclient.v1_1            import client

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR

__RCSID__ = '$Id: $'

#FIXME: where to find it ?
#libcloud.security.CA_CERTS_PATH =[ '/etc/pki/tls/certs/CERN-bundle.pem' ]
# osServiceRegion = 'cern-geneva'

class NovaClient:

  def __init__( self, user, secret, endpointConfig, imageConfig ):
    
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    
    self.endpointConfig = endpointConfig
    self.imageConfig    = imageConfig
    
    cloudManagerAPI = get_driver( Provider.OPENSTACK )   
    
    ex_force_auth_url       = endpointConfig.get( 'ex_force_auth_url', None )
    ex_force_service_region = endpointConfig.get( 'ex_force_service_region', None ) 
    ex_force_auth_version   = endpointConfig.get( 'ex_force_auth_version', '2.0_password' )
    ex_tenant_name          = endpointConfig.get( 'ex_tenant_name', None )
    
    caCert = endpointConfig.get( 'ex_force_ca_cert', None )
    if caCert is not None:
      security.CA_CERTS_PATH = [ caCert ]
    
    # The driver has the access secret, we do not want it to be public at all.    
    self.__driver = cloudManagerAPI( user, secret = secret,
                                     ex_force_auth_url = ex_force_auth_url,
                                     ex_force_service_region = ex_force_service_region,
                                     ex_force_auth_version = ex_force_auth_version,
                                     ex_tenant_name = ex_tenant_name,
                                    )
     
    # mofify to insecure=False when ca cert ready
    # The client has the access secret, we do not want it to be public at all.    
    self.__pynovaclient = client.Client( username = user, api_key = secret, 
                                         project_id = ex_tenant_name, 
                                         auth_url = ex_force_auth_url, 
                                         insecure = True, 
                                         region_name = ex_force_service_region, 
                                         auth_system = 'keystone' )

  def check_connection( self ):
    """
    This is a way to check the availability of a give URL as occi server
    returning a Request class
    """

    try:
      _ = self.__driver.list_images()
    except Exception, errmsg:
      return S_ERROR( errmsg )
    return S_OK()
  
  def get_image( self, imageName ):
    """
    The get_image_id function return the corresponding openstack id
    a given imageName on the current occi client self.URI of the occi server.
    """

    try:
      images = self.__driver.list_images() 
    except Exception, errmsg:
      return S_ERROR( errmsg )
          
    return S_OK( [ i for i in images if i.name == imageName ][0] )

  def get_flavor( self, flavorName ):
    
    try:
      flavors = self.__driver.list_sizes()
    except Exception, e:
      return S_ERROR( e )  
    return S_OK( [ s for s in flavors if s.name == flavorName ][ 0 ] )   

  def create_VMInstance( self, imageConfig ):
#  def create_VMInstance( self, bootImageName, contextMethod, flavorName, bootImage, ipPool,
#                         userdata = None, keyname = None, metadata = None ):
    """
    This creates a VM instance for the given boot image 
    and creates a context script, taken the given parameters.
    Successful creation returns instance VM 
    """
    
    bootImageName = imageConfig[ 'bootImageName' ]
    flavorName    = imageConfig[ 'flavorName' ]
    contextMethod = imageConfig[ 'contextMethod' ]
    
    keyname  = imageConfig.get( 'ex_keyname' , None )
    userdata = imageConfig.get( 'ex_userdata', None )
    metadata = imageConfig.get( 'ex_metadata', None )
    
    bootImage = self.get_image( bootImageName )
    if not bootImage[ 'OK' ]:
      self.log.error( bootImage[ 'Message' ] )
      return bootImage
    bootImage = bootImage[ 'Value' ]
      
    flavor = self.get_flavor( flavorName )
    if not flavor[ 'OK' ]:
      self.log.error( flavor[ 'Message' ] )
      return flavor
    flavor = flavor[ 'Value' ]
    
    vm_name = bootImageName + '+' + contextMethod + '+' + str( time.time() )[0:10] 

    try:
      vmNode = self.__driver.create_node( name        = vm_name, 
                                          image       = bootImage, 
                                          size        = flavor,
                                          ex_keyname  = keyname,
                                          ex_userdata = userdata,
                                          ex_metadata = metadata )
    except Exception, errmsg:
      return S_ERROR( errmsg )

    # Sometimes we do not have public IP
    ipPool = self.imageConfig[ 'contextConfig' ].get( 'ipPool', None )

    if not ipPool is not None:

      # FIXME
      # FIXME
      # FIXME
      # FIXME do it with Libcloud     
      # FIXME
      # FIXME
      # FIXME
      # FIXME
            
      # getting a floating IP and assign to the node:
      try:
        address = self.__pynovaclient.floating_ips.create( pool = ipPool )
        self.__pynovaclient.servers.add_floating_ip( vmNode.id, address.ip )
        public_ip = address.ip
      except Exception, errmsg:
        return S_ERROR( errmsg )
 
    else:
      public_ip = vmNode.ip

    return S_OK( ( vmNode.id, public_ip ) )
 
  def contextualize_VMInstance( self, uniqueId, publicIp ):
    """ 
    Conextualize an active instance
    This is necesary because the libcloud deploy_node, including key/cert copy and ssh run, 
    based on amiconfig, are sychronous operations which can not scale
    """

    novaContext = NovaContextualise()
    return novaContext.contextualise( self.imageConfig, self.endpointConfig,
                                      uniqueId = uniqueId, 
                                      publicIp = publicIp ) 

  def getDetails_VMInstance( self, uniqueId ):

    try:
      nodeDetails = self.__driver.ex_get_node_details( uniqueId )            
    except Exception, errmsg:
      return S_ERROR( errmsg )  

    return S_OK( nodeDetails )
                                      
  def getStatus_VMInstance( self, uniqueId ):
    """
    Get the status VM instance for a given VMinstanceId 
    """

    nodeDetails = self.getDetails_VMInstance( uniqueId )
    if not nodeDetails[ 'OK' ]:
      return nodeDetails
    
    state = nodeDetails[ 'Value' ].state

    # reversed from libcloud
    STATEMAP = { 0 : 'RUNNING',
                 1 : 'REBOOTING',
                 2 : 'TERMINATED',
                 3 : 'PENDING',
                 4 : 'UNKNOWN' }

    if not state in STATEMAP:
      return S_ERROR( 'State %s not in STATEMAP' % state )
    
    return S_OK( STATEMAP[ state ] )

  def terminate_VMinstance( self, uniqueId, public_ip = '' ):
    """
    Terminate a VM instance with uniqueId
    """

    nodeDetails = self.getDetails_VMInstance( uniqueId )
    if not nodeDetails[ 'OK' ]:
      return nodeDetails
    node = nodeDetails[ 'Value' ]

    try:
      res = self.__driver.destroy_node( node ) == True
      if not res == True:
        return S_ERROR( "Not True returned destroying %s: %s" % ( uniqueId, res ) )
        
      #_infonode = self.__pynovaclient.servers.delete(uniqueId)
    except Exception, errmsg:
      return S_ERROR( errmsg )

    ipPool = self.imageConfig[ 'contextConfig' ].get( 'ipPool', None )
    
    if not ipPool is None:
    
#    if not ipPool=='NONE':
      try:
        floating_ips = self.__pynovaclient.floating_ips.list()
        for floating_ip in floating_ips:
          if floating_ip.ip == public_ip:
            self.__pynovaclient.floating_ips.delete( floating_ip.id )
      except Exception, errmsg:
        return S_ERROR( errmsg )

    return S_OK()

#...............................................................................
# Contextualisation methods

class NovaContextualise: 
      
  def contextualise( self, imageConfig, endpointConfig, **kwargs ):
    
    contextMethod = imageConfig[ 'contextMethod' ]
    
    if contextMethod == 'ssh':
      
      cvmfs_http_proxy = endpointConfig.get( 'CVMFS_HTTP_PROXY' )
      siteName         = endpointConfig.get( 'siteName' )
      cloudDriver      = endpointConfig.get( 'driver' )
      
      uniqueId = kwargs.get( 'uniqueId' )
      publicIP = kwargs.get( 'publicIp' ) 
      
      result = self.__sshContextualise( uniqueId, publicIP, cloudDriver = cloudDriver,
                                        cvmfs_http_proxy = cvmfs_http_proxy,
                                        siteName = siteName, **imageConfig )
    elif contextMethod == 'adhoc':
      result = S_OK()
    elif contextMethod == 'amiconfig':
      result = S_OK()
    else:
      result = S_ERROR( '%s is not a known NovaContext method' % contextMethod ) 
      
    return result         
    

  def __sshContextualise( self, uniqueId, publicIP, vmCertPath = '', vmKeyPath = '',
                          vmContextualizeScriptPath = '', vmRunJobAgentURL = '', 
                          vmRunVmMonitorAgentURL = '', vmRunLogJobAgentURL = '',
                          vmRunLogVmMonitorAgentURL = '', cvmfsContextURL = '',
                          diracContextURL = '', cvmfs_http_proxy = '', siteName = '',
                          cloudDriver = '' ):
        
    # the contextualization using ssh needs the VM to be ACTIVE, so VirtualMachineContextualization 
    # check status and launch contextualize_VMInstance

    # 1) copy the necesary files

    # prepare paramiko sftp client
    try:
      privatekeyfile = os.path.expanduser( '~/.ssh/id_rsa' )
      mykey = paramiko.RSAKey.from_private_key_file( privatekeyfile )
      username =  getpass.getuser()
      transport = paramiko.Transport( ( publicIP, 22 ) )
      transport.connect( username = username, pkey = mykey )
      sftp = paramiko.SFTPClient.from_transport( transport )
    except Exception, errmsg:
      return S_ERROR( "Can't open sftp conection to %s: %s" % ( publicIP, errmsg ) )
    finally:
      transport.close()      

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
    #2)  prepare paramiko ssh client
    try:
      ssh = paramiko.SSHClient()
      ssh.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
      ssh.connect( publicIP, username = username, port = 22, pkey = mykey )
    except Exception, errmsg:
      return S_ERROR( "Can't open ssh conection to %s: %s" % ( publicIP, errmsg ) )

    #3) Run the DIRAC contextualization orchestator script:    

    try:
      remotecmd = "/bin/bash /root/contextualize-script.bash \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\'"  
      remotecmd = remotecmd % ( uniqueId, putCertPath, putKeyPath, vmRunJobAgentURL, 
                                vmRunVmMonitorAgentURL, vmRunLogJobAgentURL, vmRunLogVmMonitorAgentURL, 
                                cvmfsContextURL, diracContextURL, cvmfs_http_proxy, siteName, cloudDriver )
      print "remotecmd"
      print remotecmd
      _stdin, _stdout, _stderr = ssh.exec_command( remotecmd )
    except Exception, errmsg:
      return S_ERROR( "Can't run remote ssh to %s: %s" % ( publicIP, errmsg ) )

    return S_OK()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF