# $HeadURL$
"""
  Nova11
  
  driver to nova v_1.1 endpoint using libcloud and pyton-novaclient
  
"""
# File :   Nova11.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
# TODO: first release only with user/passd, to implement proxy auth with VOMS

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

class NovaClient:
  """
  NovaClient ( v1.1 )
  """

  def __init__( self, user, secret, endpointConfig, imageConfig ):
    """
    Constructor: uses user / secret authentication for the time being. It initializes
    the libcloud.Openstack driver and the pynovaclient driver. Ther second one is
    a backup of the first in case it does not provide functionality needed ( mainly
    floating IPs ).
    
    :Parameters:
      **user** - `string`
        username that will be used on the authentication
      **secret** - `string`
        password used on the authentication
      **endpointConfig** - `dict`
        dictionary with the endpoint configuration ( WMS.Utilities.Configuration.NovaConfiguration )
      **imageConfig** - `dict`
        dictionary with the image configuration ( WMS.Utilities.Configuration.ImageConfiguration )
    
    """
    
    # logger
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    
    self.endpointConfig = endpointConfig
    self.imageConfig    = imageConfig
  
    # Variables needed to contact the service  
    ex_force_auth_url       = endpointConfig.get( 'ex_force_auth_url', None )
    ex_force_service_region = endpointConfig.get( 'ex_force_service_region', None ) 
    ex_force_auth_version   = endpointConfig.get( 'ex_force_auth_version', '2.0_password' )
    ex_tenant_name          = endpointConfig.get( 'ex_tenant_name', None )
    
    # if we have a more restrictive certificate, force it to be the only one
    caCert = endpointConfig.get( 'ex_force_ca_cert', None )
    if caCert is not None:
      security.CA_CERTS_PATH = [ caCert ]
    
    # get the right openstack node driver
    cloudManagerAPI = get_driver( Provider.OPENSTACK )
    
    # The driver has the access secret, we do not want it to be public at all.    
    self.__driver = cloudManagerAPI( user, secret = secret,
                                     ex_force_auth_url = ex_force_auth_url,
                                     ex_force_service_region = ex_force_service_region,
                                     ex_force_auth_version = ex_force_auth_version,
                                     ex_tenant_name = ex_tenant_name,
                                    )
     
    # mofify to insecure = False when ca cert ready
    # The client has the access secret, we do not want it to be public at all.    
    self.__pynovaclient = client.Client( username = user, 
                                         api_key = secret, 
                                         project_id = ex_tenant_name, 
                                         auth_url = ex_force_auth_url, 
                                         insecure = True, 
                                         region_name = ex_force_service_region, 
                                         auth_system = 'keystone' )

  def check_connection( self ):
    """
    Checks connection status by trying to list the images.
    
    :return: S_OK | S_ERROR
    """

    try:
      _ = self.__driver.list_images()
      # the libcloud library, throws Exception. Nothing to do.
    except Exception, errmsg:
      return S_ERROR( errmsg )
    
    return S_OK()
  
  def get_image( self, imageName ):
    """
    Given the imageName, returns the current image object from the server. 
    
    :Parameters:
      **imageName** - `string`
        imageName as stored on the OpenStack image repository ( glance )
      
    :return: S_OK( image ) | S_ERROR
    """

    try:
      images = self.__driver.list_images()
      # the libcloud library, throws Exception. Nothing to do.
    except Exception, errmsg:
      return S_ERROR( errmsg )
          
    return S_OK( [ image for image in images if image.name == imageName ][ 0 ] )

  def get_flavor( self, flavorName ):
    """
    Given the flavorName, returns the current flavor object from the server. 
    
    :Parameters:
      **flavorName** - `string`
        flavorName as stored on the OpenStack service
      
    :return: S_OK( image ) | S_ERROR
    """
    
    try:
      flavors = self.__driver.list_sizes()
      # the libcloud library, throws Exception. Nothing to do.
    except Exception, errmsg:
      return S_ERROR( errmsg )
      
    return S_OK( [ flavor for flavor in flavors if flavor.name == flavorName ][ 0 ] )   

  def create_VMInstance( self ):
    """
    This creates a VM instance for the given boot image 
    and creates a context script, taken the given parameters.
    Successful creation returns instance VM 
    
    Boots a new node on the OpenStack server defined by self.endpointConfig. The
    'personality' of the node is done by self.imageConfig. Both variables are
    defined on initialization phase.
    
    The node name has the following format:
    <bootImageName><contextMethod><time>
    
    It boots the node. If IPpool is defined on the imageConfiguration, a floating
    IP is created and assigned to the node. 
    
    :return: S_OK( ( nodeID, publicIP ) ) | S_ERROR
    """
    
    # Common Image Attributes  
    bootImageName = self.imageConfig[ 'bootImageName' ]
    flavorName    = self.imageConfig[ 'flavorName' ]
    contextMethod = self.imageConfig[ 'contextMethod' ]
    
    # Optional node contextualization parameters
    keyname  = self.imageConfig.get( 'ex_keyname' , None )
    userdata = self.imageConfig.get( 'ex_userdata', None )
    metadata = self.imageConfig.get( 'ex_metadata', None )
    
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
    
    #FIXME: change the VM name, we want to know which version of contextualization
    vm_name = bootImageName + '+' + contextMethod + '+' + str( time.time() )[0:10] 

    self.log.info( "Creating node" )
    self.log.verbose( "name : %s" % vm_name )
    self.log.verbose( "image : %s" % bootImage )
    self.log.verbose( "size : %s" % flavor )
    self.log.verbose( "ex_keyname : %s" % keyname )
    self.log.verbose( "ex_userdata : %s" % userdata )
    self.log.verbose( "ex_metadata : %s" % metadata )

    try:
      vmNode = self.__driver.create_node( name        = vm_name, 
                                          image       = bootImage, 
                                          size        = flavor,
                                          ex_keyname  = keyname,
                                          ex_userdata = userdata,
                                          ex_metadata = metadata )
      # the libcloud library, throws Exception. Nothing to do.
    except Exception, errmsg:
      return S_ERROR( errmsg )

    publicIP = self.__assignFloatingIP( vmNode )
    if not publicIP[ 'OK' ]:
      self.log.error( publicIP[ 'Message' ] )
      return publicIP

    return S_OK( ( vmNode.id, publicIP[ 'Value' ] ) )

  def getDetails_VMInstance( self, uniqueId ):
    """
    Given a Node ID, returns all its configuration details on a 
    libcloud.compute.base.Node object.
    
    :Parameters:
      **uniqueId** - `string`
        openstack node id ( not uuid ! )
    
    :return: S_OK( Node ) | S_ERROR    
    """

    try:
      nodeDetails = self.__driver.ex_get_node_details( uniqueId )
      # the libcloud library, throws Exception. Nothing to do.        
    except Exception, errmsg:
      return S_ERROR( errmsg )  

    return S_OK( nodeDetails )
                                      
  def getStatus_VMInstance( self, uniqueId ):
    """
    Get the status for a given node ID. libcloud translates the status into a digit
    from 0 to 4 using a many-to-one relation ( ACTIVE and RUNNING -> 0 ), which
    means we cannot undo that translation. It uses an intermediate states mapping
    dictionary, SITEMAP, which we use here inverted to return the status as a 
    meaningful string. The five possible states are ( ordered from 0 to 4 ):
    RUNNING, REBOOTING, TERMINATED, PENDING & UNKNOWN.
    
    :Parameters:
      **uniqueId** - `string`
        openstack node id ( not uuid ! )
    
    :return: S_OK( status ) | S_ERROR      
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
    Given the node ID it gets the node details, which are used to destroy the
    node making use of the libcloud.openstack driver. If three is any public IP
    ( floating IP ) assigned, frees it as well.
    
    :Parameters:
      **uniqueId** - `string`
        openstack node id ( not uuid ! )
      **public_ip** - `string`
        public IP assigned to the node if any
      
    :return: S_OK | S_ERROR
    """

    # Get Node object with node details
    nodeDetails = self.getDetails_VMInstance( uniqueId )
    if not nodeDetails[ 'OK' ]:
      return nodeDetails
    node = nodeDetails[ 'Value' ]

    # Destroys the node
    try:
      res = self.__driver.destroy_node( node ) == True
      if not res == True:
        return S_ERROR( "Not True returned destroying %s: %s" % ( uniqueId, res ) )
        
      #_infonode = self.__pynovaclient.servers.delete(uniqueId)
      # the libcloud library, throws Exception. Nothing to do.
    except Exception, errmsg:
      return S_ERROR( errmsg )

    # Delete floating IP if any
    publicIP = self.__deleteFloatingIP( public_ip )
    if not publicIP[ 'OK' ]:
      self.log.error( publicIP[ 'Message' ] )
      return publicIP

    return S_OK()

  def contextualize_VMInstance( self, uniqueId, publicIp ):
    """
    This method is only used ( at the moment ) by the ssh contextualization method.
    It is called once the vm has been booted.
    
    <IS THIS REALLY TRUE ?>
    This is necesary because the libcloud deploy_node, including key/cert copy and ssh run, 
    based on amiconfig, are sychronous operations which can not scale
    </>
    
    :Parameters:
      **uniqueId** - `string`
        openstack node id ( not uuid ! )
      **publicIp** - `string`
        public IP assigned to the node if any
      
    :return: S_OK | S_ERROR
    """

    novaContext = NovaContextualise()
    return novaContext.contextualise( self.imageConfig, self.endpointConfig,
                                      uniqueId = uniqueId, 
                                      publicIp = publicIp ) 

  #.............................................................................
  # Private methods

  def __assignFloatingIP( self, node ):
    """
    Given a node, assign a floating IP from the ipPool defined on the imageConfiguration
    on the CS.
    
    :Parameters:
      **node** - `libcloud.compute.base.Node`
        node object with the vm details
    
    :return: S_OK( public_ip ) | S_ERROR   
    """

    # Sometimes we do not have public IP
    ipPool = self.imageConfig[ 'contextConfig' ].get( 'ipPool', None )

    if not ipPool is not None:

      ################################## 
      # 
      # FIXME do it with Libcloud     
      # 
      ################################## 
           
      # getting a floating IP and assign to the node:
      try:
        address = self.__pynovaclient.floating_ips.create( pool = ipPool )
        self.__pynovaclient.servers.add_floating_ip( node.id, address.ip )
        public_ip = address.ip
      #FIXME: double check if pynovaclient raises Exception
      except Exception, errmsg:
        return S_ERROR( errmsg )
 
    else:
      public_ip = node.ip
      
    return S_OK( public_ip )  

  def __deleteFloatingIP( self, public_ip ):
    """
    Deletes a floating IP <public_ip> from the server.
    
    :Parameters:
      **public_ip** - `string`
        public IP to be deleted
    
    :return: S_OK | S_ERROR   
    """
    
    ipPool = self.imageConfig[ 'contextConfig' ].get( 'ipPool', None )
    
    if not ipPool is None:

      #############################
      # 
      # FIXME do it with Libcloud     
      # 
      ############################# 
      
      try:
        floating_ips = self.__pynovaclient.floating_ips.list()
        for floating_ip in floating_ips:
          if floating_ip.ip == public_ip:
            self.__pynovaclient.floating_ips.delete( floating_ip.id )
      #FIXME: double check if pynovaclient raises Exception
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
      sshusername = 'root'
      transport = paramiko.Transport( ( publicIP, 22 ) )
      transport.connect( username = sshusername, pkey = mykey )
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
      ssh.connect( publicIP, username = sshusername, port = 22, pkey = mykey )
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
