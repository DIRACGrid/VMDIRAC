# $HeadURL$
"""
  Nova11
  
  driver to nova v_1.1 endpoint using libcloud
  
"""
# File :   Nova11.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )

import os
import paramiko
import time 

from libcloud                   import security
from libcloud.compute.types     import Provider
from libcloud.compute.providers import get_driver

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.SshContextualize   import SshContextualize
from VMDIRAC.WorkloadManagementSystem.Client.BuildCloudinitScript   import BuildCloudinitScript

__RCSID__ = '$Id: $'

class NovaClient:
  """
  NovaClient ( v1.1 )
  """

  def __init__( self, user, secret, endpointConfig, imageConfig ):
    """
    Multiple constructor depending on the passed parameters
    
    :Parameters:
      **user** - `string`
        username that will be used on the authentication
      **secret** - `string`
        password used on the authentication
      If secret is None then user actually is:
      **proxyPath** - `string`
        path to the valid X509 proxy 
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
    ex_force_auth_version   = endpointConfig.get( 'ex_force_auth_version', None )
    ex_tenant_name          = endpointConfig.get( 'ex_tenant_name', None )
    
    # we force SSL cacert, if defined
    ex_force_ca_cert        = endpointConfig.get( 'ex_force_ca_cert', None )
    if ex_force_ca_cert is not None:
      security.CA_CERTS_PATH = [ ex_force_ca_cert ]
    # no server ssl verify only for testing
    # security.VERIFY_SSL_CERT = False

    # log info:
    os.system("export LIBCLOUD_DEBUG=/tmp/libcloud.log")
    self.log.info( "ex_force_auth_url %s" % ex_force_auth_url )
    self.log.info( "ex_force_service_region %s" % ex_force_service_region )
    self.log.info( "ex_force_auth_version %s" % ex_force_auth_version )
    self.log.info( "ex_tenant_name %s" % ex_tenant_name )
    self.log.info( "ex_force_ca_cert %s" % ex_force_ca_cert )

    # get openstack driver
    openstack_driver = get_driver( Provider.OPENSTACK )
    
    if secret == None:
      # with VOMS (from Alvaro Lopez trunk https://github.com/alvarolopez/libcloud/blob/trunk):
      proxyPath=user
      username = password = None

      self.__driver = openstack_driver( username, password,
                                     ex_force_auth_url = ex_force_auth_url,
                                     ex_force_service_region = ex_force_service_region,
                                     ex_force_auth_version = ex_force_auth_version,
                                     ex_tenant_name = ex_tenant_name,
                                     ex_voms_proxy = proxyPath
                                    )
    else:
      # with user password
      username = user
      password = secret
      # eventually libcloud access to Grizzly force service name:
                                     #ex_force_service_name = 'image',
                                     #ex_force_service_name = 'nova',
      self.__driver = openstack_driver( username, password,
                                     ex_force_auth_url = ex_force_auth_url,
                                     ex_force_service_region = ex_force_service_region,
                                     ex_force_auth_version = ex_force_auth_version,
                                     ex_tenant_name = ex_tenant_name
                                    )
  

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

    # for openstack compatibility Grizlly:    
    #return S_OK( [ image for image in images if image.name == imageName ][ 0 ] )

    found=None
    for image in images:
	if image.name == imageName: 
		found = image
		break

    if found is None:
       return S_ERROR( "Image %s not found" % imageName )
    
    return S_OK( found )
 
  def get_flavor( self, flavorName ):
    """
    Given the flavorName, returns the current flavor object from the server. 
    
    :Parameters:
      **flavorName** - `string`
        flavorName as stored on the OpenStack service
      
    :return: S_OK( flavor ) | S_ERROR
    """
    try:
      flavors = self.__driver.list_sizes()
      # the libcloud library, throws Exception. Nothing to do.
    except Exception, errmsg:
      return S_ERROR( errmsg )

    return S_OK( [ flavor for flavor in flavors if flavor.name == flavorName ][ 0 ] )

  def get_security_groups( self, securityGroupNames ):
    """
    Given the securityGroupName, returns the current security group object from the server. 
    
    :Parameters:
      **securityGroupName** - `string`
        securityGroupName as stored on the OpenStack service
      
    :return: S_OK( securityGroup ) | S_ERROR
    """
     
    if not securityGroupNames:
      securityGroupNames = []
    elif not isinstance( securityGroupNames, list ):
      securityGroupNames = [ securityGroupNames ] 

    if not 'default' in securityGroupNames:
      securityGroupNames.append( 'default' )

    try:
      secGroups = self.__driver.ex_list_security_groups()
      # the libcloud library, throws Exception. Nothing to do.
    except Exception, errmsg:
      return S_ERROR( errmsg )
      
    return S_OK( [ secGroup for secGroup in secGroups if secGroup.name in securityGroupNames ] )   

  def create_VMInstance( self, vmdiracInstanceID, runningPodRequirements ):
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
    cloudDriver = self.endpointConfig[ 'cloudDriver' ]
    vmPolicy = self.endpointConfig[ 'vmPolicy' ]
    vmStopPolicy = self.endpointConfig[ 'vmStopPolicy' ]
    siteName = self.endpointConfig[ 'siteName' ]
    
    # Optional node contextualization parameters
    userdata    = self.imageConfig[ 'contextConfig' ].get( 'ex_userdata', None )
    metadata    = self.imageConfig[ 'contextConfig' ].get( 'ex_metadata', {} )
    secGroup    = self.imageConfig[ 'contextConfig' ].get( 'ex_security_groups', None )
    keyname     = self.imageConfig[ 'contextConfig' ].get( 'ex_keyname' , None )
    pubkeyPath  = self.imageConfig[ 'contextConfig' ].get( 'ex_pubkey_path' , None )
    
    if userdata is not None:
      with open( userdata, 'r' ) as userDataFile: 
        userdata = ''.join( userDataFile.readlines() )
    
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
    
    secGroupRes = self.get_security_groups( secGroup )
    if not secGroupRes[ 'OK' ]:
      self.log.error( secGroupRes[ 'Message' ] )
      return secGroupRes
    secGroup = secGroupRes[ 'Value' ]
          
    vm_name = 'DIRAC' + contextMethod + str( time.time() )[0:10]

    # keypair management is not available in libcloud openstack, jet.
    # https://issues.apache.org/jira/browse/LIBCLOUD-392
    # open pull request(10/09/2013): https://github.com/apache/libcloud/pull/145
    # if keyname is defined, it means that allready exisits with the key pair

    self.log.info( "Creating node" )
    self.log.verbose( "name : %s" % vm_name )
    self.log.verbose( "image : %s" % bootImage )
    self.log.verbose( "size : %s" % flavor )
    # mandatory for amiconfig, checked at Context.py
    self.log.verbose( "ex_userdata : %s" % userdata )
    self.log.verbose( "ex_metadata : %s" % metadata )
    # mandatory for ssh, checked at Context.py
    self.log.info( "ex_keyname : %s" % keyname )
    self.log.verbose( "ex_pubkey_path : %s" % pubkeyPath )

    try:
      if contextMethod == 'cloudinit':
        cloudinitScript = BuildCloudinitScript();
        result = cloudinitScript.buildCloudinitScript(self.imageConfig, self.endpointConfig, 
							runningPodRequirements = runningPodRequirements)
        if not result[ 'OK' ]:
          return result
        composedUserdataPath = result[ 'Value' ] 
        self.log.info( "cloudinitScript : %s" % composedUserdataPath )
        with open( composedUserdataPath, 'r' ) as userDataFile: 
          userdata = ''.join( userDataFile.readlines() )
        os.remove( composedUserdataPath )

        if ( keyname == None or keyname == 'nouse' ):
            vmNode = self.__driver.create_node( name               = vm_name, 
                                                image              = bootImage, 
                                                size               = flavor,
                                                ex_userdata        = userdata,
                                                ex_security_groups = secGroup)
	else:
            vmNode = self.__driver.create_node( name               = vm_name, 
                                                image              = bootImage, 
                                                size               = flavor,
                                                ex_userdata        = userdata,
                                                ex_keyname         = keyname,
                                                ex_security_groups = secGroup)
      elif contextMethod == 'amiconfig':
        if ( keyname == None or keyname == 'nouse' ):
            vmNode = self.__driver.create_node( name               = vm_name, 
                                            image              = bootImage, 
                                            size               = flavor,
                                            ex_userdata        = userdata,
                                            ex_security_groups = secGroup,
                                            ex_metadata        = metadata )	
	else:
            vmNode = self.__driver.create_node( name               = vm_name, 
                                            image              = bootImage, 
                                            size               = flavor,
                                            ex_keyname         = keyname,
                                            ex_userdata        = userdata,
                                            ex_security_groups = secGroup,
                                            ex_metadata        = metadata )
      else:
        # contextMethod ssh or adhoc
        vmNode = self.__driver.create_node( name                        = vm_name,
                                            image                       = bootImage,
                                            size                        = flavor
                                        )
      # the libcloud library, throws Exception. Nothing to do.
    except Exception, errmsg:
      return S_ERROR( errmsg )


    # giving time sleep to REST API caching the instance to be available:
    time.sleep( 12 )

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

    # Delete floating IP if any
    ipPool = self.endpointConfig.get( 'ipPool' )
    if ipPool is not None:
      if ( ipPool != 'nouse' ):
        publicIP = self.__deleteFloatingIP( public_ip, node )
        if not publicIP[ 'OK' ]:
          self.log.error( publicIP[ 'Message' ] )
          return publicIP

    # Destroys the node
    try:
      res = self.__driver.destroy_node( node ) == True
      if not res == True:
        return S_ERROR( "Not True returned destroying %s: %s" % ( uniqueId, res ) )

      # the libcloud library, throws Exception. Nothing to do.
    except Exception, errmsg:
      return S_ERROR( errmsg )

    return S_OK()

  def contextualize_VMInstance( self, uniqueId, publicIp, cpuTime, submitPool ):
    """
    This method is only used ( at the moment ) by the ssh contextualization method.
    It is called once the vm has been booted.
    </>
    
    :Parameters:
      **uniqueId** - `string`
        openstack node id ( not uuid ! )
      **publicIp** - `string`
        public IP assigned to the node if any
      
    :return: S_OK | S_ERROR
    """

    novaContext = SshContextualize()
    return novaContext.contextualise( self.imageConfig, self.endpointConfig,
                                      uniqueId = uniqueId, 
                                      publicIp = publicIp,
                                      cpuTime = cpuTime,
                                      submitPool = submitPool ) 

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

    ipPool = self.endpointConfig.get( 'ipPool' )

    if ipPool is not None:

      if ( ipPool != 'nouse' ):
        try:
          pool_list = self.__driver.ex_list_floating_ip_pools()

          for pool in pool_list:
            if pool.name == ipPool:
              floating_ip = pool.create_floating_ip()
              self.__driver.ex_attach_floating_ip_to_node(node, floating_ip)
              public_ip = floating_ip.ip_address
              return S_OK( public_ip )  

          return S_ERROR( 'Context parameter ipPool=%s is not defined in the openstack endpoint' % ipPool )

        except Exception, errmsg:
          return S_ERROR( errmsg )
 
    # for the case of not using floating ip assigment
    public_ip = ''

    return S_OK( public_ip )  
      

  def __deleteFloatingIP( self, public_ip, node ):
    """
    Deletes a floating IP <public_ip> from the server.
    
    :Parameters:
      **public_ip** - `string`
        public IP to be deleted
    
    :return: S_OK | S_ERROR   
    """
    
    ipPool = self.endpointConfig.get( 'ipPool' )
    
    if not ipPool is None:

      if ( ipPool != 'nouse' ):
        try:
          pool_list = self.__driver.ex_list_floating_ip_pools()

          for pool in pool_list:
            if pool.name == ipPool:
              floating_ip = pool.get_floating_ip(public_ip)
              self.__driver.ex_detach_floating_ip_from_node(node, floating_ip)

              floating_ip.delete()
              return S_OK()

          return S_ERROR( 'Context parameter ipPool=%s is not defined in the openstack endpoint' % ipPool )

        except Exception, errmsg:
          return S_ERROR( errmsg )

    return S_OK()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
