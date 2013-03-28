# $HeadURL$
"""
  NovaImage
"""
# File   :   NovaImage.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.Nova11           import NovaClient
from VMDIRAC.WorkloadManagementSystem.Utilities.Configuration import NovaConfiguration, ImageConfiguration

__RCSID__ = '$Id: $'

class NovaImage( NovaConfiguration, ImageConfiguration ):

  #FIXME: mixture of upper and lower case keys.. not good at all
#  NOVA_ENDPOINT_KEYS = [ 'driver', 'siteName', 'MaxEndpointInstances',
#                         'osBaseURL', 'osAuthURL', 'osUserName', 'osPasswd',
#                         'osTenantName', 'osServiceRegion', 'osCaCert', 
#                         'CVMFS_HTTP_PROXY' ]

  #FIXME: added osCaCert

  def __init__( self, imageName, endpoint ):
    """
    The NovaImage provides the functionality required to use
    a OpenStack cloud infrastructure, with NovaAPI DIRAC driver
    Authentication is provided by user/password attributes
    """
    
    self.log = gLogger.getSubLogger( 'NovaImage %s: ' % imageName )
    
    NovaImage.__init__( endpoint )
    ImageConfiguration.__init__( imageName )
    
    self.imageName = imageName
    self.endpoint  = endpoint        
           
    self.__clinova   = None
    self.__bootImage = None
    
    #...........................................................................
    #...........................................................................
    #...........................................................................       

#    if self.__contextMethod == 'ssh':

      #FIXME: isn't this on the CloudEndpoint ! ??
      # cvmfs http proxy:
      #self.__cvmfs_http_proxy = self.__getCSImageOption( "CVMFS_HTTP_PROXY" )
#      self.__cvmfs_http_proxy = self.__getCSCloudEndpointOption( "CVMFS_HTTP_PROXY" )
#      if not self.__cvmfs_http_proxy:
#        self.__errorStatus = "Can't find the CVMFS_HTTP_PROXY for endpoint %s" % self.__endpoint
#        self.log.error( self.__errorStatus )
#        return

    ## Additional Network pool
    #self.__osIpPool = self.__getCSImageOption( "vmOsIpPool" )

# FIXME: WE DO NOT WANT DEFAULTS ??
#    self.__osIpPool = self.imageOptions.contextDict.get( 'vmOsIpPool', '' )
#    if not self.__osIpPool:
#      self.__osIpPool = 'NO'

  def connectNova( self ):

    # Before doing anything, make sure the configurations make sense
    # ImageConfiguration
    validImage = self.validateImageConfig()
    if not validImage[ 'OK' ]:
      return validImage
    # EndpointConfiguration
    validNova = self.validateEndpointConfig()
    if not validNova[ 'OK' ]:
      return validNova
    
    # Get authentication configuration
    user, secret = self.authConfig()

    self.__clinova = NovaClient( user, secret, **self.endpointConfig )

    request = self.__clinova.check_connection()
    if request.returncode != 0:
      self.log.error( "NovaClient returned code %s checking connection" % request.returncode )
      return S_ERROR( "NovaClient returned code %s checking connection" % request.returncode )

#    _msg = "Available OpenStack nova endpoint  %s and Auth URL: %s"
#    self.log.info( _msg % ( self.cloudEndpointDict[ 'osBaseURL' ], self.cloudEndpointDict[ 'osAuthURL' ] ) )
     
    request = self.__clinova.get_image( self.imageDict[ 'bootImageName' ] )
    if request.returncode != 0:
      _msg      = "Can't get the boot image for %s: \n%s" % ( self.imageDict[ 'bootImageName' ], request.stderr )
      self.log.error( _msg )
      return S_ERROR( _msg )
      
    self.__bootImage = request.image
    return S_OK()    

  def startNewInstance( self, instanceType ):
    """
    Wrapping the image creation
    """
    
    _msg = "Starting new instance for image: %s; to endpoint %s DIRAC driver of nova endpoint"
    self.log.info( _msg % ( self.imageDict[ 'bootImageName' ], self.endpoint ) )
    
    request = self.__clinova.create_VMInstance( self.imageDict[ 'bootImageName' ], 
                                                self.imageDict[ 'contextMethod' ], 
                                                instanceType, 
                                                self.__bootImage, 
                                                self.imageDict[ 'osIpPool' ] )
    if request.returncode != 0:
      _errMsg = "Can't create instance for boot image: %s at server %s and Auth URL: %s \n%s"
      _errMsg = _errMsg % ( self.imageDict[ 'bootImageName' ], self.cloudEndpointDict[ 'osBaseURL' ], 
                            self.cloudEndpointDict[ 'osAuthURL' ], request.stderr )
      self.log.error( _errMsg )
      return S_ERROR( _errMsg )

    return S_OK( request )

  def contextualizeInstance( self, uniqueId, public_ip ):
    """
    Wrapping the contextualization
    With ssh method, contextualization is asyncronous operation
    """
    if self.imageDict[ 'contextMethod' ] =='ssh':
      
      self.log.verbose( 'Contextualising %s with ssh' % uniqueId )
      request = self.__clinova.contextualize_VMInstance( uniqueId, public_ip, self.imageDict[ 'contextMethod' ], 
                                                         self.imageDict[ 'vmCertPath' ], 
                                                         self.imageDict[ 'vmKeyPath' ], 
                                                         self.imageDict[ 'vmContextualizeScriptPath' ], 
                                                         self.imageDict[ 'vmRunJobAgentURL' ], 
                                                         self.imageDict[ 'vmRunVmMonitorAgentURL' ], 
                                                         self.imageDict[ 'vmRunLogJobAgentURL' ], 
                                                         self.imageDict[ 'vmRunLogVmMonitorAgentURL' ],
                                                         self.imageDict[ 'vmCvmfsContextURL' ], 
                                                         self.imageDict[ 'vmDiracContextURL' ] , 
                                                         self.imageDict[ 'CVMFS_HTTP_PROXY' ], 
                                                         self.cloudEndpointDict[ 'siteName' ], 
                                                         self.cloudEndpointDict[ 'cloudDriver' ] )
      if request.returncode != 0:
        __errorStatus = "Can't contextualize VM id %s at endpoint %s: %s" % ( uniqueId, self.endpoint, request.stderr )
        self.log.error( __errorStatus )
        return S_ERROR( __errorStatus )

    return S_OK( uniqueId )

  def getInstanceStatus( self, uniqueId ):
    """
    Wrapping the get status of the uniqueId VM from the endpoint
    """
    request = self.__clinova.getStatus_VMInstance( uniqueId )
    if request.returncode != 0:
      __errorStatus = "Can't get status %s at endpoint %s: %s" % (uniqueId, self.endpoint, request.stderr)
      self.log.error( __errorStatus )
      return S_ERROR( __errorStatus )

    return S_OK( request.status )

  def stopInstance( self, uniqueId, public_ip ):
    """
    Simple call to terminate a VM based on its id
    """

    request = self.__clinova.terminate_VMinstance( uniqueId, self.cloudEndpointDict[ 'osIpPool' ], public_ip )
    if request.returncode != 0:
      __errorStatus = "Can't delete VM instance %s, IP %s, IpPool %s, from endpoint %s: %s"
      __errorStatus = __errorStatus % ( uniqueId, public_ip, self.cloudEndpointDict[ 'osIpPool' ], 
                                        self.endpoint, request.stderr )
      self.log.error( __errorStatus )
      return S_ERROR( __errorStatus )

    return S_OK( request.stderr )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF