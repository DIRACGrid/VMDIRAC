# $HeadURL$
""" Configuration
  
  Module that contains all helpers needed to obtain the endpoint or image
  configuration from the CS and validate.
  
"""

# DIRAC
from DIRAC import gConfig, gLogger, S_OK, S_ERROR

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Utilities.Context import ContextConfig

__RCSID__ = '$Id: $'

#...............................................................................

class EndpointConfiguration( object ):
  """
  EndpointConfiguration is the base class for all endpoints. It defines two methods
  which must be implemented on the child classes:
  * config
  * validate
  """

  ENDPOINT_PATH = '/Resources/VirtualMachines/CloudEndpoints'

  def __init__( self ):
    """
    Constructor
    """
    self.log = gLogger.getSubLogger( self.__class__.__name__ )

  def config( self ):
    """
    Must be implemented on the child class. Returns a dictionary with the configuration.
    
    :return: dict
    """
    raise NotImplementedError( "%s.endpointConfig() must be implemented" % self.__class__.__name__ )

  def validate( self ):
    """
    Must be implemented on the child class. Returns S_OK / S_ERROR, depending on
    the validity / omission of the parameters in the config.
    
    :return: S_OK | S_ERROR 
    """
    raise NotImplementedError( "%s.validate() must be implemented" % self.__class__.__name__ )

#...............................................................................

class OcciConfiguration( EndpointConfiguration ):
  """
  OcciConfiguration Class parses the section <occiEndpoint> 
  and builds a configuration if possible, with the information obtained from the CS.
  """

  # Keys that MUST be present on ANY Occi CloudEndpoint configuration in the CS
  MANDATORY_KEYS = [ 'cloudDriver', 'vmPolicy', 'vmStopPolicy', 'siteName', 'occiURI', 'maxEndpointInstances', 'auth', 'iface' ]
    
  def __init__( self, occiEndpoint ):
    """
    Constructor
    
    :Parameters:
      **occiEndpoint** - `string`
        string with the name of the CloudEndpoint defined on the CS
    """
    super( OcciConfiguration, self ).__init__() 
      
    occiOptions = gConfig.getOptionsDict( '%s/%s' % ( self.ENDPOINT_PATH, occiEndpoint ) )
    if not occiOptions[ 'OK' ]:
      self.log.error( occiOptions[ 'Message' ] )
      occiOptions = {}
    else:
      occiOptions = occiOptions[ 'Value' ] 

    # FIXME: make it generic !

    # Purely endpoint configuration ............................................              
    # This two are passed as arguments, not keyword arguments
    self.__auth                    = occiOptions.get( 'auth'                    , None )
    self.__user                    = occiOptions.get( 'user'                    , None )
    self.__password                = occiOptions.get( 'password'                , None )
    self.__userCredPath            = occiOptions.get( 'userCredPath'            , None )
    self.__proxyCaPath             = occiOptions.get( 'proxyCaPath'             , None )

    self.__cloudDriver             = occiOptions.get( 'cloudDriver'             , None )
    self.__vmStopPolicy            = occiOptions.get( 'vmStopPolicy'            , None )
    self.__vmPolicy                = occiOptions.get( 'vmPolicy'                , None )
    self.__siteName                = occiOptions.get( 'siteName'                , None )
    self.__maxEndpointInstances    = occiOptions.get( 'maxEndpointInstances'    , None )
    
    self.__occiURI                 = occiOptions.get( 'occiURI'                 , None )
    self.__imageDriver             = occiOptions.get( 'imageDriver'             , None )
    self.__netId                   = occiOptions.get( 'netId'                   , None )
    self.__iface                   = occiOptions.get( 'iface'                   , None )
    self.__dns1                    = occiOptions.get( 'DNS1'                    , None )
    self.__dns2                    = occiOptions.get( 'DNS2'                    , None )
    self.__domain                  = occiOptions.get( 'domain'                  , None )
    self.__cvmfs_http_proxy        = occiOptions.get( 'CVMFS_HTTP_PROXY'        , None )
    self.__ipPool                  = occiOptions.get( 'ipPool'                  , None )

  def config( self ):
    
    config = {}
    
    config[ 'auth' ]                    = self.__auth
    config[ 'user' ]                    = self.__user
    config[ 'password' ]                = self.__password
    config[ 'userCredPath' ]            = self.__userCredPath
    config[ 'proxyCaPath' ]             = self.__proxyCaPath

    config[ 'cloudDriver' ]             = self.__cloudDriver
    config[ 'vmPolicy' ]                = self.__vmPolicy
    config[ 'vmStopPolicy' ]            = self.__vmStopPolicy
    config[ 'siteName' ]                = self.__siteName
    config[ 'maxEndpointInstances' ]    = self.__maxEndpointInstances
    config[ 'occiURI' ]                 = self.__occiURI 
    config[ 'iface' ]                   = self.__iface

    # optionals depending on endpoint/image setup:
    config[ 'imageDriver' ]             = self.__imageDriver
    config[ 'netId' ]                   = self.__netId
    config[ 'dns1' ]                    = self.__dns1
    config[ 'dns2' ]                    = self.__dns2
    config[ 'domain' ]                  = self.__domain
    config[ 'cvmfs_http_proxy' ]        = self.__cvmfs_http_proxy
    config[ 'ipPool' ]                  = self.__ipPool
    
    # Do not return dictionary with None values
    for key, value in config.items():
      if value is None:
        del config[ key ]
        
    return config  

  def validate( self ):
    
  
    endpointConfig = self.config()

 
    missingKeys = set( self.MANDATORY_KEYS ).difference( set( endpointConfig.keys() ) ) 
    if missingKeys:
      return S_ERROR( 'Missing mandatory keys on endpointConfig %s' % str( missingKeys ) )
    
    # on top of the MANDATORY_KEYS, we make sure the corresponding auth parameters are set:
    if self.__auth == 'userpasswd':
      if self.__user is None:
        return S_ERROR( 'user is None' )
      if self.__password is None:
        return S_ERROR( 'password is None' )
    elif self.__auth == 'proxycacert':
      if self.__userCredPath is None:
        return S_ERROR( 'userCredPath is None' )
      if self.__proxyCaPath is None:
        return S_ERROR( 'proxyCaPath is None' )
    else:
      return S_ERROR( 'endpoint auth: %s not defined (userpasswd/proxycacert)' % self.__auth)
    
    self.log.info( '*' * 50 )
    self.log.info( 'Displaying endpoint info' )
    for key, value in endpointConfig.iteritems():
      if key == 'user':
        self.log.info( '%s : *********' % ( key ) )
      elif key == 'password':
        self.log.info( '%s : *********' % ( key ) )
      else:
        self.log.info( '%s : %s' % ( key, value ) )
    self.log.info( 'User and Password are NOT printed.')
    self.log.info( '*' * 50 )
        
    return S_OK()

  def authConfig( self ):
    
    if self.__auth == 'userpasswd':
      return ( self.__auth, self.__user, self.__password )
    elif self.__auth == 'proxycacert':
      return ( self.__auth, self.__userCredPath, self.__proxyCaPath )
    else:
      return S_ERROR( 'endpoint auth: %s not defined (userpasswd/proxycacert)' % self.__auth)
  
  def cloudDriver( self ):
    
    return ( self.__cloudDriver )

  def occiURI( self ):
    
    return ( self.__occiURI )

#...............................................................................

class NovaConfiguration( EndpointConfiguration ):
  """
  NovaConfiguration Class parses the section <novaEndpoint> 
  and builds a configuration if possible, with the information obtained from the CS.
  """

  # Keys that MUST be present on ANY Nova CloudEndpoint configuration in the CS
  MANDATORY_KEYS = [ 'ex_force_auth_url', 'ex_force_service_region', 'ex_tenant_name', 'vmPolicy', 'vmStopPolicy', 'cloudDriver', 'siteName', 'maxEndpointInstances' ]
    
  def __init__( self, novaEndpoint ):
    """
    Constructor
    
    :Parameters:
      **novaEndpoint** - `string`
        string with the name of the CloudEndpoint defined on the CS
    """
    super( NovaConfiguration, self ).__init__() 
      
    novaOptions = gConfig.getOptionsDict( '%s/%s' % ( self.ENDPOINT_PATH, novaEndpoint ) )
    if not novaOptions[ 'OK' ]:
      self.log.error( novaOptions[ 'Message' ] )
      novaOptions = {}
    else:
      novaOptions = novaOptions[ 'Value' ] 

    # FIXME: make it generic !

    # Purely endpoint configuration ............................................              
    # This two are passed as arguments, not keyword arguments
    self.__user                    = novaOptions.get( 'user'                   , None )
    self.__password                = novaOptions.get( 'password'               , None )

    self.__cloudDriver             = novaOptions.get( 'cloudDriver'            , None )
    self.__vmStopPolicy            = novaOptions.get( 'vmStopPolicy'           , None )
    self.__vmPolicy                = novaOptions.get( 'vmPolicy'               , None )
    self.__siteName                = novaOptions.get( 'siteName'               , None )
    self.__maxEndpointInstances    = novaOptions.get( 'maxEndpointInstances'   , None )
    
    self.__ex_force_ca_cert        = novaOptions.get( 'ex_force_ca_cert'       , None )
    self.__ex_force_auth_token     = novaOptions.get( 'ex_force_auth_token'    , None )
    self.__ex_force_auth_url       = novaOptions.get( 'ex_force_auth_url'      , None )
    self.__ex_force_auth_version   = novaOptions.get( 'ex_force_auth_version'  , None )
    self.__ex_force_base_url       = novaOptions.get( 'ex_force_base_url'      , None )
    self.__ex_force_service_name   = novaOptions.get( 'ex_force_service_name'  , None )
    self.__ex_force_service_region = novaOptions.get( 'ex_force_service_region', None )
    self.__ex_force_service_type   = novaOptions.get( 'ex_force_service_type'  , None )   
    self.__ex_tenant_name          = novaOptions.get( 'ex_tenant_name'         , None )

  def config( self ):
    
    config = {}
    
    config[ 'user' ]                    = self.__user
    config[ 'password' ]                = self.__password

    config[ 'cloudDriver' ]             = self.__cloudDriver
    config[ 'vmPolicy' ]                = self.__vmPolicy
    config[ 'vmStopPolicy' ]            = self.__vmStopPolicy
    config[ 'siteName' ]                = self.__siteName
    config[ 'maxEndpointInstances' ]    = self.__maxEndpointInstances

    config[ 'ex_force_ca_cert' ]        = self.__ex_force_ca_cert 
    config[ 'ex_force_auth_token' ]     = self.__ex_force_auth_token
    config[ 'ex_force_auth_url' ]       = self.__ex_force_auth_url
    config[ 'ex_force_auth_version' ]   = self.__ex_force_auth_version
    config[ 'ex_force_base_url' ]       = self.__ex_force_base_url
    config[ 'ex_force_service_name' ]   = self.__ex_force_service_name
    config[ 'ex_force_service_region' ] = self.__ex_force_service_region
    config[ 'ex_force_service_type' ]   = self.__ex_force_service_type
    config[ 'ex_tenant_name' ]          = self.__ex_tenant_name
    
    # Do not return dictionary with None values
    for key, value in config.items():
      if value is None:
        del config[ key ]
        
    return config  

  def validate( self ):
    
  
    endpointConfig = self.config()

 
    missingKeys = set( self.MANDATORY_KEYS ).difference( set( endpointConfig.keys() ) ) 
    if missingKeys:
      return S_ERROR( 'Missing mandatory keys on endpointConfig %s' % str( missingKeys ) )
    
    # on top of the MANDATORY_KEYS, we make sure the user & password are set
    if self.__user is None:
      return S_ERROR( 'User is None' )
    if self.__password is None:
      return S_ERROR( 'Password is None' )
    
    self.log.info( '*' * 50 )
    self.log.info( 'Displaying endpoint info' )
    for key, value in endpointConfig.iteritems():
      self.log.info( '%s : %s' % ( key, value ) )
    self.log.info( 'User and Password are NOT printed.')
    self.log.info( '*' * 50 )
        
    return S_OK()

  def authConfig( self ):
    
    return ( self.__user, self.__password )
  
#...............................................................................    

class ImageConfiguration( object ):
  
  def __init__( self, imageName ):
  
    self.log = gLogger.getSubLogger( 'ImageConfiguration' )
   
    imageOptions = gConfig.getOptionsDict( '/Resources/VirtualMachines/Images/%s' % imageName )
    if not imageOptions[ 'OK' ]:
      self.log.error( imageOptions[ 'Messages' ] )
      imageOptions = {}
    else:
      imageOptions = imageOptions[ 'Value' ] 
  
    self.__ic_bootImageName  = imageOptions.get( 'bootImageName'     , None )
    self.__ic_contextMethod  = imageOptions.get( 'contextMethod'     , None )
    self.__ic_flavorName     = imageOptions.get( 'flavorName'        , None )
    #self.__ic_contextConfig = ContextConfig( self.__ic_bootImageName, self.__ic_contextMethod )
    self.__ic_contextConfig  = ContextConfig( imageName, self.__ic_contextMethod )

  def config( self ):
    
    config = {
              'bootImageName' : self.__ic_bootImageName,
              'contextMethod' : self.__ic_contextMethod,
              'flavorName'    : self.__ic_flavorName,  
              'contextConfig' : self.__ic_contextConfig.config()
              }
      
    return config

  def validate( self ):
    
    if self.__ic_bootImageName is None:
      return S_ERROR( 'self._ic_bootImageName is None' )
    if self.__ic_contextMethod is None:
      return S_ERROR( 'self._ic_contextMethod is None' )
    if self.__ic_flavorName is None:
      return S_ERROR( 'self._ic_flavorName is None' )
   
    validateContext = self.__ic_contextConfig.validate()
    if not validateContext[ 'OK' ]:
      self.log.error( validateContext[ 'Message' ] )
      return validateContext
    
    self.log.info( 'Displaying image info' )
    self.log.info( '*' * 50 )
    self.log.info( 'ic_bootImageName %s' % self.__ic_bootImageName )
    self.log.info( 'ic_contextMethod %s' % self.__ic_contextMethod )
    for key, value in self.__ic_contextConfig.config().iteritems():
      self.log.info( '%s : %s' % ( key, value ) )
    self.log.info( '*' * 50 )  
      
    return S_OK()   

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    
