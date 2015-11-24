# $HeadURL$
""" Configuration
  
  Module that contains all helpers needed to obtain the endpoint or image
  configuration from the CS and validate.
  
"""

import copy

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
    self.__cvmfs_http_proxy        = occiOptions.get( 'cvmfs_http_proxy'        , None )
    self.__ipPool                  = occiOptions.get( 'ipPool'                  , None )

  def config( self ):
    
    config = {}
    
    config[ 'auth' ]                    = self.__auth
    config[ 'user' ]                    = self.__user
    config[ 'password' ]                = self.__password
    config[ 'userCredPath' ]            = self.__userCredPath

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
    elif self.__auth == 'proxy':
      if self.__userCredPath is None:
        return S_ERROR( 'userCredPath is None' )
    else:
      return S_ERROR( 'endpoint auth: %s not defined (userpasswd/proxy)' % self.__auth)
    
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
    elif self.__auth == 'proxy':
      return ( self.__auth, self.__userCredPath, 'Nouse' )
    else:
      return S_ERROR( 'endpoint auth: %s not defined (userpasswd/proxy)' % self.__auth)
  
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
  MANDATORY_KEYS = [ 'auth', 'ex_force_auth_url', 'ex_force_auth_version', 'ex_force_service_region', 'ex_tenant_name', 'vmPolicy', 'vmStopPolicy', 'cloudDriver', 'siteName', 'maxEndpointInstances' ]
    
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
    self.__auth                    = novaOptions.get( 'auth'                   , None )
    self.__user                    = novaOptions.get( 'user'                   , None )
    self.__password                = novaOptions.get( 'password'               , None )
    self.__proxyPath               = novaOptions.get( 'proxyPath'               , None )

    self.__cloudDriver             = novaOptions.get( 'cloudDriver'            , None )
    self.__vmStopPolicy            = novaOptions.get( 'vmStopPolicy'           , None )
    self.__vmPolicy                = novaOptions.get( 'vmPolicy'               , None )
    self.__siteName                = novaOptions.get( 'siteName'               , None )
    self.__maxEndpointInstances    = novaOptions.get( 'maxEndpointInstances'   , None )
    self.__cvmfs_http_proxy        = novaOptions.get( 'cvmfs_http_proxy'        , None )
    self.__ipPool                  = novaOptions.get( 'ipPool'                 , None )
    
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
    
    config[ 'auth' ]                    = self.__auth
    config[ 'user' ]                    = self.__user
    config[ 'password' ]                = self.__password
    config[ 'proxyPath' ]               = self.__proxyPath

    config[ 'cloudDriver' ]             = self.__cloudDriver
    config[ 'vmPolicy' ]                = self.__vmPolicy
    config[ 'vmStopPolicy' ]            = self.__vmStopPolicy
    config[ 'siteName' ]                = self.__siteName
    config[ 'maxEndpointInstances' ]    = self.__maxEndpointInstances
    config[ 'cvmfs_http_proxy' ]        = self.__cvmfs_http_proxy
    config[ 'ipPool' ]                  = self.__ipPool

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
    
    # on top of the MANDATORY_KEYS, we make sure the corresponding auth parameters are set:
    if self.__auth == 'userpasswd':
      if self.__user is None:
        return S_ERROR( 'user is None' )
      if self.__password is None:
        return S_ERROR( 'password is None' )
    elif self.__auth == 'proxy':
      if self.__proxyPath is None:
        return S_ERROR( 'proxyPath is None' )
    else:
      return S_ERROR( 'endpoint auth: %s not defined (userpasswd/proxy)' % self.__auth)
    
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
    
    return S_OK()

  def authConfig( self ):
    
    if self.__auth == 'userpasswd':
      return ( self.__auth, self.__user, self.__password )
    elif self.__auth == 'proxy':
      return ( self.__auth, self.__proxyPath, None )
    else:
      return S_ERROR( 'endpoint auth: %s not defined (userpasswd/proxy)' % self.__auth)

  def isFloatingIP( self ):

    endpointConfig = self.config()
    ipPool = endpointConfig.get( 'ipPool' )

    return ipPool is not None

#...............................................................................  

class AmazonConfiguration( EndpointConfiguration ):
  """
  AmazonConfiguration Class parses the section <amazonEndpoint> 
  and builds a configuration if possible, with the information obtained from the CS.
  """

  # Keys that MUST be present on ANY Amazon CloudEndpoint configuration in the CS
  MANDATORY_KEYS = [ 'accessKey', 'secretKey', 'cloudDriver', 'vmPolicy', 'vmStopPolicy', 'siteName', 'endpointURL', 'regionName', 'maxEndpointInstances', 'auth' ]
    
  def __init__( self, amazonEndpoint ):
    """
    Constructor
    
    :Parameters:
      **amazonEndpoint** - `string`
        string with the name of the CloudEndpoint defined on the CS
    """
    super( AmazonConfiguration, self ).__init__() 
      
    amazonOptions = gConfig.getOptionsDict( '%s/%s' % ( self.ENDPOINT_PATH, amazonEndpoint ) )
    if not amazonOptions[ 'OK' ]:
      self.log.error( amazonOptions[ 'Message' ] )
      amazonOptions = {}
    else:
      amazonOptions = amazonOptions[ 'Value' ] 

    # FIXME: make it generic !

    # Purely endpoint configuration ............................................              
    # This two are passed as arguments, not keyword arguments
    self.__accessKey               = amazonOptions.get( 'accessKey'              , None )
    self.__secretKey               = amazonOptions.get( 'secretKey'              , None )
    self.__cloudDriver             = amazonOptions.get( 'cloudDriver'            , None )
    self.__vmStopPolicy            = amazonOptions.get( 'vmStopPolicy'           , None )
    self.__vmPolicy                = amazonOptions.get( 'vmPolicy'               , None )
    self.__siteName                = amazonOptions.get( 'siteName'               , None )
    self.__endpointURL             = amazonOptions.get( 'endpointURL'            , None )
    self.__regionName              = amazonOptions.get( 'regionName'             , None )
    self.__maxEndpointInstances    = amazonOptions.get( 'maxEndpointInstances'   , None )
    self.__maxOportunisticEndpointInstances    = amazonOptions.get( 'maxOportunisticEndpointInstances'   , 0 )
    self.__cvmfs_http_proxy        = amazonOptions.get( 'cvmfs_http_proxy'        , None )
    
    self.__auth                    = amazonOptions.get( 'auth'       , None )

  def config( self ):
    
    config = {}
    
    config[ 'auth' ]                    = self.__auth
    config[ 'accessKey' ]               = self.__accessKey
    config[ 'secretKey' ]               = self.__secretKey

    config[ 'cloudDriver' ]             = self.__cloudDriver
    config[ 'vmPolicy' ]                = self.__vmPolicy
    config[ 'vmStopPolicy' ]            = self.__vmStopPolicy
    config[ 'siteName' ]                = self.__siteName
    config[ 'endpointURL' ]                = self.__endpointURL
    config[ 'regionName' ]                = self.__regionName
    config[ 'maxEndpointInstances' ]    = self.__maxEndpointInstances
    config[ 'maxOportunisticEndpointInstances' ]    = self.__maxOportunisticEndpointInstances
    config[ 'cvmfs_http_proxy' ]        = self.__cvmfs_http_proxy

    # Do not return dictionary with None values
    for key, value in config.items():
      if value is None:
        del config[ key ]
        
    return config  

  def validate( self ):
    
  
    endpointConfig = self.config()

 
    missingKeys = set( self.MANDATORY_KEYS ).difference( set( endpointConfig.keys() ) ) 
    if missingKeys:
      self.log.error( 'Missing mandatory keys on endpointConfig %s' % str( missingKeys ) )
      return S_ERROR( 'Missing mandatory keys on endpointConfig %s' % str( missingKeys ) )
    
    # on top of the MANDATORY_KEYS, we make sure the corresponding auth parameters are set:
    if self.__auth == 'secretaccesskey':
      if self.__accessKey is None:
        self.log.error( 'accessKey is None' )
        return S_ERROR( 'accessKey is None' )
      if self.__secretKey is None:
        self.log.error( 'secretKey is None' )
        return S_ERROR( 'secretKey is None' )
    else:
      self.log.error( 'endpoint auth: %s not defined (secretaccesskey)' % self.__auth)
      return S_ERROR( 'endpoint auth: %s not defined (secretaccesskey)' % self.__auth)
    
    self.log.info( '*' * 50 )
    self.log.info( 'Displaying endpoint info' )
    for key, value in endpointConfig.iteritems():
      if key == 'accessKey':
        self.log.info( '%s : *********' % ( key ) )
      elif key == 'secretKey':
        self.log.info( '%s : *********' % ( key ) )
      else:
        self.log.info( '%s : %s' % ( key, value ) )
    self.log.info( 'accessKey and secretKey are NOT printed.')
    
    return S_OK()

  def authConfig( self ):
    
    if self.__auth == 'secretaccesskey':
      return ( self.__auth, self.__acccessKey, self.__secretKey )
    else:
      self.log.error( 'endpoint auth: %s not defined (secretaccesskey)' % self.__auth)
      return S_ERROR( 'endpoint auth: %s not defined (secretaccesskey)' % self.__auth)

#...............................................................................  

class StratusLabConfiguration( EndpointConfiguration ):
  """
  Class that parses the <stratusLabEndpoint> section of the configuration and
  formats this configuration as a dictionary.

  Author: Charles Loomis

  """

  DIRAC_REQUIRED_KEYS = frozenset( [ 'vmPolicy', 'vmStopPolicy', 'cloudDriver',
                                     'siteName', 'maxEndpointInstances' ] )

  STRATUSLAB_REQUIRED_KEYS = frozenset( [ 'ex_endpoint' ] )

  STRATUSLAB_OPTIONAL_KEYS = frozenset( [ 'ex_name', 'ex_country',
                                          'ex_pdisk_endpoint', 'ex_marketplace_endpoint',
                                          'ex_username', 'ex_password', 'ex_pem_key', 'ex_pem_certificate',
                                          'ex_user_ssh_private_key', 'ex_user_ssh_public_key' ] )

  def __init__( self, stratuslabEndpoint ):
    """
    Constructor directly reads the named stratuslabEndpoint section
    in the configuration.

    :Parameters:
      **stratuslabEndpoint** - `string`
        name of the element containing the configuration for a StratusLab cloud
    """

    super( StratusLabConfiguration, self ).__init__()

    options = gConfig.getOptionsDict( '%s/%s' % ( self.ENDPOINT_PATH, stratuslabEndpoint ) )

    if not options[ 'OK' ]:
      self.log.error(options[ 'Message' ] )
      options = {}
    else:
      options = options[ 'Value' ]

    # Save a shallow copy of the given dictionary for safety.
    self._options = copy.copy(options)

    # Remove any 'None' mappings from the dictionary.
    for key, value in self._options.items():
      if value is None:
        del self._options[ key ]

  def config( self ):
    return copy.copy( self._options )

  def validate( self ):

    cfg = self.config()

    defined_keys      = frozenset( cfg.keys() )
    all_required_keys = self.DIRAC_REQUIRED_KEYS.union( self.STRATUSLAB_REQUIRED_KEYS )
    all_keys          = all_required_keys.union( self.STRATUSLAB_OPTIONAL_KEYS )

    missing_keys = all_required_keys.difference( defined_keys )
    if missing_keys:
      return S_ERROR( 'Missing mandatory keys for StratusLab endpoint configuration: %s' % str( missing_keys ) )

    unknown_keys = defined_keys.difference( all_keys )
    if unknown_keys:
      return S_ERROR( 'Unknown keys in StratusLab endpoint configuration: %s' % unknown_keys )

    # username and password must either both be defined or both be undefined
    credential_keys = frozenset( [ 'ex_username', 'ex_password' ] )
    defined_credential_keys = defined_keys.intersection( credential_keys )
    if not ( len( defined_credential_keys ) == 0 or len( defined_credential_keys ) == 2 ):
      return S_ERROR( 'the keys "%s" must be both defined or both undefined' % credential_keys )

    # same for the user's certificate and key
    credential_keys = frozenset( [ 'ex_pem_key', 'ex_pem_certificate' ] )
    defined_credential_keys = defined_keys.intersection( credential_keys )
    if not ( len( defined_credential_keys ) == 0 or len( defined_credential_keys ) == 2):
      return S_ERROR( 'the keys "%s" must be both defined or both undefined' % credential_keys )

    self.log.info( '*' * 50 )
    self.log.info( 'StratusLab Endpoint Configuration' )
    for key, value in sorted( cfg.iteritems() ):
      if key != 'ex_password':
        self.log.info( '%s : %s' % ( key, value ) )
      else:
        self.log.info( '%s : ********' % key )
    self.log.info( '*' * 50 )

    return S_OK()
  
#...............................................................................    

class ImageConfiguration( object ):
  
  def __init__( self, imageName, endPointName ):
  
    self.log = gLogger.getSubLogger( 'ImageConfiguration' )
   
    imageOptions = gConfig.getOptionsDict( '/Resources/VirtualMachines/Images/%s' % imageName )
    if not imageOptions[ 'OK' ]:
      self.log.error( imageOptions[ 'Message' ] )
      imageOptions = {}
    else:
      imageOptions = imageOptions[ 'Value' ] 
  
    self.__ic_DIRACImageName  = imageName

    endPointName = endPointName.strip()

    # A DIRAC image can have different boot image names in the cloud endPoints 
    bootImageOptions = gConfig.getOptionsDict( '/Resources/VirtualMachines/Images/%s/BootImages' % imageName )
    if not bootImageOptions[ 'OK' ]:
      self.log.error( bootImageOptions[ 'Message' ] )
      return
    bootImageOptions = bootImageOptions[ 'Value' ] 
    bootImageName = None
    for bootEndpoint, bootImage in bootImageOptions.items():
      if endPointName == bootEndpoint:
        bootImageName  =  bootImage
        break
    if bootImageName is None:
      self.log.error( 'Missing mandatory boot image of the endPoint %s in BootImages section, image %s' % (endPointName, imageName) )
    self.__ic_bootImageName  = bootImageName

    # A DIRAC image can have different flavor names names in the cloud endPoints 
    flavorOptions = gConfig.getOptionsDict( '/Resources/VirtualMachines/Images/%s/Flavors' % imageName )
    if not flavorOptions[ 'OK' ]:
      self.log.error( flavorOptions[ 'Message' ] )
      return
    flavorOptions = flavorOptions[ 'Value' ] 
    flavorName = None
    for bootEndpoint, flavor in flavorOptions.items():
      if endPointName == bootEndpoint:
        flavorName  =  flavor
        break
    if flavorName is None:
      self.log.error( 'Missing mandatory flavor of the endPoint %s in BootImages section, image %s' % (endPointName, imageName) )
    self.__ic_flavorName     = flavorName

    self.__ic_contextMethod  = imageOptions.get( 'contextMethod'     , None )
    #optional:
    self.__ic_maxAllowedPrice     = imageOptions.get( 'maxAllowedPrice'        , None )
    self.__ic_keyName     = imageOptions.get( 'keyName'        , None )
    #self.__ic_contextConfig = ContextConfig( self.__ic_bootImageName, self.__ic_contextMethod )
    self.__ic_contextConfig  = ContextConfig( imageName, self.__ic_contextMethod )

  def config( self ):
    
    config = {
              'DIRACImageName' : self.__ic_DIRACImageName,
              'bootImageName' : self.__ic_bootImageName,
              'contextMethod' : self.__ic_contextMethod,
              'flavorName'    : self.__ic_flavorName,  
              'maxAllowedPrice'    : self.__ic_maxAllowedPrice,  
              'keyName'    : self.__ic_keyName,  
              'contextConfig' : self.__ic_contextConfig.config()
              }
      
    return config

  def validate( self ):
    
    if self.__ic_DIRACImageName is None:
      return S_ERROR( 'self._ic_DIRACImageName is None' )
    if self.__ic_bootImageName is None:
      return S_ERROR( 'self._ic_bootImageName is None' )
    if self.__ic_flavorName is None:
      return S_ERROR( 'self._ic_flavorName is None' )
    if self.__ic_contextMethod is None:
      return S_ERROR( 'self._ic_contextMethod is None' )
   
    validateContext = self.__ic_contextConfig.validate()
    if not validateContext[ 'OK' ]:
      self.log.error( validateContext[ 'Message' ] )
      return validateContext
    
    self.log.info( 'Displaying image info' )
    self.log.info( '*' * 50 )
    self.log.info( 'ic_DIRACImageName %s' % self.__ic_DIRACImageName )
    self.log.info( 'ic_bootImageName %s' % self.__ic_bootImageName )
    self.log.info( 'ic_flavorName %s' % self.__ic_flavorName )
    if not self.__ic_maxAllowedPrice is None:
      self.log.info( 'ic_maxAllowedPrice %s' % self.__ic_maxAllowedPrice )
    if not self.__ic_keyName is None:
      self.log.info( 'ic_keyName %s' % self.__ic_keyName )
    self.log.info( 'ic_contextMethod %s' % self.__ic_contextMethod )
    for key, value in self.__ic_contextConfig.config().iteritems():
      self.log.info( '%s : %s' % ( key, value ) )
    self.log.info( '*' * 50 )  
      
    return S_OK()   

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    
