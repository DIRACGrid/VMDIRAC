# $HeadURL$
""" Configuration
  
  Module that contains all helpers needed to obtain the endpoint or image
  configuration from the CS and validate.
  
"""

# DIRAC
from DIRAC import gConfig, S_OK, S_ERROR

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Utilities.Context import ContextConfig

__RCSID__ = '$Id: $'

#...............................................................................

class EndpointConfiguration( object ):
  """
  EndpointConfiguration is the base class for all endpoints. It defines two methods
  which must be implemented on the child classes:
  * endpointConfig
  * validateEndpointConfig
  """

  log           = None
  ENDPOINT_PATH = '/Resources/VirtualMachines/CloudEndpoints'

  def endpointConfig( self ):
    """
    Must be implemented on the child class. Returns a dictionary with the configuration.
    
    :return: dict
    """
    raise NotImplementedError( "%s.endpointConfig() must be implemented" % self.__class__.__name__ )

  def validateEndpointConfig( self ):
    """
    Must be implemented on the child class. Returns S_OK / S_ERROR, depending on
    the validity / omission of the parameters in the config.
    
    :return: S_OK | S_ERROR 
    """
    raise NotImplementedError( "%s.validateEndpointConfig() must be implemented" % self.__class__.__name__ )

#...............................................................................

class NovaConfiguration( EndpointConfiguration ):
  """
  NovaConfiguration Class parses the section <novaEndpoint> 
  and builds a configuration if possible, with the information obtained from the CS.
  """

  # Keys that MUST be present on ANY Nova CloudEndpoint configuration in the CS
  MANDATORY_KEYS = [ 'ex_force_auth_url', 'ex_force_service_region', 'ex_tenant_name' ]
    
  def __init__( self, novaEndpoint ):
    """
    Constructor
    
    :Parameters:
      **novaEndpoint** - `string`
        string with the name of the CloudEndpoint defined on the CS
    """
       
    novaOptions = gConfig.getOptionsDict( '%s/%s' % ( self.ENDPOINT_PATH, novaEndpoint ) )
    if not novaOptions[ 'OK' ]:
      self.log.error( novaOptions[ 'Message' ] )
      novaOptions = {}
    else:
      novaOptions = novaOptions[ 'Value' ] 

    # Purely endpoint configuration ............................................              
    # This two are passed as arguments, not keyword arguments
    self.__user                    = novaOptions.get( 'password'               , None )
    self.__password                = novaOptions.get( 'user'                   , None )
    
    self.__ex_force_auth_token     = novaOptions.get( 'ex_force_auth_token'    , None )
    self.__ex_force_auth_url       = novaOptions.get( 'ex_force_auth_url'      , None )
    self.__ex_force_auth_version   = novaOptions.get( 'ex_force_auth_version'  , None )
    self.__ex_force_base_url       = novaOptions.get( 'ex_force_base_url'      , None )
    self.__ex_force_service_name   = novaOptions.get( 'ex_force_service_name'  , None )
    self.__ex_force_service_region = novaOptions.get( 'ex_force_service_region', None )
    self.__ex_force_service_type   = novaOptions.get( 'ex_force_service_type'  , None )   
    self.__ex_tenant_name          = novaOptions.get( 'ex_tenant_name'         , None )

  def authConfig( self ):
    
    return ( self.__user, self.__password )

  def endpointConfig( self ):
    
    config = {}
    
    for item in dir( self ):
      if not item.startswith( '__ex_' ):
        continue
      
      itemName = item.replace( '__ex', 'ex' )
    
      itemValue = getattr( self, item )
      if itemValue is not None:
        config[ itemName ] = itemValue
    
    return config  

  def validateEndpointConfig( self ):
    
    endpointConfig = self.endpointConfig()
    
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
    for key, value in self.endpointConfig.iteritems:
      self.log.info( '%s : %s' % ( key, value ) )
    self.log.info( 'User and Password are NOT printed.')
    self.log.info( '*' * 50 )
        
    return S_OK()
  
#...............................................................................    

class ImageConfiguration( object ):
  
  log = None
  
  def __init__( self, imageName ):
    
    imageOptions = gConfig.getOptionsDict( '/Resources/VirtualMachines/Images/%s' % imageName )
    if not imageOptions[ 'OK' ]:
      self.log.error( imageOptions[ 'Messages' ] )
      imageOptions = {}
    else:
      imageOptions = imageOptions[ 'Value' ] 
  
    self.__ic_bootImageName = imageOptions.get( 'bootImageName', None )
    self.__ic_contextMethod = imageOptions.get( 'contextMethod', None )
    self.__ic_contextConfig = ContextConfig( self.__ic_bootImageName, self.__ic_contextMethod )

  def imageConfig( self ):
    
    config = {
              'ic_bootImageName' : self.__ic_bootImageName,
              'ic_contextMethod' : self.__ic_contextMethod,   
              'ic_contextConfig' : self.__ic_contextConfig.contextConfig()
              }
      
    return config

  def validateImageConfig( self ):
    
    if self.__ic_bootImageName is None:
      return S_ERROR( 'self._ic_bootImageName is None' )
    if self.__ic_contextMethod is None:
      return S_ERROR( 'self._ic_contextMethod is None' )
    
    validateContext = self.__ic_contextConfig.validateContextConfig()
    if not validateContext[ 'OK' ]:
      self.log.error( validateContext[ 'Message' ] )
      return validateContext
    
    self.log.info( 'Displaying image info' )
    self.log.info( '*' * 50 )
    self.log.info( 'ic_bootImageName %s' % self.__ic_bootImageName )
    self.log.info( 'ic_contextMethod %s' % self.__ic_contextMethod )
    for key, value in self.__ic_contextConfig.contextConfig().iteritems():
      self.log.info( '%s : %s' % ( key, value ) )
    self.log.info( '*' * 50 )  
      
    return S_OK()   
    
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    