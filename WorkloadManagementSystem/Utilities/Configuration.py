# $HeadURL$
"""
  NovaImage
"""

# DIRAC
from DIRAC import gConfig, S_OK, S_ERROR

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Utilities.Context import ContextConfig

__RCSID__ = '$Id: $'

class Configuration( object ):
  
  log = None

  def config( self ):
    raise NotImplementedError( "%s.config() must be implemented" % self.__class__.__name__ )

class NovaConfiguration( Configuration ):
  
  MANDATORY_KEYS = [ 'size', 'image', 'ex_force_auth_url', 
                     'ex_force_service_region', 'ex_tenant_name' ]
    
  def __init__( self, novaEndpoint ):
       
    novaOptions = gConfig.getOptionsDict( '/Resources/VirtualMachines/CloudEndpoints/%s' % novaEndpoint )
    if not novaOptions[ 'OK' ]:
      self.log.error( novaOptions[ 'Message' ] )
      novaOptions = {}
    else:
      novaOptions = novaOptions[ 'Value' ] 

    # Purely endpoint configuration ............................................              
    # This two are passed as arguments, not keyword arguments
    self._user                    = novaOptions.get( 'password'               , None )
    self._password                = novaOptions.get( 'user'                   , None )
    
    self._ex_force_auth_token     = novaOptions.get( 'ex_force_auth_token'    , None )
    self._ex_force_auth_url       = novaOptions.get( 'ex_force_auth_url'      , None )
    self._ex_force_auth_version   = novaOptions.get( 'ex_force_auth_version'  , None )
    self._ex_force_base_url       = novaOptions.get( 'ex_force_base_url'      , None )
    self._ex_force_service_name   = novaOptions.get( 'ex_force_service_name'  , None )
    self._ex_force_service_region = novaOptions.get( 'ex_force_service_region', None )
    self._ex_force_service_type   = novaOptions.get( 'ex_force_service_type'  , None )   
    self._ex_tenant_name          = novaOptions.get( 'ex_tenant_name'         , None )

    # VM configuration .........................................................
    self._ex_size     = novaOptions.get( 'ex_size'    , None )
    self._ex_image    = novaOptions.get( 'ex_image'   , None )
    self._ex_metadata = novaOptions.get( 'ex_metadata', None )
    self._ex_keyname  = novaOptions.get( 'ex_keyname' , None )
    self._ex_userdata = novaOptions.get( 'ex_userdata', None )

  def config( self ):
    
    config = {}
    
    for item in dir( self ):
      if not item.startswith( '_ex_' ):
        continue
      
      itemName = item.replace( '_ex', 'ex' )
      # This is a particularity of libcloud. Size and image do not have the ex_
      if 'size' in itemName:
        itemName = 'size' 
      elif 'image' in itemName:
        itemName = 'image'
    
      itemValue = getattr( self, item )
      if itemValue is not None:
        config[ itemName ] = itemValue
    
    return config  

  def validateNovaConfig( self ):
    
    endpointConfig = self.config()
    
    missingKeys = set( self.MANDATORY_KEYS ).difference( set( endpointConfig.keys() ) ) 
    if missingKeys:
      return S_ERROR( 'Missing mandatory keys on endpointConfig %s' % str( missingKeys ) )
    
    self.log.info( 'Validating endpoint required info' )
    for key in self.MANDATORY_KEYS:
      self.log.info( '%s : %s' % ( key, endpointConfig[ key ] ) )
    
    # on top of the MANDATORY_KEYS, we make sure the user & password are set
    if self._user is None:
      return S_ERROR( 'User is None' )
    if self._password is None:
      return S_ERROR( 'Password is None' )
        
    return S_OK()
    
#...............................................................................    

class ImageConfiguration( Configuration ):
  
  def __init__( self, imageName ):
    
    imageOptions = gConfig.getOptionsDict( '/Resources/VirtualMachines/Images/%s' % imageName )
    if not imageOptions[ 'OK' ]:
      self.log.error( imageOptions[ 'Messages' ] )
      imageOptions = {}
    else:
      imageOptions = imageOptions[ 'Value' ] 
  
    self._ic_bootImageName = imageOptions.get( 'bootImageName', None )
    self._ic_contextMethod = imageOptions.get( 'contextMethod', None )
    self._ic_contextConfig = ContextConfig( self._ic_bootImageName, self._ic_contextMethod )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    