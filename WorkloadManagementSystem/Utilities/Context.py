# $HeadURL$
"""
  Context
  
  This module contains the helpers needed to fetch the contextualisation setup
  configuration. The usage is always the same. Instance a new ContextConfig object,
  and it will make sure is the right one.
  
  This module provides Context helpers for the contextualisation methods:
  * occiOpenNebula
  * ssh
  * adhoc
  * amiconfig
"""

# DIRAC
from DIRAC import gConfig, gLogger, S_ERROR, S_OK

__RCSID__ = '$Id: $'

class ContextConfig( object ):
  """
  ContextConfig class. Is the main and also the base class for all the different
  context classes.
  """
  
  # CS path where the context configuration lies
  CONTEXT_PATH   = '/Resources/VirtualMachines/Images/%s/%s'
  # Mandatory keys for the basic Context Configuration. Are the options that 
  # will be used by other components. Is a sanity check against a miss-configured
  # ConfigurationService.
  MANDATORY_KEYS = [ 'bootImageName', 'flavorName', 'contextMethod' ]
  
  def __new__( cls, _imageName, contextName ):
    """
    Uses the contextName parameter to decide which class to load. If not in
    `ssh`, `adhoc`, `amiconfig` or `OcciOpennebulaContext` raises a NotImplementedException
    
    :Parameters:
      **_imageName** - `string` 
        name of the image on the CS ( unused )
      **contextName** - `string`
        string with the type of context on the CS. It decides which class to load. 
        Either `ssh`,`adhoc`,`amiconfig`.
    
    :raises: NotImplementedException    
    """
    if cls is ContextConfig:
      if contextName == 'ssh':
        cls = SSHContext
      elif contextName == 'adhoc':
        cls = AdHocContext
      elif contextName == 'amiconfig':
        cls = AmiconfigContext
      elif contextName == 'occi_opennebula':
        cls = OcciOpennebulaContext
      else:
        raise NotImplementedError( "No context implemented for %s" % contextName )
      
    return super( ContextConfig, cls ).__new__( cls )  

  def __init__( self, imageName, contextName ):
    """
    Constructor. Gets section from <CONTEXT_PATH>/<imageName>/<contextName> or
    empty dictionary in case it fails.

    :Parameters:
      **imageName** - `string`
        name of the image on the CS
      **contextName** - `string`
        string with the type of context on the CS. It decides which class to load. 
        Either `ssh`,`adhoc`,`amiconfig`, `occi_opennebula`.
    
    """
    # Get sublogger with the class name loaded in __new__
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
 
    contextOptions = gConfig.getOptionsDict( self.CONTEXT_PATH % ( imageName, contextName ) )
    if not contextOptions[ 'OK' ]:
      self.log.error( contextOptions[ 'Message' ] )
      contextOptions = {}
    else:
      contextOptions = contextOptions[ 'Value' ] 
    
    self.__context = contextOptions
    
  def config( self ):
    """
    Method that returns a copy of the context dictionary.
    
    :return: dictionary
    """
    # A copy instead of the original one, just in case.
    return self.__context.copy()

  def validate( self ):
    """
    Method that validates the context configuration obtained from the CS. If 
    <MANDATORY_KEYS> are not present in the context configuration dictionary
    key set, returns S_ERROR. Otherwise, prints them and returns S_OK.  
      
    :return: S_OK | S_ERROR  
    """

 
    contextConfig = self.config()
  
    missingKeys = set( self.MANDATORY_KEYS ).difference( set( contextConfig.keys() ) ) 
    if missingKeys:
      return S_ERROR( 'Missing mandatory keys on endpointConfig %s' % str( missingKeys ) )

#    self.log.info( '*' * 50 )
#    self.log.info( 'Displaying context info' )
#    for key, value in self.contextConfig.iteritems:
#      self.log.info( '%s : %s' % ( key, value ) )
#    self.log.info( 'User and Password are NOT printed.')
#    self.log.info( '*' * 50 )
    
    return S_OK()  
    
#...............................................................................
# SSH Context

class SSHContext( ContextConfig ):
  """
  SSHContext defines the following mandatory keys:
  
  * flavorName
  * vmCertPath : the virtualmachine cert to be copied on the VM of a specific endpoint
  * vmKeyPath  : the virtualmachine key to be copied on the VM of a specific endpoint
  * vmContextualizeScriptPath
  * vmCvmfsContextURL : the cvmfs context URL
  * vmDiracContextURL : the dirac specific context URL
  * vmRunJobAgentURL : the runsvdir run file for JobAgent
  * vmRunVmMonitorAgentURL : the runsvdir run file vmMonitorAgent 
  * vmRunVmUpdaterJobAgentURL : the runsvdir run file VirtualMachineConfigUpdater agent
  * vmRunLogAgentURL : the runsvdir run.log file 
  * cpuTime : the VM cpuTime of the image
  * cloudDriver : the endpoint dirac cloud driver
  * pubkey_path: path to the public key .pem file to be inserted in the VM allowing ssh
  * ex_keyname: keyname to be created asociated to the ex_pubkey_path
  
  """
  MANDATORY_KEYS = [ 'vmCertPath', 
                     'vmKeyPath', 'vmContextualizeScriptPath', 'vmCvmfsContextURL', 
                     'vmDiracContextURL', 'vmRunJobAgentURL', 'vmRunVmMonitorAgentURL', 
                     'vmRunVmUpdaterAgentURL', 'vmRunLogAgentURL', 'ex_pubkey_path', 'ex_keyname' ]

#...............................................................................    
# AdHoc Context

class AdHocContext( ContextConfig ):
  """
  AdHocContext does not define any mandatory key.
  """
  pass

#...............................................................................
# AmiconfigContext

class AmiconfigContext( ContextConfig ):
  """
  AmiconfigContext defines the following mandatory keys:
  
  * ex_size
  * ex_image
  * ex_security_groups
  * ex_userdata
    
  """

  MANDATORY_KEYS = [ 'ex_size', 'ex_image', 
                     'ex_security_groups', 'ex_userdata' ]

#...............................................................................
# OcciOpennebulaContext

class OcciOpennebulaContext( ContextConfig ):
  """
  AmiconfigContext defines the following mandatory keys:
  
  * hdcImangeName
    
  """

  MANDATORY_KEYS = [ 'hdcImageName','context_files_url' ]

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
