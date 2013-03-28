# $HeadURL$
"""
  Context
"""

# DIRAC
from DIRAC import gConfig, gLogger

__RCSID__ = '$Id: $'

class ContextConfig( object ):

  CONTEXT_PATH = '/Resources/VirtualMachines/Images/%s/%s'

  def __new__( cls, imageName, contextName ):
    
    if cls is ContextConfig:
      if contextName == 'ssh':
        cls = SSHContext
      elif contextName == 'adhoc':
        cls = AdHocContext
      elif contextName == 'amiconfig':
        cls = AmiconfigContext
      else:
        raise NotImplementedError( "No context implemented for %s" % contextName )
      
    return super( ContextConfig, cls ).__new__( cls )  

  def __init__( self, imageName, contextName ):
    
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    
    contextOptions = gConfig.getOptionsDict( self.CONTEXT_PATH % ( imageName, contextName ) )
    if not contextOptions[ 'OK' ]:
      self.log.error( contextOptions[ 'Message' ] )
      contextOptions = {}
    else:
      contextOptions = contextOptions[ 'Value' ] 
    
    self._context = contextOptions
    
  def config( self ):
    raise NotImplementedError( "%s.config() must be implemented" % self.__class__.__name__ )

#...............................................................................
# SSH Context

class SSHContext( ContextConfig ):
  
  def config( self ):
    return {}

#...............................................................................    
# AdHoc Context

class AdHocContext( ContextConfig ):
  
  def config( self ):
    return {}

#...............................................................................
# AmiconfigContext

class AmiconfigContext( ContextConfig ):
  
  def config( self ):
    return {}

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF