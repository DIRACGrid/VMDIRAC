# $HeadURL$
""" VirtualMachineConfigUpdater

  Agent that monitors updates dirac.cfg/ReleaseVersion with the latest
  and greatest value from the CS/Operations/<setup>/Pilot/Version

"""

import glob
import os

from DIRAC                                               import S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.AgentModule                         import AgentModule
from DIRAC.Core.Utilities.CFG                            import CFG

__RCSID__ = '$Id: $'


class VirtualMachineConfiguUpdater( AgentModule ):

  CONFIG_FILE = '/opt/dirac/etc/dirac.cfg'

  def __init__( self, *args, **kwargs ):
    """ Constructor
    
    """
        
    AgentModule.__init__( self, *args, **kwargs )  
  
    self.opHelper = None 
  
  
  def initialize( self ):
    """ initialize
    
    """
    
    self.opHelper = Operations() 
  
    return S_OK()
  
  
  def execute( self ):
    """ execute
    
    """

    stopAgents = self.findStopAgents()[ 'Value' ]
    if stopAgents:
      self.log.info( 'Aborting, there are stop_agents to be picked' )
      return S_OK()

    pilotVersion = self.opHelper.getValue( 'Pilot/Version', '' )
    localCFG     = CFG()
    
    #load local CFG
    localCFG.loadFromFile( self.CONFIG_FILE )
    releaseVersion = localCFG.getRecursive( 'LocalSite/ReleaseVersion' )
    
    if pilotVersion > releaseVersion:
    
      self.log.info( 'UPDATING %s > %s' % ( pilotVersion, releaseVersion ) )
    
      localCFG.setOption( 'LocalSite/ReleaseVersion', pilotVersion )
      localCFG.writeToFile( self.CONFIG_FILE )  
      
      self.touchStopAgents()
         
    return S_OK()     
   
   
  def findStopAgents( self ):
    """ findStopAgents
    
    """  
    
    stopAgents = glob.glob( '/opt/dirac/control/*/*/stop_agent' )
    for stopAgent in stopAgents:
      self.log.warn( 'Found stop_agent in: %s' % stopAgent )
    
    return S_OK( stopAgents )
    
    
  def touchStopAgents( self ):
    """ touchStopAgents
    
    """
    
    controlAgents = glob.glob( '/opt/dirac/control/*/*' )
    for controlAgent in controlAgents:
      
      stopAgent = '%s/stop_agent' % controlAgent
      
      with file( stopAgent, 'a' ):
        os.utime( stopAgent, None )
        self.log.info( 'Written stop_agent in: %s' % stopAgent ) 

    return S_OK()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF