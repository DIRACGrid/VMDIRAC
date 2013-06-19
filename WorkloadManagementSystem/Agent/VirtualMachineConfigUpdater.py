# $HeadURL$
""" VirtualMachineConfigUpdater

  Agent that monitors updates dirac.cfg/ReleaseVersion with the latest
  and greatest value from the CS/Operations/<setup>/Pilot/Version

"""

import glob
import os

from DIRAC                                               import gConfig, S_OK, rootPath
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.AgentModule                         import AgentModule
from DIRAC.Core.Utilities.CFG                            import CFG

__RCSID__ = '$Id: $'


class VirtualMachineConfigUpdater( AgentModule ):

  def __init__( self, *args, **kwargs ):
    """ Constructor
    
    """
        
    AgentModule.__init__( self, *args, **kwargs )  
  
    self.opHelper      = None
    self.controlPath   = '' 
    self.stopAgentPath = ''
  
  
  def initialize( self ):
    """ initialize
    
    """
    
    self.opHelper    = Operations()
    
    # Set control path 
    instancePath       = gConfig.getValue( '/LocalSite/InstancePath', rootPath )
    self.controlPath   = os.path.join( instancePath, 'control', '*', '*' )
    self.stopAgentPath = os.path.join( self.controlPath, 'stop_agent' )
  
    self.log.info( 'Instance path: %s' % instancePath )
    self.log.info( 'Control path: %s' % self.controlPath )
    self.log.info( 'Stop Agent path: %s' % self.stopAgentPath )
  
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
    localCFG.loadFromFile( gConfig.diracConfigFilePath )
    releaseVersion = localCFG.getRecursive( 'LocalSite/ReleaseVersion' )
    
    if pilotVersion > releaseVersion:
    
      self.log.info( 'UPDATING %s > %s' % ( pilotVersion, releaseVersion ) )
    
      localCFG.setOption( 'LocalSite/ReleaseVersion', pilotVersion )
      localCFG.writeToFile( gConfig.diracConfigFilePath )  
      
      self.touchStopAgents()
         
    return S_OK()     
   
   
  def findStopAgents( self ):
    """ findStopAgents
    
    """  
        
    stopAgents = glob.glob( self.stopAgentPath )
    for stopAgent in stopAgents:
      self.log.warn( 'Found stop_agent in: %s' % stopAgent )
    
    return S_OK( stopAgents )
    
    
  def touchStopAgents( self ):
    """ touchStopAgents
    
    """
    
    controlAgents = glob.glob( self.controlPath )
    for controlAgent in controlAgents:
      
      stopAgent = os.path.join( controlAgent, 'stop_agent' )
      
      with file( stopAgent, 'a' ):
        os.utime( stopAgent, None )
        self.log.info( 'Written stop_agent in: %s' % stopAgent ) 

    return S_OK()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
