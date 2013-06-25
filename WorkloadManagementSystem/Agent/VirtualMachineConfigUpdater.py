# $HeadURL$
""" VirtualMachineConfigUpdater

  Agent that monitors updates dirac.cfg/ReleaseVersion with the latest
  and greatest value from the CS/Operations/<setup>/Pilot/Version

"""

import glob
import os

from distutils.version import LooseVersion

from DIRAC                                               import gConfig, S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.AgentModule                         import AgentModule
from DIRAC.Core.Utilities.CFG                            import CFG

__RCSID__  = '$Id: $'
AGENT_NAME = 'WorkloadManagement/VirtualMachineConfigUpdater'


class VirtualMachineConfigUpdater( AgentModule ):

  def __init__( self, *args, **kwargs ):
    """ Constructor
    
    """
        
    AgentModule.__init__( self, *args, **kwargs )  
  
    self.opHelper      = None
    self.stopAgentPath = ''
    self.cfgToUpdate   = '' 
  
  def initialize( self ):
    """ initialize
    
    """
    
    self.opHelper    = Operations()
    
    self.stopAgentPath = self.am_getStopAgentFile().replace( AGENT_NAME, '*/*' )
    self.cfgToUpdate   = self.am_getOption( 'cfgToUpdate', gConfig.diracConfigFilePath )        
    
    self.log.info( 'Stop Agent path: %s' % self.stopAgentPath )
    self.log.info( 'Config To Update: %s' % self.cfgToUpdate )
  
    return S_OK() 
   
   
  def execute( self ):
    """ execute
    
    """

    stopAgents = self.findStopAgents()[ 'Value' ]
    if stopAgents:
      self.log.info( 'Aborting, there are stop_agents to be picked' )
      return S_OK()

    pilotVersion = self.opHelper.getValue( 'Pilot/Version', '' )
    if not pilotVersion:
      self.log.error( 'There is no pilot version on the CS' )
      return S_OK()

    pilotVersion = self.getNewestPilotVersion()
    if not pilotVersion[ 'OK' ]:
      self.log.error( pilotVersion[ 'Message' ] )
      return S_ERROR( pilotVersion[ 'Message' ] )
    pilotVersion = pilotVersion[ 'Value' ]
      
    localCFG = CFG()
    
    #load local CFG
    localCFG.loadFromFile( self.cfgToUpdate )
    releaseVersion = localCFG.getRecursive( 'LocalSite/ReleaseVersion' )[ 'value' ]
    
    self.log.info( 'PilotVersion : %s' % pilotVersion )
    self.log.info( 'ReleaseVersion : %s' % releaseVersion )
            
    if LooseVersion( pilotVersion ) > LooseVersion( releaseVersion ):
    
      self.log.info( 'UPDATING %s > %s' % ( pilotVersion, releaseVersion ) )
    
      localCFG.setOption( 'LocalSite/ReleaseVersion', pilotVersion )
      localCFG.writeToFile( self.cfgToUpdate )  
      
      self.touchStopAgents()
    
    else:
      
      self.log.info( 'Nothing to do' )
    
    return S_OK()     
   

  def getNewestPilotVersion( self ):
    """ getNewestPilotVersion
    
    """
    
    pilotVersion = self.opHelper.getValue( 'Pilot/Version', [] )
    if not pilotVersion:
      return S_ERROR( 'Empty pilot version' )
        
    pilotVersion = [ LooseVersion( pV ) for pV in pilotVersion ]
    pilotVersion.sort()
    
    return S_OK( pilotVersion.pop().vstring )
   
      
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
    
    stopAgentDirs = glob.glob( self.stopAgentPath.replace( 'stop_agent' , '' ) )
    for stopAgentDir in stopAgentDirs:
      
      # Do not restart itself
      if AGENT_NAME in stopAgentDir:
        continue
      
      stopAgentFile = os.path.join( stopAgentDir, 'stop_agent' )
      
      with file( stopAgentFile, 'a' ):
        os.utime( stopAgentFile, None )
        self.log.info( 'Written stop_agent in: %s' % stopAgentFile )

    return S_OK()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
