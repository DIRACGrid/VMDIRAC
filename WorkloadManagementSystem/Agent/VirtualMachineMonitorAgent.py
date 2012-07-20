__RCSID__ = "$Id$"

import time
import urllib2
import os
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC import gLogger, S_OK, S_ERROR, gConfig, rootPath
from DIRAC.Core.Utilities import List, Network
from VMDIRAC.WorkloadManagementSystem.Client.ServerUtils import virtualMachineDB
from VMDIRAC.WorkloadManagementSystem.private.OutputDataExecutor import OutputDataExecutor

try:
  from hashlib import md5
except:
  from md5 import md5

class VirtualMachineMonitorAgent( AgentModule ):

  def getAmazonVMId( self ):
    try:
      fd = urllib2.urlopen( "http://instance-data.ec2.internal/latest/meta-data/instance-id" )
      id = fd.read().strip()
      fd.close()
      return S_OK( id )
    except Exception, e:
      return S_ERROR( "Could not retrieve amazon instance id: %s" % str( e ) )

  def getOcciVMId( self ):
    try:
      fd = open(os.path.join('/etc','VMID'),'r')
      id = fd.read().strip()
      fd.close()
      return S_OK( id )
    except Exception, e:
      return S_ERROR( "Could not retrieve occi instance id: %s" % str( e ) )

  def getUptime( self ):
    fd = open( "/proc/uptime" )
    uptime = float( List.fromChar( fd.read().strip(), " " )[0] )
    fd.close()
    return uptime

  def getGenericVMId( self ):
    fd = open( "/proc/stat" )
    lines = fd.readlines()
    fd.close()
    btime = False
    for line in lines:
      fields = List.fromChar( line, " " )
      if fields[0] == "btime":
        btime = fields[1]
        break
    if not btime:
      return S_ERROR( "Could not find btime in /proc/stat" )
    hash = md5()
    hash.update( btime )
    netData = Network.discoverInterfaces()
    for iface in sorted( netData ):
      if iface == "lo":
        continue
      hash.update( netData[ iface ][ 'mac' ] )
    return S_OK( hash.hexdigest() )

  def __getCSConfig( self ):
    if not self.vmName:
      return S_ERROR( "/LocalSite/VirtualMachineName is not defined" )
    #Variables coming from the vm 
    imgPath = "/Resources/VirtualMachines/Images/%s" % self.vmName
    # temporal patch for occi until CS Endpoint implemented:
    for csOption, csDefault, varName in ( ( "MinWorkingLoad", 0.01, "vmMinWorkingLoad" ),
                                          ( "LoadAverageTimespan", 60, "vmLoadAvgTimespan" ),
                                          ( "JobWrappersLocation", "/opt/dirac/pro/job/Wrapper/", "vmJobWrappersLocation" ),
                                          ( "HaltPeriod", 1200, "haltPeriod" ),
                                          ( "HaltBeforeMargin", 300, "haltBeforeMargin" ),
                                          ( "HeartBeatPeriod", 600, "heartBeatPeriod" ),
                                        ):

      path = "%s/%s" % ( imgPath, csOption )
      value = gConfig.getValue( path, csDefault )
      if not value:
        return S_ERROR( "%s is not defined" % path )
      setattr( self, varName, value )

    self.haltBeforeMargin = max( self.haltBeforeMargin, int( self.am_getPollingTime() ) + 5 )
    self.haltPeriod = max( self.haltPeriod, int( self.am_getPollingTime() ) + 5 )
    self.heartBeatPeriod = max( self.heartBeatPeriod, int( self.am_getPollingTime() ) + 5 )

    gLogger.info( "** VM Info **" )
    gLogger.info( "Name                  : %s" % self.vmName )
    gLogger.info( "Min Working Load      : %f" % self.vmMinWorkingLoad )
    gLogger.info( "Load Avg Timespan     : %d" % self.vmLoadAvgTimespan )
    gLogger.info( "Job wrappers location : %s" % self.vmJobWrappersLocation )
    gLogger.info( "Halt Period           : %d" % self.haltPeriod )
    gLogger.info( "Halt Before Margin    : %d" % self.haltBeforeMargin )
    gLogger.info( "HeartBeat Period      : %d" % self.heartBeatPeriod )
    if self.vmId:
      gLogger.info( "ID                    : %s" % self.vmId )
    gLogger.info( "*************" )
    return S_OK()

  def __declareInstanceRunning( self ):
    #Connect to VM monitor and register as running
    retries = 3
    sleepTime = 30
    for i in range( retries ):
      result = virtualMachineDB.declareInstanceRunning( self.vmId, self.ipAddress )
      if result[ 'OK' ]:
        gLogger.info( "Declared instance running" )
        return result
      gLogger.error( "Could not declare instance running", result[ 'Message' ] )
      if i < retries - 1 :
        gLogger.info( "Sleeping for %d seconds and retrying" % sleepTime )
        time.sleep( sleepTime )
    return S_ERROR( "Could not declare instance running after %d retries" % retries )

  def initialize( self ):
    #Init vars
    self.vmName = ""
    self.__loadHistory = []
    self.__outDataExecutor = OutputDataExecutor()
    self.vmId = ""
    self.am_setOption( "MaxCycles", 0 )
    self.am_setOption( "PollingTime", 60 )
    #Discover id based on flavor
    #flavor = gConfig.getValue( "/LocalSite/Flavor", "" ).lower()
    # epilog.sh dirac-configure con /LocalSite/Contextualization
    self.contextualization = gConfig.getValue( "/LocalSite/Contextualization", "" ).lower()
    gLogger.info( "ID contextualization is %s" % self.contextualization )
    if self.contextualization == 'generic':
      result = self.getGenericVMId()
    elif self.contextualization == 'amazon':
      result = self.getAmazonVMId()
    elif self.contextualization == 'occi':
      result = self.getOcciVMId()
    else:
      return S_ERROR( "Unknown VM CloudEndpoint (%s)" % self.contextualization )
    if not result[ 'OK' ]:
      return S_ERROR( "Could not generate VM id: %s" % result[ 'Message' ] )
    self.vmId = result[ 'Value' ]
    gLogger.info( "VM ID is %s" % self.vmId )
    #Discover net address
    netData = Network.discoverInterfaces()
    for iface in sorted( netData ):
      if iface.find( "eth" ) == 0:
        self.ipAddress = netData[ iface ][ 'ip' ]
        break
    gLogger.info( "IP Address is %s" % self.ipAddress )
    #Declare instance running
    result = self.__declareInstanceRunning()
    if not result[ 'OK' ]:
      gLogger.error( "Could not declare instance running", result[ 'Message' ] )
      self.__haltInstance()
      return S_ERROR( "Halting!" )
    self.__instanceInfo = result[ 'Value' ]
    self.vmName = self.__instanceInfo[ 'Image' ][ 'Name' ]
    gLogger.info( "Image name is %s" % self.vmName )
    #Get the cs config
    result = self.__getCSConfig()
    if not result[ 'OK' ]:
      return result
    #Define the shifter proxy needed
    self.am_setModuleParam( "shifterProxy", "DataManager" )
    #Start output data executor
    odeCSPAth = "/Resources/VirtualMachines/Images/%s/OutputData" % self.vmName
    self.__outDataExecutor = OutputDataExecutor( odeCSPAth )
    return S_OK()


  def __getLoadAvg( self ):
    result = self.__getCSConfig()
    if not result[ 'OK' ]:
      return result
    fd = open( "/proc/loadavg", "r" )
    data = [ float( v ) for v in List.fromChar( fd.read(), " " )[:3] ]
    fd.close()
    self.__loadHistory.append( data )
    numRequiredSamples = max( self.vmLoadAvgTimespan / self.am_getPollingTime(), 1 )
    while len( self.__loadHistory ) > numRequiredSamples:
      self.__loadHistory.pop( 0 )
    gLogger.info( "Load averaged through %d seconds" % self.vmLoadAvgTimespan )
    gLogger.info( " %d/%s required samples to average load" % ( len( self.__loadHistory ),
                                                                numRequiredSamples ) )
    avgLoad = 0
    for f in self.__loadHistory:
      avgLoad += f[0]
    return avgLoad / len( self.__loadHistory ), len( self.__loadHistory ) == numRequiredSamples

  def __getNumJobWrappers( self ):
    if not os.path.isdir( self.vmJobWrappersLocation ):
      return 0
    nJ = 0
    for entry in os.listdir( self.vmJobWrappersLocation ):
      entryPath = os.path.join( self.vmJobWrappersLocation, entry )
      if os.path.isfile( entryPath ) and entry.find( "Wrapper_" ) == 0:
        nJ += 1
    return nJ

  def execute( self ):
    #Process transfers
    self.__outDataExecutor.checkForTransfers()
    #Get load
    avgLoad, avgRequiredSamples = self.__getLoadAvg()
    gLogger.info( "Load Average is %.2f" % avgLoad )
    if not avgRequiredSamples:
      gLogger.info( " Not all required samples yet there" )
    #Do we need to send heartbeat?
    uptime = self.getUptime()
    hours = int( uptime / 3600 )
    minutes = int( uptime - hours * 3600 ) / 60
    seconds = uptime - hours * 3600 - minutes * 60
    gLogger.info( "Uptime is %.2f (%d:%02d:%02d)" % ( uptime, hours, minutes, seconds ) )
    #Num jobs 
    numJobs = self.__getNumJobWrappers()
    gLogger.info( "There are %d job wrappers" % numJobs )
    gLogger.info( "Transferred %d files" % self.__outDataExecutor.getNumOKTransferredFiles() )
    gLogger.info( "Transferred %d bytes" % self.__outDataExecutor.getNumOKTransferredBytes() )
    if uptime % self.heartBeatPeriod <= self.am_getPollingTime():
      #Heartbeat time!
      gLogger.info( "Sending hearbeat..." )
      result = virtualMachineDB.instanceIDHeartBeat( self.vmId, avgLoad, numJobs,
                                                     self.__outDataExecutor.getNumOKTransferredFiles(),
                                                     self.__outDataExecutor.getNumOKTransferredBytes(),
                                                     int( uptime ) )
      if result[ 'OK' ]:
        gLogger.info( " heartbeat sent!" )
      else:
        gLogger.error( "Could not send heartbeat", result[ 'Message' ] )
      self.__processHeartBeatMessage( result[ 'Value' ] )
    #Check if there are local outgoing files
    localOutgoing = self.__outDataExecutor.getNumLocalOutgoingFiles()
    if localOutgoing or self.__outDataExecutor.transfersPending():
      gLogger.info( "There are transfers pending. Not halting." )
      return S_OK()
    else:
      gLogger.info( "No local outgoing files to be transferred" )
    #Do we need to check if halt?
    if avgRequiredSamples and uptime % self.haltPeriod + self.haltBeforeMargin > self.haltPeriod:
      gLogger.info( "Load average is %s (minimum for working instance is %s)" % ( avgLoad,
                                                                                  self.vmMinWorkingLoad ) )
      #If load less than X, then halt!
      if avgLoad < self.vmMinWorkingLoad:
        self.__haltInstance( avgLoad )
    return S_OK()

  def __processHeartBeatMessage( self, hbMsg ):
    if 'stop' in hbMsg and hbMsg[ 'stop' ]:
      #Write stop file for jobAgent
      gLogger.info( "Received STOP signal. Writing stop files..." )
      for agentName in [ "WorkloadManagement/JobAgent" ]:
        ad = os.path.join( *agentName.split( "/" ) )
        stopDir = os.path.join( gConfig.getValue( '/LocalSite/InstancePath', rootPath ), 'control', ad )
        stopFile = os.path.join( stopDir, "stop_agent" )
        try:
          if not os.path.isdir( stopDir ):
            os.makedirs( stopDir )
          fd = open( stopFile, "w" )
          fd.write( "stop!" )
          fd.close()
          gLogger.info( "Wrote stop file %s for agent %s" % ( stopFile, agentName ) )
        except Exception, e:
          gLogger.error( "Could not write stop agent file", stopFile )
    if 'halt' in hbMsg and hbMsg[ 'halt' ]:
      self.__haltInstance()

  def __haltInstance( self, avgLoad = 0 ):
    gLogger.info( "Halting instance..." )
    retries = 3
    sleepTime = 10
    for i in range( retries ):
      result = virtualMachineDB.declareInstanceHalting( self.vmId, avgLoad, self.contextualization )
      if result[ 'OK' ]:
        gLogger.info( "Declared instance halting" )
        break
      gLogger.error( "Could not send halting state", result[ 'Message' ] )
      if i < retries - 1 :
        gLogger.info( "Sleeping for %d seconds and retrying" % sleepTime )
        time.sleep( sleepTime )

    #time.sleep( sleepTime )
      
    # all endpoint:
    gLogger.info( "Executing system halt..." )
    os.system( "halt" )
