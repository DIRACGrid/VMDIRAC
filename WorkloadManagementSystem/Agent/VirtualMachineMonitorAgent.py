__RCSID__ = "$Id$"

import time
import urllib2
import os
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities import List, Network
from BelleDIRAC.WorkloadManagementSystem.Client.ServerUtils import virtualMachineDB
from BelleDIRAC.WorkloadManagementSystem.private.OutputDataExecutor import OutputDataExecutor

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
    self.vmName = gConfig.getValue( "/LocalSite/VirtualMachineName", "" )
    if not self.vmName:
      return S_ERROR( "/LocalSite/VirtualMachineName is not defined" )
    #Variables coming from the vm 
    imgPath = "/Resources/VirtualMachines/Images/%s" % self.vmName
    for csOption, csDefault, varName in ( ( "Flavor", "", "vmFlavor" ),
                                          ( "MinWorkingLoad", 1, "vmMinWorkingLoad" ),
                                          ( "LoadAverageTimespan", 900, "vmLoadAvgTimespan" ),
                                          ( "JobWrappersLocation", "/opt/dirac/pro/job/Wrapper/", "vmJobWrappersLocation" )
                                        ):

      path = "%s/%s" % ( imgPath, csOption )
      value = gConfig.getValue( path, csDefault )
      if not value:
        return S_ERROR( "%s is not defined" % path )
      setattr( self, varName, value )
    #Variables coming from the flavor
    flavorPath = "/Resources/VirtualMachines/Flavors/%s" % self.vmFlavor
    for csOption, csDefault, varName in ( ( "HaltPeriod", 3600, "haltPeriod" ),
                                          ( "HaltBeforeMargin", 300, "haltBeforeMargin" ),
                                          ( "HeartBeatPeriod", 900, "heartBeatPeriod" ),
                                        ):

      path = "%s/%s" % ( flavorPath, csOption )
      value = gConfig.getValue( path, csDefault )
      if not value:
        return S_ERROR( "%s is not defined" % path )
      setattr( self, varName, value )

    self.haltBeforeMargin = max( self.haltBeforeMargin, int( self.am_getPollingTime() ) + 5 )
    self.haltPeriod = max( self.haltPeriod, int( self.am_getPollingTime() ) + 5 )
    self.heartBeatPeriod = max( self.heartBeatPeriod, int( self.am_getPollingTime() ) + 5 )

    gLogger.info( "** VM Info **" )
    gLogger.info( "Name                  : %s" % self.vmName )
    gLogger.info( "Flavor                : %s" % self.vmFlavor )
    gLogger.info( "Min Working Load      : %d" % self.vmMinWorkingLoad )
    gLogger.info( "Load Avg Timespan     : %d" % self.vmLoadAvgTimespan )
    gLogger.info( "Job wrappers location : %s" % self.vmJobWrappersLocation )
    gLogger.info( "Halt Period           : %d" % self.haltPeriod )
    gLogger.info( "Halt Before Margin    : %d" % self.haltBeforeMargin )
    gLogger.info( "HeartBeat Period      : %d" % self.heartBeatPeriod )
    if self.vmId:
      gLogger.info( "ID                 : %s" % self.vmId )
    gLogger.info( "*************" )
    return S_OK()

  def __declareInstanceRunning( self ):
    #Connect to VM monitor and register as running
    retries = 3
    sleepTime = 30
    for i in range( retries ):
      result = virtualMachineDB.declareInstanceRunning( self.vmName, self.vmId, self.ipAddress )
      if result[ 'OK' ]:
        gLogger.info( "Declared instance running" )
        return S_OK()
      gLogger.error( "Could not declare instance running", result[ 'Message' ] )
      if i < retries - 1 :
        gLogger.info( "Sleeping for %d seconds and retrying" % sleepTime )
        time.sleep( 60 )
    return S_ERROR( "Could not declare instance running after %d retries" % retries )

  def initialize( self ):
    self.__loadHistory = []
    self.__outDataExecutor = OutputDataExecutor()
    self.vmId = ""
    result = self.__getCSConfig()
    if not result[ 'OK' ]:
      return result
    flavor = self.vmFlavor.lower()
    if flavor == 'generic':
      result = self.getGenericVMId()
    elif flavor == 'amazon':
      result = self.getAmazonVMId()
    else:
      return S_ERROR( "Unknown VM Flavor (%s)" % self.vmFlavor )
    if not result[ 'OK' ]:
      return S_ERROR( "Could not generate VM id: %s" % result[ 'Message' ] )
    self.vmId = result[ 'Value' ]
    gLogger.info( "VM ID is %s" % self.vmId )
    self.am_setOption( "MaxCycles", 0 )
    self.am_setOption( "PollingTime", 60 )
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
      return result
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
    if uptime % self.heartBeatPeriod <= self.am_getPollingTime():
      #Heartbeat time!
      gLogger.info( "Sending hearbeat..." )
      result = virtualMachineDB.instanceIDHeartBeat( self.vmId, avgLoad, self.__getNumJobWrappers(),
                                                     self.__outDataExecutor.getNumOKTransferredFiles(),
                                                     self.__outDataExecutor.getNumOKTransferredBytes() )
      if result[ 'OK' ]:
        gLogger.info( " heartbeat sent!" )
      else:
        gLogger.error( "Could not send heartbeat", result[ 'Message' ] )
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
        gLogger.info( "Halting instance..." )
        retries = 3
        sleepTime = 10
        for i in range( retries ):
          result = virtualMachineDB.declareInstanceHalting( self.vmId, avgLoad )
          if result[ 'OK' ]:
            gLogger.info( "Declared instance halting" )
            break
          gLogger.error( "Could not send halting state", result[ 'Message' ] )
          if i < retries - 1 :
            gLogger.info( "Sleeping for %d seconds and retrying" % sleepTime )
            time.sleep( 60 )

        #HALT
        gLogger.info( "Executing system halt..." )
        os.system( "halt" )
    return S_OK()


