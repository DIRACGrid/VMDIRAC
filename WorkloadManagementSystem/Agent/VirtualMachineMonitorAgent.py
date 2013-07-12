# $HeadURL$

import commands
import os
import simplejson
import time
import urllib2

try:
  from hashlib import md5
except:
  from md5 import md5

# DIRAC
from DIRAC                       import S_OK, S_ERROR, gConfig, rootPath
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities        import List, Network

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.ServerUtils         import virtualMachineDB
from VMDIRAC.WorkloadManagementSystem.private.OutputDataExecutor import OutputDataExecutor

__RCSID__ = "$Id$"

class VirtualMachineMonitorAgent( AgentModule ):

  def getAmazonVMId( self ):
    try:
      fd = urllib2.urlopen( "http://instance-data.ec2.internal/latest/meta-data/instance-id" )
      iD = fd.read().strip()
      fd.close()
      return S_OK( iD )
    except Exception, e:
      return S_ERROR( "Could not retrieve amazon instance id: %s" % str( e ) )

  def getOcciVMId( self ):
    try:
      fd = open( os.path.join( '/etc', 'VMID' ), 'r' )
      iD = fd.read().strip()
      fd.close()
      return S_OK( iD )
    except Exception, e:
      return S_ERROR( "Could not retrieve occi instance id: %s" % str( e ) )

  def getNovaVMId( self ):
    
    metadataUrl = 'http://169.254.169.254/openstack/2012-08-10/meta_data.json'
    opener      = urllib2.build_opener()
        
    try:
      request  = urllib2.Request( metadataUrl )
      jsonFile = opener.open( request )
      jsonDict = simplejson.load( jsonFile )
 
      return S_OK( jsonDict[ 'meta' ][ 'vmdiracid' ] )
        
    except:
      pass  
    
    try:
      fd = open( os.path.join( '/etc', 'VMID' ), 'r' )
      iD = fd.read().strip()
      fd.close()
      return S_OK( iD )
    except Exception, e:
      return S_ERROR( "Could not retrieve nova instance id: %s" % str( e ) )

  def getCloudStackVMId( self ):
    try:
      iD    = commands.getstatusoutput( '/bin/hostname' )
      idStr = iD[1].split( '-' )
      return S_OK( idStr[2] )
    except Exception, e:
      return S_ERROR( "Could not retrieve CloudStack instance id: %s" % str( e ) )

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
    md5Hash = md5()
    md5Hash.update( btime )
    netData = Network.discoverInterfaces()
    for iface in sorted( netData ):
      if iface == "lo":
        continue
      md5Hash.update( netData[ iface ][ 'mac' ] )
    return S_OK( md5Hash.hexdigest() )

  def __getCSConfig( self ):
    if not self.vmName:
      return S_ERROR( "/LocalSite/VirtualMachineName is not defined" )
    #Variables coming from the vm 
    imgPath = "/Resources/VirtualMachines/Images/%s" % self.vmName
    for csOption, csDefault, varName in ( ( "MinWorkingLoad", 0.01, "vmMinWorkingLoad" ),
                                          ( "LoadAverageTimespan", 60, "vmLoadAvgTimespan" ),
                                          ( "JobWrappersLocation", "/tmp/", "vmJobWrappersLocation" ),
                                          ( "HaltPeriod", 600, "haltPeriod" ),
                                          ( "HaltBeforeMargin", 300, "haltBeforeMargin" ),
                                          ( "HeartBeatPeriod", 300, "heartBeatPeriod" ),
                                        ):

      path = "%s/%s" % ( imgPath, csOption )
      value = gConfig.getValue( path, csDefault )
      if not value:
        return S_ERROR( "%s is not defined" % path )
      setattr( self, varName, value )

    self.haltBeforeMargin = max( self.haltBeforeMargin, int( self.am_getPollingTime() ) + 5 )
    self.haltPeriod = max( self.haltPeriod, int( self.am_getPollingTime() ) + 5 )
    self.heartBeatPeriod = max( self.heartBeatPeriod, int( self.am_getPollingTime() ) + 5 )

    self.log.info( "** VM Info **" )
    self.log.info( "Name                  : %s" % self.vmName )
    self.log.info( "Min Working Load      : %f" % self.vmMinWorkingLoad )
    self.log.info( "Load Avg Timespan     : %d" % self.vmLoadAvgTimespan )
    self.log.info( "Job wrappers location : %s" % self.vmJobWrappersLocation )
    self.log.info( "Halt Period           : %d" % self.haltPeriod )
    self.log.info( "Halt Before Margin    : %d" % self.haltBeforeMargin )
    self.log.info( "HeartBeat Period      : %d" % self.heartBeatPeriod )
    if self.vmId:
      self.log.info( "ID                    : %s" % self.vmId )
    self.log.info( "*************" )
    return S_OK()

  def __declareInstanceRunning( self ):
    #Connect to VM monitor and register as running
    retries = 3
    sleepTime = 30
    for i in range( retries ):
      result = virtualMachineDB.declareInstanceRunning( self.vmId, self.ipAddress )
      if result[ 'OK' ]:
        self.log.info( "Declared instance running" )
        return result
      self.log.error( "Could not declare instance running", result[ 'Message' ] )
      if i < retries - 1 :
        self.log.info( "Sleeping for %d seconds and retrying" % sleepTime )
        time.sleep( sleepTime )
    return S_ERROR( "Could not declare instance running after %d retries" % retries )

  def initialize( self ):
    #Init vars
    self.vmName = ""
    self.__loadHistory = []
    self.__outDataExecutor = OutputDataExecutor()
    self.vmId = ""
    self.vmMinWorkingLoad = None
    self.vmLoadAvgTimespan = None
    self.vmJobWrappersLocation = None
    self.haltPeriod = None
    self.haltBeforeMargin = None
    self.heartBeatPeriod = None
    self.am_setOption( "MaxCycles", 0 )
    self.am_setOption( "PollingTime", 60 )
    self.cloudDriver = gConfig.getValue( "/LocalSite/CloudDriver", "" ).lower()
    self.log.info( "cloudDriver is %s" % self.cloudDriver )
    if self.cloudDriver == 'generic':
      result = self.getGenericVMId()
    elif self.cloudDriver == 'amazon':
      result = self.getAmazonVMId()
    elif (self.cloudDriver == 'occi-0.9' or self.cloudDriver == 'occi-0.8' or self.cloudDriver == 'rocci-1.1' ):
      result = self.getOcciVMId()
    elif self.cloudDriver == 'cloudstack':
      result = self.getCloudStackVMId()
    elif self.cloudDriver == 'nova-1.1':
      result = self.getNovaVMId()
    else:
      return S_ERROR( "Unknown cloudDriver %s" % self.cloudDriver )
    if not result[ 'OK' ]:
      return S_ERROR( "Could not generate VM id: %s" % result[ 'Message' ] )
    self.vmId = result[ 'Value' ]
    self.log.info( "VM ID is %s" % self.vmId )
    #Discover net address
    netData = Network.discoverInterfaces()
    for iface in sorted( netData ):
      if iface.find( "eth" ) == 0:
        self.ipAddress = netData[ iface ][ 'ip' ]
        break
    self.log.info( "IP Address is %s" % self.ipAddress )
    #getting the stoppage policy
    self.vmStopPolicy = gConfig.getValue( "/LocalSite/VMStopPolicy", "" ).lower()
    self.log.info( "vmStopPolicy is %s" % self.vmStopPolicy )
    #Declare instance running
    result = self.__declareInstanceRunning()
    if not result[ 'OK' ]:
      self.log.error( "Could not declare instance running", result[ 'Message' ] )
      self.__haltInstance()
      return S_ERROR( "Halting!" )
    self.__instanceInfo = result[ 'Value' ]
    self.vmName = self.__instanceInfo[ 'Image' ][ 'Name' ]
    self.log.info( "Image name is %s" % self.vmName )
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
    self.log.info( "Load averaged through %d seconds" % self.vmLoadAvgTimespan )
    self.log.info( " %d/%s required samples to average load" % ( len( self.__loadHistory ),
                                                                numRequiredSamples ) )
    avgLoad = 0
    for f in self.__loadHistory:
      avgLoad += f[0]
    return avgLoad / len( self.__loadHistory ), len( self.__loadHistory ) == numRequiredSamples

  def __getNumJobWrappers( self ):
    if not os.path.isdir( self.vmJobWrappersLocation ):
      return 0
    self.log.info( "VM job wrappers path: %s" % self.vmJobWrappersLocation )
    nJ = 0
    for entry in os.listdir( self.vmJobWrappersLocation ):
      entryPath = os.path.join( self.vmJobWrappersLocation, entry )
      if (entry.find( "jobAgent-" ) != -1):
        for jobAgentEntry in os.listdir(entryPath):
          if (jobAgentEntry.find( ".jdl" ) != -1):
            self.log.info( "VM job jdl %s found at: %s" % (jobAgentEntry, entryPath) )
            nJ += 1
    return nJ

  def execute( self ):
    #Process transfers
    self.__outDataExecutor.checkForTransfers()
    #Get load
    avgLoad, avgRequiredSamples = self.__getLoadAvg()
    self.log.info( "Load Average is %.2f" % avgLoad )
    if not avgRequiredSamples:
      self.log.info( " Not all required samples yet there" )
    #Do we need to send heartbeat?
    uptime = self.getUptime()
    hours = int( uptime / 3600 )
    minutes = int( uptime - hours * 3600 ) / 60
    seconds = uptime - hours * 3600 - minutes * 60
    self.log.info( "Uptime is %.2f (%d:%02d:%02d)" % ( uptime, hours, minutes, seconds ) )
    #Num jobs 
    numJobs = self.__getNumJobWrappers()
    self.log.info( "There are %d job wrappers" % numJobs )
    self.log.info( "Transferred %d files" % self.__outDataExecutor.getNumOKTransferredFiles() )
    self.log.info( "Transferred %d bytes" % self.__outDataExecutor.getNumOKTransferredBytes() )
    if uptime % self.heartBeatPeriod <= self.am_getPollingTime():
      #Heartbeat time!
      self.log.info( "Sending hearbeat..." )
      result = virtualMachineDB.instanceIDHeartBeat( self.vmId, avgLoad, numJobs,
                                                     self.__outDataExecutor.getNumOKTransferredFiles(),
                                                     self.__outDataExecutor.getNumOKTransferredBytes(),
                                                     int( uptime ) )
      if result[ 'OK' ]:
        self.log.info( " heartbeat sent!" )
      else:
        self.log.error( "Could not send heartbeat", result[ 'Message' ] )
      self.__processHeartBeatMessage( result[ 'Value' ] )
    #Check if there are local outgoing files
    localOutgoing = self.__outDataExecutor.getNumLocalOutgoingFiles()
    if localOutgoing or self.__outDataExecutor.transfersPending():
      self.log.info( "There are transfers pending. Not halting." )
      return S_OK()
    else:
      self.log.info( "No local outgoing files to be transferred" )
    #Do we need to check if halt?
    if avgRequiredSamples and uptime % self.haltPeriod + self.haltBeforeMargin > self.haltPeriod:
      self.log.info( "Load average is %s (minimum for working instance is %s)" % ( avgLoad,
                                                                                  self.vmMinWorkingLoad ) )
      #current stop polices: elastic (load) and never
      if self.vmStopPolicy == 'elastic':
        #If load less than X, then halt!
          if avgLoad < self.vmMinWorkingLoad:
            self.__haltInstance( avgLoad )
      if self.vmStopPolicy == 'never':
        self.log.info( "VM stoppage policy is defined to never (until SaaS or site request)")
    return S_OK()

  def __processHeartBeatMessage( self, hbMsg ):
    if 'stop' in hbMsg and hbMsg[ 'stop' ]:
      #Write stop file for jobAgent
      self.log.info( "Received STOP signal. Writing stop files..." )
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
          self.log.info( "Wrote stop file %s for agent %s" % ( stopFile, agentName ) )
        except Exception, e:
          self.log.error( "Could not write stop agent file", stopFile )
    if 'halt' in hbMsg and hbMsg[ 'halt' ]:
      self.__haltInstance()

  def __haltInstance( self, avgLoad = 0 ):
    self.log.info( "Halting instance..." )
    retries = 3
    sleepTime = 10
    for i in range( retries ):
      result = virtualMachineDB.declareInstanceHalting( self.vmId, avgLoad, self.cloudDriver )
      if result[ 'OK' ]:
        self.log.info( "Declared instance halting" )
        break
      self.log.error( "Could not send halting state", result[ 'Message' ] )
      if i < retries - 1 :
        self.log.info( "Sleeping for %d seconds and retrying" % sleepTime )
        time.sleep( sleepTime )

    self.log.info( "Executing system halt..." )
    os.system( "halt" )
