# $HeadURL$
""" VirtualMachineHandler provides remote access to VirtualMachineDB

    The following methods are available in the Service interface:
    
    - insertInstance
    - declareInstanceSubmitted
    - declareInstanceRunning
    - instanceIDHeartBeat
    - declareInstanceHalting
    - getInstancesByStatus
    - declareInstancesStopping

"""

from types import DictType, FloatType, IntType, ListType, LongType, StringType, TupleType, UnicodeType

# DIRAC
from DIRAC                                import gConfig, gLogger, S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler      import RequestHandler
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.NovaImage    import NovaImage
from VMDIRAC.WorkloadManagementSystem.Client.OcciImage    import OcciImage
from VMDIRAC.WorkloadManagementSystem.DB.VirtualMachineDB import VirtualMachineDB
from VMDIRAC.Security                                     import VmProperties

__RCSID__ = '$Id: $'

# This is a global instance of the VirtualMachineDB class
gVirtualMachineDB = False

def initializeVirtualMachineManagerHandler( _serviceInfo ):

  global gVirtualMachineDB
  
  gVirtualMachineDB = VirtualMachineDB()
  gVirtualMachineDB.declareStalledInstances()
  
  if gVirtualMachineDB._connected:
    gThreadScheduler.addPeriodicTask( 60 * 15, gVirtualMachineDB.declareStalledInstances )
    return S_OK()
  
  return S_ERROR()

class VirtualMachineManagerHandler( RequestHandler ):

  def initialize( self ):

     credDict = self.getRemoteCredentials()  
     self.rpcProperties       = credDict[ 'properties' ]

#     self.ownerDN              = credDict[ 'DN' ]
#     self.ownerGroup           = credDict[ 'group' ]
#     self.owner                = credDict[ 'username' ]
#     self.peerUsesLimitedProxy = credDict[ 'isLimitedProxy' ]
#    
#     self.diracSetup           = self.serviceInfoDict[ 'clientSetup' ]

  @staticmethod
  def __logResult( methodName, result ):
    '''
    Method that writes to log error messages 
    '''
    if not result[ 'OK' ]:
      gLogger.error( '%s%s' % ( methodName, result[ 'Message' ] ) )  

  types_checkVmWebOperation = [ StringType ]
  def export_checkVmWebOperation( self, operation ):
    """
    return true if rpc has VM_WEB_OPERATION
    """
    if VmProperties.VM_WEB_OPERATION in self.rpcProperties:
      return S_OK( 'Auth' )
    return S_OK( 'Unauth' )

  types_insertInstance = [ StringType, ( StringType, UnicodeType ), ]
  def export_insertInstance( self, imageName, instanceName, endpoint, runningPodName ):
    """
    Check Status of a given image
    Will insert a new Instance in the DB
    """    
    res = gVirtualMachineDB.insertInstance( imageName, instanceName, endpoint, runningPodName )
    self.__logResult( 'insertInstance', res )
    
    return res


  types_setInstanceUniqueID = [ LongType, ( StringType, UnicodeType ) ]
  def export_setInstanceUniqueID( self, instanceID, uniqueID ):
    """
    Check Status of a given image
    Will insert a new Instance in the DB
    """    
    res = gVirtualMachineDB.setInstanceUniqueID( instanceID, uniqueID )
    self.__logResult( 'setInstanceUniqueID', res )
    
    return res
  
  types_declareInstanceSubmitted = [ StringType ]
  def export_declareInstanceSubmitted( self, uniqueID ):
    """
    After submission of the instance the Director should declare the new Status
    """
    res = gVirtualMachineDB.declareInstanceSubmitted( uniqueID )
    self.__logResult( 'declareInstanceSubmitted', res )
    
    return res


  types_declareInstanceRunning = [ StringType, StringType ]
  def export_declareInstanceRunning( self, uniqueID, privateIP ):
    """
    Declares an instance Running and sets its associated info (uniqueID, publicIP, privateIP)
    Returns S_ERROR if:
      - instanceName does not have a "Submitted" entry 
      - uniqueID is not unique
    """
    if not VmProperties.VM_RPC_OPERATION in self.rpcProperties:
      return S_ERROR( "Unauthorized declareInstanceRunning RPC" )

    publicIP = self.getRemoteAddress()[ 0 ]
    
    res = gVirtualMachineDB.declareInstanceRunning( uniqueID, publicIP, privateIP )
    self.__logResult( 'declareInstanceRunning', res )
    
    return res


  types_instanceIDHeartBeat = [ StringType, FloatType, ( IntType, LongType ),
                               ( IntType, LongType ), ( IntType, LongType ) ]
  def export_instanceIDHeartBeat( self, uniqueID, load, jobs,
                                  transferredFiles, transferredBytes, uptime = 0 ):
    """
    Insert the heart beat info from a running instance
    It checks the status of the instance and the corresponding image
    Declares "Running" the instance and the image 
    It returns S_ERROR if the status is not OK
    """
    if not VmProperties.VM_RPC_OPERATION in self.rpcProperties:
      return S_ERROR( "Unauthorized declareInstanceIDHeartBeat RPC" )

    #FIXME: do we really need the try / except. The type is fixed to int / long.
    try:
      uptime = int( uptime )
    except ValueError:
      uptime = 0
      
    res = gVirtualMachineDB.instanceIDHeartBeat( uniqueID, load, jobs,
                                                 transferredFiles, transferredBytes, uptime )
    self.__logResult( 'instanceIDHeartBeat', res )
    
    return res
    
  types_declareInstancesStopping = [ ListType ]
  def export_declareInstancesStopping( self, instanceIdList ):
    """
    Declares "Stoppig" the instance because the Delete button of Browse Instances
    The instanceID is the VMDIRAC VM id
    When next instanceID heat beat with stoppig status on the DB the VM will stop the job agent and terminates ordenery
    It returns S_ERROR if the status is not OK
    """
    if not VmProperties.VM_WEB_OPERATION in self.rpcProperties:
      return S_ERROR( "Unauthorized VM Stopping" )

    for instanceID in instanceIdList:
      gLogger.info( 'Stopping DIRAC instanceID: %s' % ( instanceID ) )  
      result = gVirtualMachineDB.getInstanceStatus( instanceID )
      if not result[ 'OK' ]:
        self.__logResult( 'declareInstancesStopping on getInstanceStatus call: ', result )
        return result
      state = result[ 'Value' ]
      gLogger.info( 'Stopping DIRAC instanceID: %s, current state %s' % ( instanceID, state ) )  

      if state == 'Stalled': 
        result = gVirtualMachineDB.getUniqueID( instanceID )
        if not result[ 'OK' ]:
          self.__logResult( 'declareInstancesStopping on getUniqueID call: ', result )
          return result
        uniqueID = result [ 'Value' ]
        result = gVirtualMachineDB.getEndpointFromInstance( uniqueID )
        if not result[ 'OK' ]:
          self.__logResult( 'declareInstancesStopping on getEndpointFromInstance call: ', result )
          return result
        endpoint = result [ 'Value' ]
        cloudDriver = gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( endpoint, "cloudDriver" ) )
        if not cloudDriver:
          msg = 'Cloud not found driver option in the Endpoint %s value %s' % (endpoint, cloudDriver)
          return S_ERROR( msg )
        result = self.export_declareInstanceHalting( uniqueID, 0, cloudDriver )
      elif state == 'New': 
        result = gVirtualMachineDB.recordDBHalt( instanceID, 0 )
        self.__logResult( 'declareInstanceHalted', result )
      else:
        # this is only aplied to allowed trasitions 
        result = gVirtualMachineDB.declareInstanceStopping( instanceID )
        self.__logResult( 'declareInstancesStopping: on declareInstanceStopping call: ', result )

    return result

  types_declareInstanceHalting = [ StringType, FloatType ]
  def export_declareInstanceHalting( self, uniqueID, load, cloudDriver ):
    """
    Insert the heart beat info from a halting instance
    The VM has the uniqueID, which is the Cloud manager VM id
    Declares "Halted" the instance and the image 
    It returns S_ERROR if the status is not OK
    """
    if not VmProperties.VM_RPC_OPERATION in self.rpcProperties:
      return S_ERROR( "Unauthorized declareInstanceHalting RPC" )

    endpoint = gVirtualMachineDB.getEndpointFromInstance( uniqueID )
    if not endpoint[ 'OK' ]:
      self.__logResult( 'declareInstanceHalting', endpoint )
      return endpoint
    endpoint = endpoint[ 'Value' ]

    result = gVirtualMachineDB.declareInstanceHalting( uniqueID, load )
    if not result[ 'OK' ]:
      self.__logResult( 'declareInstanceHalting on change status: ', result )
      return result
   
    if ( cloudDriver == 'occi-0.9' or cloudDriver == 'occi-0.8' or cloudDriver == 'rocci-1.1' ):
      imageName = gVirtualMachineDB.getImageNameFromInstance( uniqueID )
      if not imageName[ 'OK' ]:
        self.__logResult( 'declareInstanceHalting getImageNameFromInstance: ', imageName )
        return imageName
      imageName = imageName[ 'Value' ]

      gLogger.info( 'Declare instance haltig:  %s, endpoint: %s imageName: %s' % (str(uniqueID),endpoint,imageName) )
      oima   = OcciImage( imageName, endpoint )
      connOcci = oima.connectOcci()
      if not connOcci[ 'OK' ]:
        return connOcci

      result = oima.stopInstance( uniqueID )

    elif cloudDriver == 'nova-1.1':
      imageName = gVirtualMachineDB.getImageNameFromInstance( uniqueID )
      if not imageName[ 'OK' ]:
        self.__logResult( 'declareInstanceHalting getImageNameFromInstance: ', imageName )
        return imageName
      imageName = imageName[ 'Value' ]

      nima     = NovaImage( imageName, endpoint )
      connNova = nima.connectNova()
      if not connNova[ 'OK' ]:
        return connNova
      
      publicIP = gVirtualMachineDB.getPublicIpFromInstance ( uniqueID )
      if not publicIP[ 'OK' ]:
        self.__logResult( 'declareInstanceHalting getPublicIpFromInstance: ', publicIP )
        return publicIP
      publicIP = publicIP[ 'Value' ]
      
      result = nima.stopInstance( uniqueID, publicIP )

    else:
      gLogger.warn( 'Unexpected cloud driver:  %s' % cloudDriver )

    self.__logResult( 'declareInstanceHalting: ', result )
    return result

  types_getInstancesByStatus = [ StringType ]
  def export_getInstancesByStatus( self, status ):
    """
    Get dictionary of Image Names with InstanceIDs in given status 
    """
    
    res = gVirtualMachineDB.getInstancesByStatus( status )
    self.__logResult( 'getInstancesByStatus', res )
    return res


  types_getAllInfoForUniqueID = [ StringType ]
  def export_getAllInfoForUniqueID( self, uniqueID ):
    """
    Get all the info for a UniqueID
    """
    res = gVirtualMachineDB.getAllInfoForUniqueID( uniqueID )
    self.__logResult( 'getAllInfoForUniqueID', res )
    
    return res


  types_getInstancesContent = [ DictType, ( ListType, TupleType ),
                                ( IntType, LongType ), ( IntType, LongType ) ]
  def export_getInstancesContent( self, selDict, sortDict, start, limit ):
    """
    Retrieve the contents of the DB
    """
    res = gVirtualMachineDB.getInstancesContent( selDict, sortDict, start, limit )
    self.__logResult( 'getInstancesContent', res )
    
    return res


  types_getHistoryForInstanceID = [ ( IntType, LongType ) ]
  def export_getHistoryForInstanceID( self, instanceId ):
    """
    Retrieve the contents of the DB
    """
    res = gVirtualMachineDB.getHistoryForInstanceID( instanceId )
    self.__logResult( 'getHistoryForInstanceID', res )
    
    return res


  types_getInstanceCounters = []
  def export_getInstanceCounters( self ):
    """
    Retrieve the contents of the DB
    """
    res = gVirtualMachineDB.getInstanceCounters()
    self.__logResult( 'getInstanceCounters', res )
    
    return res

  
  types_getHistoryValues = [ IntType, DictType  ]
  def export_getHistoryValues( self, averageBucket, selDict, fields2Get = [], timespan = 0 ):
    """
    Retrieve the contents of the DB
    """
    res = gVirtualMachineDB.getHistoryValues( averageBucket, selDict, fields2Get, timespan )
    self.__logResult( 'getHistoryValues', res )
    
    return res


  types_getRunningInstancesHistory = [ IntType, IntType ]
  def export_getRunningInstancesHistory( self, timespan, bucketSize ):
    """
    Retrieve number of running instances in each bucket
    """
    res = gVirtualMachineDB.getRunningInstancesHistory( timespan, bucketSize )
    self.__logResult( 'getRunningInstancesHistory', res )
    
    return res


  types_getRunningInstancesBEPHistory = [ IntType, IntType ]
  def export_getRunningInstancesBEPHistory( self, timespan, bucketSize ):
    """
    Retrieve number of running instances in each bucket
    """
    res = gVirtualMachineDB.getRunningInstancesBEPHistory( timespan, bucketSize )
    self.__logResult( 'getRunningInstancesBEPHistory', res )
    
    return res

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
