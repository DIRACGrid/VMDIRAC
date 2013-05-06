# $HeadURL$
""" VirtualMachineHandler provides remote access to VirtualMachineDB

    The following methods are available in the Service interface:
    
    - insertInstance
    - declareInstanceSubmitted
    - declareInstanceRunning
    - instanceIDHeartBeat
    - declareInstanceHalting
    - getInstancesByStatus
    - declareInstanceStopping

"""

from types import DictType, FloatType, IntType, ListType, LongType, StringType, TupleType, UnicodeType

# DIRAC
from DIRAC                                import gLogger, S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler      import RequestHandler
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.NovaImage    import NovaImage
from VMDIRAC.WorkloadManagementSystem.Client.OcciImage    import OcciImage
from VMDIRAC.WorkloadManagementSystem.DB.VirtualMachineDB import VirtualMachineDB

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

# FIXME: is all this actually used ????       
#    credDict = self.getRemoteCredentials()  
#    self.ownerDN              = credDict[ 'DN' ]
#    self.ownerGroup           = credDict[ 'group' ]
#    self.userProperties       = credDict[ 'properties' ]
#    self.owner                = credDict[ 'username' ]
#    self.peerUsesLimitedProxy = credDict[ 'isLimitedProxy' ]
#    
#    self.diracSetup           = self.serviceInfoDict[ 'clientSetup' ]
    pass

  @staticmethod
  def __logResult( methodName, result ):
    '''
    Method that writes to log error messages 
    '''
    if not result[ 'OK' ]:
      gLogger.error( '%s%s' % ( methodName, result[ 'Message' ] ) )  


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
    #FIXME: do we really need the try / except. The type is fixed to int / long.
    try:
      uptime = int( uptime )
    except ValueError:
      uptime = 0
      
    res = gVirtualMachineDB.instanceIDHeartBeat( uniqueID, load, jobs,
                                                 transferredFiles, transferredBytes, uptime )
    self.__logResult( 'instanceIDHeartBeat', res )
    
    return res
    
  types_declareInstanceStopping = [ StringType, FloatType ]
  def export_declareInstanceStopping( self, instanceID ):
    """
    Declares "Stoppig" the instance because the Delete button of Browse Instances
    The instanceID is the VMDIRAC VM id
    When next instanceID heat beat with stoppig status on the DB the VM will stop the job agent and terminates ordenery
    It returns S_ERROR if the status is not OK
    """
    result = gVirtualMachineDB.declareInstanceStopping( instanceID )
    if not result[ 'OK' ]:
      return S_ERROR()
    self.__logResult( 'declareInstanceStopping', result )
    return result

  types_declareInstanceHalting = [ StringType, FloatType ]
  def export_declareInstanceHalting( self, uniqueID, load, cloudDriver ):
    """
    Insert the heart beat info from a halting instance
    The VM has the uniqueID, which is the Cloud manager VM id
    Declares "Halted" the instance and the image 
    It returns S_ERROR if the status is not OK
    """
    endpoint = gVirtualMachineDB.getEndpointFromInstance( uniqueID )
    if not endpoint[ 'OK' ]:
      self.__logResult( 'declareInstanceHalting', endpoint )
      return endpoint
    endpoint = endpoint[ 'Value' ]

    result = gVirtualMachineDB.declareInstanceHalting( uniqueID, load )
    if not result[ 'OK' ]:
      self.__logResult( 'declareInstanceHalting', result )
      return result
    
    if ( cloudDriver == 'occi-0.9' or cloudDriver == 'occi-0.8' ):
      imageName = gVirtualMachineDB.getImageNameFromInstance( uniqueID )
      if not imageName[ 'OK' ]:
        self.__logResult( 'declareInstanceHalting', imageName )
        return imageName
      imageName = imageName[ 'Value' ]

      oima   = OcciImage( imageName, endpoint )
      result = oima.stopInstance( uniqueID )

    elif cloudDriver == 'nova-1.1':
      imageName = gVirtualMachineDB.getImageNameFromInstance( uniqueID )
      if not imageName[ 'OK' ]:
        self.__logResult( 'declareInstanceHalting', imageName )
        return imageName
      imageName = imageName[ 'Value' ]

      nima = NovaImage( imageName, endpoint )
      
      publicIP = gVirtualMachineDB.getPublicIpFromInstance ( uniqueID )
      if not publicIP[ 'OK' ]:
        self.__logResult( 'declareInstanceHalting', publicIP )
        return publicIP
      publicIP = publicIP[ 'Value' ]
      
      result = nima.stopInstance( uniqueID, publicIP )

    else:
      gLogger.warn( 'Unexpected cloud driver is %s' % cloudDriver )

    self.__logResult( 'declareInstanceHalting', result )
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
