########################################################################
# $HeadURL$
########################################################################

""" VirtualMachineHandler provides remote access to VirtualMachineDB

    The following methods are available in the Service interface:
    
    - insertInstance
    - declareInstanceSubmitted
    - declareInstanceRunning
    - instanceIDHeartBeat
    - declareInstanceHalting
    - getInstancesByStatus

"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.ThreadScheduler              import gThreadScheduler
from VMDIRAC.WorkloadManagementSystem.DB.VirtualMachineDB               import VirtualMachineDB
from VMDIRAC.WorkloadManagementSystem.Client.OcciImage import OcciImage

from types import *

# This is a global instance of the VirtualMachineDB class
gVirtualMachineDB = False

def initializeVirtualMachineManagerHandler( serviceInfo ):

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
    self.ownerDN = credDict[ 'DN' ]
    self.ownerGroup = credDict[ 'group' ]
    self.userProperties = credDict[ 'properties' ]
    self.owner = credDict[ 'username' ]
    self.peerUsesLimitedProxy = credDict[ 'isLimitedProxy' ]
    self.diracSetup = self.serviceInfoDict[ 'clientSetup' ]


  ###########################################################################
  types_insertInstance = [ StringType, ( StringType, UnicodeType ), ]
  def export_insertInstance( self, imageName, instanceName, endpoint, runningPodName ):
    """
    Check Status of a given image
    Will insert a new Instance in the DB
    """
    return gVirtualMachineDB.insertInstance( imageName, instanceName, endpoint, runningPodName)

  ###########################################################################
  types_setInstanceUniqueID = [ LongType, ( StringType, UnicodeType ) ]
  def export_setInstanceUniqueID( self, instanceID, uniqueID ):
    """
    Check Status of a given image
    Will insert a new Instance in the DB
    """
    return gVirtualMachineDB.setInstanceUniqueID( instanceID, uniqueID )

  ###########################################################################
  types_declareInstanceSubmitted = [ StringType ]
  def export_declareInstanceSubmitted( self, uniqueID ):
    """
    After submission of the instance the Director should declare the new Status
    """
    return gVirtualMachineDB.declareInstanceSubmitted( uniqueID )

  ###########################################################################
  types_declareInstanceRunning = [ StringType, StringType ]
  def export_declareInstanceRunning( self, uniqueID, privateIP ):
    """
    Declares an instance Running and sets its associated info (uniqueID, publicIP, privateIP)
    Returns S_ERROR if:
      - instanceName does not have a "Submitted" entry 
      - uniqueID is not unique
    """
    publicIP = self.getRemoteAddress()[0]
    return gVirtualMachineDB.declareInstanceRunning( uniqueID, publicIP, privateIP )

  ###########################################################################
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
    try:
      uptime = int( uptime )
    except:
      uptime = 0
    return gVirtualMachineDB.instanceIDHeartBeat( uniqueID, load, jobs,
                                                  transferredFiles, transferredBytes, uptime )

  ###########################################################################
  types_declareInstanceHalting = [ StringType, FloatType ]
  def export_declareInstanceHalting( self, vmId, load, contextualization ):
    """
    Insert the heart beat info from a halting instance
    Declares "Halted" the instance and the image 
    It returns S_ERROR if the status is not OK
    """
    result = gVirtualMachineDB.getEndpointFromInstance( vmId )
    if not result[ 'OK' ]:
      return result
    endpoint = result[ 'Value' ]

    result = gVirtualMachineDB.declareInstanceHalting( vmId, load)
    if not contextualization =='occi':
      return result
    if not result[ 'OK' ]:
      return result

    result = gVirtualMachineDB.getImageNameFromInstance( vmId )
    if not result[ 'OK' ]:
      return result
    imageName = result[ 'Value' ]

    oima = OcciImage( imageName, endpoint )
    result = oima.stopInstance( vmId )

    return result

  ###########################################################################
  types_getInstancesByStatus = [ StringType ]
  def export_getInstancesByStatus( self, status ):
    """
    Get dictionary of Image Names with InstanceIDs in given status 
    """
    return gVirtualMachineDB.getInstancesByStatus( status )

  ###########################################################################
  types_getAllInfoForUniqueID = [ StringType ]
  def export_getAllInfoForUniqueID( self, uniqueID ):
    """
    Get all the info for a UniqueID
    """
    return gVirtualMachineDB.getAllInfoForUniqueID( uniqueID )

  ###########################################################################
  types_getInstancesContent = [ DictType, ( ListType, TupleType ),
                                ( IntType, LongType ), ( IntType, LongType ) ]
  def export_getInstancesContent( self, selDict, sortDict, start, limit ):
    """
    Retrieve the contents of the DB
    """
    return gVirtualMachineDB.getInstancesContent( selDict, sortDict, start, limit )

  ###########################################################################
  types_getHistoryForInstanceID = [ ( IntType, LongType ) ]
  def export_getHistoryForInstanceID( self, instanceId ):
    """
    Retrieve the contents of the DB
    """
    return gVirtualMachineDB.getHistoryForInstanceID( instanceId )

  ###########################################################################
  types_getInstanceCounters = []
  def export_getInstanceCounters( self ):
    """
    Retrieve the contents of the DB
    """
    return gVirtualMachineDB.getInstanceCounters()

  ###########################################################################
  types_getHistoryValues = [ IntType, DictType  ]
  def export_getHistoryValues( self, averageBucket, selDict, fields2Get = [], timespan = 0 ):
    """
    Retrieve the contents of the DB
    """
    return gVirtualMachineDB.getHistoryValues( averageBucket, selDict, fields2Get, timespan )

  ###########################################################################
  types_getRunningInstancesHistory = [ IntType, IntType ]
  def export_getRunningInstancesHistory( self, timespan, bucketSize ):
    """
    Retrieve number of running instances in each bucket
    """
    return gVirtualMachineDB.getRunningInstancesHistory( timespan, bucketSize )
