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
from BelleDIRAC.WorkloadManagementSystem.DB.VirtualMachineDB               import VirtualMachineDB

from types import *

# This is a global instance of the VirtualMachineDB class
gVirtualMachineDB = False

def initializeVirtualMachineManagerHandler( serviceInfo ):

  global gVirtualMachineDB
  gVirtualMachineDB = VirtualMachineDB()
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
  types_insertInstance = [ StringType, StringType ]
  def export_insertInstance( self, imageName, instanceName ):
    """
    Check Status of a given image
    Will insert a new Instance in the DB
    """
    return gVirtualMachineDB.insertInstance( imageName, instanceName )

  ###########################################################################
  types_declareInstanceSubmitted = [ LongType ]
  def export_declareInstanceSubmitted( self, instanceID ):
    """
    After submission of the instance the Director should declare the new Status
    """
    return gVirtualMachineDB.declareInstanceSubmitted( instanceID )

  ###########################################################################
  types_declareInstanceRunning = [ StringType, StringType, StringType ]
  def export_declareInstanceRunning( self, imageName, uniqueID, privateIP ):
    """
    Declares an instance Running and sets its associated info (uniqueID, publicIP, privateIP)
    Returns S_ERROR if:
      - instanceName does not have a "Submitted" entry 
      - uniqueID is not unique
    """
    publicIP = self.getRemoteAddress()[0]
    return gVirtualMachineDB.declareInstanceRunning( imageName, uniqueID, publicIP, privateIP )

  ###########################################################################
  types_instanceIDHeartBeat = [ StringType, FloatType ]
  def export_instanceIDHeartBeat( self, uniqueID, load ):
    """
    Insert the heart beat info from a running instance
    It checks the status of the instance and the corresponding image
    Declares "Running" the instance and the image 
    It returns S_ERROR if the status is not OK
    """
    return gVirtualMachineDB.instanceIDHeartBeat( uniqueID, load )

  ###########################################################################
  types_declareInstanceHalting = [ StringType, FloatType ]
  def export_declareInstanceHalting( self, uniqueID, load ):
    """
    Insert the heart beat info from a halting instance
    Declares "Halted" the instance and the image 
    It returns S_ERROR if the status is not OK
    """
    return gVirtualMachineDB.declareInstanceHalting( uniqueID, load )

  ###########################################################################
  types_getInstancesByStatus = [ StringType ]
  def export_getInstancesByStatus( self, status ):
    """
    Get dictionary of Image Names with InstanceIDs in given status 
    """
    return gVirtualMachineDB.getInstancesByStatus( status )

  ###########################################################################
  types_getInstancesContent = [ DictType, ( ListType, TupleType ),
                                ( IntType, LongType ), ( IntType, LongType ) ]
  def export_getInstancesContent( self, selDict, sortDict, start, limit ):
    """
    Retrieve the contents of the DB
    """
    return gVirtualMachineDB.getInstancesContent( selDict, sortDict, start, limit )

