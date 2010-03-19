########################################################################
# $HeadURL$
# File :   VirtualMachineDB.py
# Author : Ricardo Graciani
########################################################################
""" VirtualMachineDB class is a front-end to the virtual machines DB

  Life cycle of VMs Images in DB
  - New:     Inserted by Director (Name - Flavor - Requirements - Status = New ) if not existing when launching a new instance
  - Running: Declared by VMMonitoring Server when an Instance reports back correctly
  - Ready:   Declared by VMMonitoring Server when detects no more running instances
  - Error:   Declared by VMMonitoring Server when an Instance reports back wrong requirements

  Life cycle of VMs Instances in DB
  - New:       Inserted by Director before launching a new instance, to check if image is valid
  - Submitted: Inserted by Director (adding UniqueID) when launches a new instance
  - Running:   Declared by VMMonitoring Server when an Instance reports back correctly (add LastUpdate, publicIP and privateIP)
  - Error:     Declared by VMMonitoring Server when an Instance reports back wrong requirements
  - Stopped:   Declared by VMMonitoring Server when an Instance reports halting
  - Halted:    Declared by VMMonitoring Server when confirms the halt (gets no more heart beats)
  - Stalled:   Declared by VMMonitoring Server when detects Instance no more running

  New Instances can be launched by Director if VMImage is not in Error Status.

  Instance UniqueID: depends on Flavor, for KVM it could be the MAC, for Amazon the returned InstanceID(i-5dec3236)

"""

__RCSID__ = "$Id$"

import types
from DIRAC.Core.Base.DB import DB
import DIRAC

class VirtualMachineDB( DB ):

  # When checking the Status on the DB it must be one of these values, if not, the last one (Error) is set
  # When declaring a new Status, it will be set to Error if not in the list
  validImageStates = [ 'New', 'Running', 'Ready', 'Error' ]
  validInstanceStates = [ 'New', 'Submitted', 'Running', 'Stopped', 'Halted', 'Stalled', 'Error' ]

  # When attempting a transition it will be checked if the current state is allowed 
  allowedTransitions = { 'Image' : {
                                       'Running' : [ 'New', 'Running', 'Ready' ],
                                   },
                        'Instance' : {
                                       'Submitted' : [ 'New' ],
                                       'Running' : [ 'Submitted', 'Running' ],
                                      }
                       }

  tablesDesc = {}

  tablesDesc[ 'vm_Images' ] = { 'Fields' : { 'VMImageID' : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                             'Name' : 'VARCHAR(255) NOT NULL',
                                             'Flavor' : 'VARCHAR(32) NOT NULL',
                                             'Requirements' : 'VARCHAR(1024) NOT NULL',
                                             'Status' : 'VARCHAR(16) NOT NULL',
                                             'LastUpdate' : 'DATETIME',
                                             'ErrorMessage' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                           },
                               'PrimaryKey' : 'VMImageID',
                               'Indexes': { 'Image': [ 'Name', 'Flavor' ]
                                          }
                             }

  tablesDesc[ 'vm_Instances' ] = { 'Fields' : { 'VMInstanceID' : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                'Name' : 'VARCHAR(255) NOT NULL',
                                                'UniqueID' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                                'VMImageID' : 'INTEGER UNSIGNED NOT NULL',
                                                'Status' : 'VARCHAR(32) NOT NULL',
                                                'LastUpdate' : 'DATETIME',
                                                'PublicIP' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                                'PrivateIP' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                                'ErrorMessage' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                             },
                                   'PrimaryKey' : 'VMInstanceID',
                                   'Indexes': { 'Status': [ 'Status' ] },
                                 }

  tablesDesc[ 'vm_History' ] = { 'Fields' : { 'VMInstanceID' : 'INTEGER UNSIGNED NOT NULL',
                                              'Status' : 'VARCHAR(32) NOT NULL',
                                              'Load' : 'FLOAT NOT NULL',
                                              'Update' : 'DATETIME'
                                            },
                                 'Indexes': { 'VMInstanceID': [ 'VMInstanceID' ] },
                               }

  def __init__( self, maxQueueSize = 10 ):
    DB.__init__( self, 'VirtualMachineDB', 'WorkloadManagement/VirtualMachineDB', maxQueueSize )
    if not self._MySQL__initialized:
      raise Exception( 'Can not connect to VirtualMachineDB, exiting...' )

    result = self.__initializeDB()
    if not result[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % result[ 'Message' ] )

  def __initializeDB( self ):
    """
    Create the tables
    """
    result = self._query( "show tables" )
    if not result[ 'OK' ]:
      return result

    tablesInDB = [ t[0] for t in result[ 'Value' ] ]
    tablesToCreate = {}
    for tableName in self.tablesDesc:
      if not tableName in tablesInDB:
        tablesToCreate[ tableName ] = self.tablesDesc[ tableName ]

    return self._createTables( tablesToCreate )

  def __getTypeTuple( self, object ):
    """
    return tuple of (tableName, validStates, idName) for object
    """
    tableName = ''
    validStates = []
    idName = ''
    if object == 'Image':
      tableName = 'vm_Images'
      validStates = self.validImageStates
      idName = 'VMImageID'
    elif object == 'Instance':
      tableName = 'vm_Instances'
      validStates = self.validInstanceStates
      idName = 'VMInstanceID'

    return ( tableName, validStates, idName )

  def checkImageStatus( self, imageName, flavor, requirements ):
    """ 
    Check Status of a given image
    Will insert a new Image in the DB if it does not exits
    returns:
      S_OK(Status) if Status is valid and not Error 
      S_ERROR(ErrorMessage) otherwise
    """
    ret = self.__getImageID( imageName, flavor, requirements )
    if not ret['OK']:
      return ret
    return self.__getStatus( 'Image', ret['Value'] )

  def insertInstance( self, imageName, flavor, requirements, instanceName ):
    """ 
    Check Status of a given image
    Will insert a new Instance in the DB
    returns:
      S_OK( InstanceID ) if new Instance is properly inserted 
      S_ERROR(ErrorMessage) otherwise
    """
    imageStatus = self.checkImageStatus( imageName, flavor, requirements )
    if not imageStatus['OK']:
      return imageStatus

    return self.__insertInstance( imageName, flavor, requirements, instanceName )

  def declareInstanceSubmitted( self, instanceID ):
    """
    After submission of the instance the Director should declare the new Status
    """
    status = self.__setState( 'Instance', instanceID, 'Submitted' )
    if status['OK']:
      self.__addInstanceHistory( instanceID, 'Submitted' )

    return status

  def declareInstanceRunning( self, imageName, uniqueID, publicIP, privateIP ):
    """
    Declares an instance Running and sets its associated info (uniqueID, publicIP, privateIP)
    Returns S_ERROR if:
      - instanceName does not have a "Submitted" entry 
      - uniqueID is not unique
    """
    instanceID = self.__getInstanceID( uniqueID )
    if instanceID['OK']:
      return DIRAC.S_ERROR( 'UniqueID is not unique: %s' % uniqueID )

    instanceID = self.__getSubmittedInstanceID( imageName )
    if not instanceID['OK']:
      return instanceID
    instanceID = instanceID['Value']

    status = self.__setState( 'Instance', instanceID, 'Running' )
    if status['OK']:
      self.__addInstanceHistory( instanceID, 'Running' )

    return status

  def instanceIDHeartBeat( self, uniqueID, load ):
    """
    Insert the heart beat info from a running instance
    It checks the status of the instance and the corresponding image
    Declares "Running" the instance and the image 
    It returns S_ERROR if the status is not OK
    """
    instanceID = self.__getInstanceID( uniqueID )
    if not instanceID['OK']:
      return instanceID
    instanceID = instanceID['Value']

    status = self.__runningInstance( instanceID, load )
    if not status['OK']:
      return status

    return

  def __insertInstance( self, imageName, flavor, requirements, instanceName ):
    """
    Attempts to insert a new Instance for the given Image
    """
    image = self.__getImageID( imageName, flavor, requirements )
    if not image['OK']:
      return image
    imageID = image['Value']

    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Instance' )

    instance = self._insert( tableName , ['Name', 'VMImageID', 'Status', 'LastUpdate' ],
                                         [instanceName, imageID, validStates[0], DIRAC.Time.toString() ] )

    if instance['OK'] and 'lastRowId' in instance:
      self.__addInstanceHistory( instance['lastRowId'], validStates[0] )
      return DIRAC.S_OK( instance['lastRowId'] )

    if not instance['OK']:
      return instance

    return DIRAC.S_ERROR( 'Failed to insert new Instance' )

  def __runningInstance( self, instanceID, load ):
    """
    Checks image status, set it to running and set instance status to running
    """
    # Check the Image is OK
    imageID = self.__getImageForRunningInstance( instanceID )
    if not imageID['OK']:
      self.__setError( 'Instance', instanceID, imageID['Message'] )
      return imageID
    imageID = imageID['Value']

    # Update Instance to Running
    stateInstance = self.__setState( 'Instance', instanceID, 'Running' )
    if not stateInstance['OK']:
      return stateInstance

    # Update Image to Running
    stateImage = self.__setState( 'Image', imageID, 'Running' )
    if not stateImage['OK']:
      self.__setError( 'Instance', instanceID, stateImage['Message'] )
      return stateImage

    # Add History record
    self.__addInstanceHistory( instanceID, 'Running', load )
    return DIRAC.S_OK()

  def __getImageForRunningInstance( self, instanceID ):
    """
    Looks for imageID for a given instanceID. 
    Check image Transition to Running is allowed
    Returns:
      S_OK( imageID )
      S_ERROR( Reason ) 
    """
    info = self.__getInfo( 'Instance', id )
    if not info['OK']:
      return info
    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Image' )

    imageID = info[ idName ]

    imageStatus = self.__getStatus( 'Image', imageID )
    if not imageStatus['OK']:
      return imageStatus

    return DIRAC.S_OK( imageID )

  def __getSubmittedInstanceID( self, imageName ):
    """
    Retrieve and InstanceID associated to a submitted Instance for a given Image
    """
    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Image' )
    imageID = self._getFields( tableName, [ idName ], ['Name'], [imageName] )
    if not imageID['OK']:
      return imageID

    if not imageID['Value']:
      return DIRAC.S_ERROR( 'Unknown Image = %s' % imageName )

    imageIDs = imageID['Value'][0]

    if len( imageIDs ) <> 1:
      return DIRAC.S_ERROR( 'Image name "%s" is not unique' % imageName )

    imageID = imageIDs[0]
    imageIDName = idName

    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Instance' )

    instanceID = self._getFields( tableName, [idName], [ imageIDName, 'Status' ], [ imageID, 'Submitted' ] )
    if not instanceID['OK']:
      return instanceID

    if not instanceID['Value']:
      return DIRAC.S_ERROR( 'No Submitted instance of "%s" found' % imageName )

    return DIRAC.S_OK( instanceID['Value'][0][0] )

  def __setState( self, object, id, state ):
    """
    Attempt to set object in state, checking if transition is allowed
    """
    knownStates = self.allowedTransitions[object].keys()
    if not state in knownStates:
      return DIRAC.S_ERROR( 'Transition to %s not possible' % state )

    allowedStates = self.allowedTransitions[object][state]

    currentState = self.__getStatus( object, id )
    if not currentState['OK']:
      return currentState

    currentState = currentState['Value']
    if currentState == state:
      return DIRAC.S_OK( state )

    if not currentState in allowedStates:
      msg = 'Transition (%s -> %s) not allowed' % ( currentState, state )
      return DIRAC.S_ERROR( msg )

    ( tableName, validStates, idName ) = self.__getTypeTuple( object )

    cmd = 'UPDATE `%s` SET Status="%s",LastUpdate=UTC_TIMESTAMP() WHERE %s = %s' % \
        ( tableName, state, idName, id )

    ret = self._update( cmd )
    if not ret['OK']:
      return ret

    return DIRAC.S_OK( state )

  def __getInstanceID( self, uniqueID ):
    """
    For a given uniqueID of an instance return associated internal InstanceID 
    """
    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Instance' )
    instanceID = self._getFields( tableName, [ idName ], [ 'UniqueID' ], [ uniqueID ] )
    if not instanceID['OK']:
      return instanceID

    if not instanceID['Value']:
      return DIRAC.S_ERROR( 'Unknown %s = %s' % ( 'UniqueID', uniqueID ) )

    return DIRAC.S_OK( instanceID['Value'][0][0] )

  def __getImageID( self, imageName, flavor, requirements ):
    """
    For a given imageName, flavor and requirements return corresponding ID
    Will insert the image in New Status if it does not exits
    """
    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Image' )
    ret = self._getFields( tableName, [idName], ['Name', 'Flavor', 'Requirements'],
                                                [imageName, flavor, requirements] )
    if not ret['OK']:
      return ret
    if ret['Value']:
      return DIRAC.S_OK( ret['Value'][0][0] )
    ret = self._insert( tableName, ['Name', 'Flavor', 'Requirements', 'Status', 'LastUpdate'],
                                     [imageName, flavor, requirements, validStates[0], DIRAC.Time.toString()] )
    if ret['OK'] and 'lastRowId' in ret:
      id = ret['lastRowId']
      ret = self._getFields( tableName, [idName], ['Name', 'Flavor', 'Requirements'],
                                                  [imageName, flavor, requirements] )
      if not ret['OK']:
        return ret
      if not ret['Value'] or id <> ret['Value'][0][0]:
        image = self.__getInfo( 'Image', id )
        if image['OK']:
          self.log.error( 'Trying to insert Name: "%s", Flavor: "%s", Requirements: "%s"' %
                                            ( imageName, flavor, requirements ) )
          self.log.error( 'But inserted     Name: "%s", Flavor: "%s", Requirements: "%s"' %
                                            ( image['Name'], image['Flavor'], image['Requirements'] ) )
        return self.__setError( 'Image', id, 'Failed to insert new Image' )
      return DIRAC.S_OK( id )
    return DIRAC.S_ERROR( 'Failed to insert new Image' )

  def __addInstanceHistory( self, instanceID, status, load = 0.0 ):
    """
    Insert a History Record
    """
    self._insert( 'vm_History' , [ 'VMInstanceID', 'Status', 'Load', 'Update' ],
                                 [ instanceID, status, load, DIRAC.Time.toString() ] )
    return

  def __getInfo( self, object, id ):
    """
    Return dictionary with info for Images and Instances by ID
    """
    ( tableName, validStates, idName ) = self.__getTypeTuple( object )
    if not tableName:
      return DIRAC.S_ERROR( 'Unknown DB object: %s' % object )
    fields = self.__tablesDesc[ tableName ]['Fields']
    ret = self._getFields( tableName , fields, [idName], [id] )
    if not ret['OK']:
      return ret
    if not ret['Value']:
      return DIRAC.S_ERROR( 'Unknown %s = %s' % ( idName, id ) )

    info = DIRAC.S_OK()
    values = ret['Value'][0]
    fields = fields.keys()
    for i in range( len( fields ) ):
      info[fields[i]] = values[i]

    return info


  def __getStatus( self, object, id ):
    """
    Check and return status of Images and Instances by ID
    returns:
      S_OK(Status) if Status is valid and not Error 
      S_ERROR(ErrorMessage) otherwise
    """
    ( tableName, validStates, idName ) = self.__getTypeTuple( object )
    if not tableName:
      return DIRAC.S_ERROR( 'Unknown DB object: %s' % object )

    ret = self._getFields( tableName, ['Status', 'ErrorMessage'], [idName], [id] )
    if not ret['OK']:
      return ret

    if not ret['Value']:
      return DIRAC.S_ERROR( 'Unknown %s = %s' % ( idName, id ) )

    ( status, msg ) = ret['Value'][0]
    if not status in validStates:
      return self.__setError( object, id, 'Invalid Status: %s' % status )
    if status == validStates[-1]:
      return DIRAC.S_ERROR( msg )

    return DIRAC.S_OK( status )


  def __setError( self, object, id, reason ):
    """
    """
    ( tableName, validStates, idName ) = self.__getTypeTuple( object )
    if not tableName:
      return DIRAC.S_ERROR( 'Unknown DB object: %s' % object )

    cmd = 'UPDATE `%s` SET Status="%s",ErrorMessage="%s",LastUpdate=UTC_TIMESTAMP() WHERE %s = %s' % ( tableName,
                                                                                 validStates[-1],
                                                                                 reason,
                                                                                 idName,
                                                                                 id )
    ret = self._update( cmd )
    if not ret['OK']:
      return ret

    return DIRAC.S_ERROR( reason )
