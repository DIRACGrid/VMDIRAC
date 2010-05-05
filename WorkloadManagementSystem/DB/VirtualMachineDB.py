########################################################################
# $HeadURL: https://dirac-grid.googlecode.com/svn/BelleDIRAC/trunk/BelleDIRAC/WorkloadManagementSystem/DB/VirtualMachineDB.py $
# File :   VirtualMachineDB.py
# Author : Ricardo Graciani
########################################################################
""" VirtualMachineDB class is a front-end to the virtual machines DB

  Life cycle of VMs Images in DB
  - New:       Inserted by Director (Name - Flavor - Requirements - Status = New ) if not existing when launching a new instance
  - Validated: Declared by VMMonitoring Server when an Instance reports back correctly
  - Error:     Declared by VMMonitoring Server when an Instance reports back wrong requirements

  Life cycle of VMs Instances in DB
  - New:       Inserted by Director before launching a new instance, to check if image is valid
  - Submitted: Inserted by Director (adding UniqueID) when launches a new instance
  - Running:   Declared by VMMonitoring Server when an Instance reports back correctly (add LastUpdate, publicIP and privateIP)
  - Halted:    Declared by VMMonitoring Server when an Instance reports halting
  - Stalled:   Declared by VMMonitoring Server when detects Instance no more running
  - Error:     Declared by VMMonitoring Server when an Instance reports back wrong requirements or reports as running when Halted

  New Instances can be launched by Director if VMImage is not in Error Status.

  Instance UniqueID: depends on Flavor, for KVM it could be the MAC, for Amazon the returned InstanceID(i-5dec3236)


"""

__RCSID__ = "$Id: VirtualMachineDB.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

import types
from DIRAC.Core.Base.DB import DB
import DIRAC

class VirtualMachineDB( DB ):

  # When checking the Status on the DB it must be one of these values, if not, the last one (Error) is set
  # When declaring a new Status, it will be set to Error if not in the list
  validImageStates = [ 'New', 'Validated', 'Error' ]
  validInstanceStates = [ 'New', 'Submitted', 'Running', 'Halted', 'Stalled', 'Error' ]

  stallingInterval = 30 * 60 # This are seconds

  # When attempting a transition it will be checked if the current state is allowed 
  allowedTransitions = { 'Image' : {
                                       'Validated' : [ 'New', 'Validated' ],
                                   },
                        'Instance' : {
                                       'Submitted' : [ 'New' ],
                                       'Running' : [ 'Submitted', 'Running', 'Stalled' ],
                                       'Halted' : [ 'Running', 'Stalled' ],
                                       'Stalled': [ 'New', 'Submitted', 'Running' ],
                                      }
                       }

  tablesDesc = {}

  tablesDesc[ 'vm_Images' ] = { 'Fields' : { 'VMImageID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
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

  tablesDesc[ 'vm_Instances' ] = { 'Fields' : { 'VMInstanceID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                                'Name' : 'VARCHAR(255) NOT NULL',
                                                'UniqueID' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                                'VMImageID' : 'INTEGER UNSIGNED NOT NULL',
                                                'Status' : 'VARCHAR(32) NOT NULL',
                                                'LastUpdate' : 'DATETIME',
                                                'PublicIP' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                                'PrivateIP' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                                'ErrorMessage' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                                'MaxPrice' : 'FLOAT DEFAULT NULL',
                                                'Uptime' : 'MEDIUMINT UNSIGNED DEFAULT 0',
                                                'Load' : 'FLOAT DEFAULT 0'
                                             },
                                   'PrimaryKey' : 'VMInstanceID',
                                   'Indexes': { 'Status': [ 'Status' ] },
                                 }

  tablesDesc[ 'vm_History' ] = { 'Fields' : { 'VMInstanceID' : 'INTEGER UNSIGNED NOT NULL',
                                              'Status' : 'VARCHAR(32) NOT NULL',
                                              'Load' : 'FLOAT NOT NULL',
                                              'Jobs' : 'INTEGER UNSIGNED NOT NULL DEFAULT 0',
                                              'TransferredFiles' : 'INTEGER UNSIGNED NOT NULL DEFAULT 0',
                                              'TransferredBytes' : 'BIGINT UNSIGNED NOT NULL DEFAULT 0',
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

  def checkImageStatus( self, imageName ):
    """ 
    Check Status of a given image
    Will insert a new Image in the DB if it does not exits
    returns:
      S_OK(Status) if Status is valid and not Error 
      S_ERROR(ErrorMessage) otherwise
    """
    ret = self.__getImageID( imageName )
    if not ret['OK']:
      return ret
    return self.__getStatus( 'Image', ret['Value'] )

  def insertInstance( self, imageName, instanceName ):
    """ 
    Check Status of a given image
    Will insert a new Instance in the DB
    returns:
      S_OK( InstanceID ) if new Instance is properly inserted 
      S_ERROR(ErrorMessage) otherwise
    """
    imageStatus = self.checkImageStatus( imageName )
    if not imageStatus['OK']:
      return imageStatus

    return self.__insertInstance( imageName, instanceName )

  def setInstanceUniqueID( self, instanceID, uniqueID ):
    """
    Assign a uniqueID to an instance
    """
    result = self.__getInstanceID( uniqueID )
    if result['OK']:
      return DIRAC.S_ERROR( 'UniqueID is not unique: %s' % uniqueID )

    result = self._escapeString( uniqueID )
    if not result['OK']:
      return result
    uniqueID = result[ 'Value' ]
    try:
      instanceID = int( instanceID )
    except Exception, e:
      raise e
      return DIRAC.S_ERROR( "Instance id has to be a number" )
    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Instance' )
    sqlUpdate = "UPDATE `%s` SET UniqueID = %s WHERE %s = %d" % ( tableName, uniqueID, idName, instanceID )
    return self._update( sqlUpdate )

  def declareInstanceSubmitted( self, uniqueID ):
    """
    After submission of the instance the Director should declare the new Status
    """
    instanceID = self.__getInstanceID( uniqueID )
    if not instanceID['OK']:
      return instanceID
    instanceID = instanceID['Value']

    status = self.__setState( 'Instance', instanceID, 'Submitted' )
    if status['OK']:
      self.__addInstanceHistory( instanceID, 'Submitted' )

    return status

  def declareInstanceRunning( self, uniqueID, publicIP, privateIP = "" ):
    """
    Declares an instance Running and sets its associated info (uniqueID, publicIP, privateIP)
    Returns S_ERROR if:
      - instanceName does not have a "Submitted" entry 
      - uniqueID is not unique
    """
    instanceID = self.__getInstanceID( uniqueID )
    if not instanceID['OK']:
      return instanceID
    instanceID = instanceID['Value']

    self.__setInstanceIPs( instanceID, publicIP, privateIP )

    status = self.__setState( 'Instance', instanceID, 'Running' )
    if status['OK']:
      self.__addInstanceHistory( instanceID, 'Running' )

    return self.getAllInfoForUniqueID( uniqueID )

  def declareInstanceHalting( self, uniqueID, load ):
    """
    Insert the heart beat info from a halting instance
    Declares "Halted" the instance and the image 
    It returns S_ERROR if the status is not OK
    """
    instanceID = self.__getInstanceID( uniqueID )
    if not instanceID['OK']:
      return instanceID
    instanceID = instanceID['Value']

    status = self.__setState( 'Instance', instanceID, 'Halted' )
    if status['OK']:
      self.__addInstanceHistory( instanceID, 'Halted', load )

    return status

  def declareStalledInstances( self ):
    """
    Check last Heart Beat for all Running instances and declare them Stalled if older than interval
    """
    oldInstances = self.__getOldInstanceIDs( self.stallingInterval, self.allowedTransitions['Instance']['Stalled'] )
    if not oldInstances['OK']:
      return oldInstances

    stallingInstances = []

    if not oldInstances['Value']:
      return DIRAC.S_OK( stallingInstances )

    for id in oldInstances['Value']:
      id = id[0]
      stalled = self.__setState( 'Instance', id, 'Stalled' )
      if not stalled['OK']:
        continue
      self.__addInstanceHistory( id, 'Stalled' )
      stallingInstances.append( id )

    return DIRAC.S_OK( stallingInstances )


  def instanceIDHeartBeat( self, uniqueID, load, jobs, transferredFiles, transferredBytes, uptime ):
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

    status = self.__runningInstance( instanceID, load, jobs, transferredFiles, transferredBytes )
    if not status['OK']:
      return status

    self.__setLastLoadAndUptime( instanceID, load, uptime )

    #TODO: Send empty dir just in case we want to send flags (such as stop vm)
    return DIRAC.S_OK( {} )

  def getInstancesByStatus( self, status ):
    """
    Get dictionary of Image Names with InstanceIDs in given status 
    """
    if status not in self.validInstanceStates:
      return DIRAC.S_ERROR( 'Status %s is not known' % status )

    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Instance' )

    runningInstances = self._getFields( tableName, [ 'VMImageID', 'UniqueID' ], ['Status'], [status] )
    if not runningInstances['OK']:
      return runningInstances
    runningInstances = runningInstances['Value']
    instancesDict = {}
    imagesDict = {}
    for imageID, uniqueID in runningInstances:
      if not imageID in imagesDict:
        ( tableName, validStates, idName ) = self.__getTypeTuple( 'Image' )
        imageName = self._getFields( tableName, ['Name'], [idName], [imageID] )
        if not imageName['OK']:
          continue
        imagesDict[imageID] = imageName['Value'][0][0]
      if not imagesDict[imageID] in instancesDict:
        instancesDict[imagesDict[imageID]] = []
      instancesDict[imagesDict[imageID]].append( uniqueID )

    return DIRAC.S_OK( instancesDict )

  def getAllInfoForUniqueID( self, uniqueID ):
    """
    Get all fields for a uniqueID
    """
    result = self.__getInstanceID( uniqueID )
    if not result['OK']:
      return result
    instanceID = result['Value']
    result = self.__getInfo( 'Instance', instanceID )
    if not result[ 'OK' ]:
      return result
    instData = result[ 'Value' ]
    result = self.__getInfo( 'Image', instData[ 'VMImageID' ] )
    if not result[ 'OK' ]:
      return result
    imgData = result[ 'Value' ]
    return DIRAC.S_OK( { 'Image' : imgData, 'Instance' : instData } )


  def __insertInstance( self, imageName, instanceName ):
    """
    Attempts to insert a new Instance for the given Image
    """
    image = self.__getImageID( imageName )
    if not image['OK']:
      return image
    imageID = image['Value']

    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Instance' )

    fields = ['Name', 'VMImageID', 'Status', 'LastUpdate' ]
    values = [instanceName, imageID, validStates[0], DIRAC.Time.toString() ]
    result = getImageDict( imageName )
    if not result[ 'OK' ]:
      return result
    imgDict = result[ 'Value' ]
    if 'MaxPrice' in imgDict:
      fields.append( 'MaxPrice' )
      values.append( imgDict[ 'MaxPrice' ] )

    instance = self._insert( tableName , fields, values )

    if instance['OK'] and 'lastRowId' in instance:
      self.__addInstanceHistory( instance['lastRowId'], validStates[0] )
      return DIRAC.S_OK( instance['lastRowId'] )

    if not instance['OK']:
      return instance

    return DIRAC.S_ERROR( 'Failed to insert new Instance' )

  def __runningInstance( self, instanceID, load, jobs, transferredFiles, transferredBytes ):
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

    # Update Image to Validated
    stateImage = self.__setState( 'Image', imageID, 'Validated' )
    if not stateImage['OK']:
      self.__setError( 'Instance', instanceID, stateImage['Message'] )
      return stateImage

    # Add History record
    self.__addInstanceHistory( instanceID, 'Running', load, jobs, transferredFiles, transferredBytes )
    return DIRAC.S_OK()

  def __getImageForRunningInstance( self, instanceID ):
    """
    Looks for imageID for a given instanceID. 
    Check image Transition to Running is allowed
    Returns:
      S_OK( imageID )
      S_ERROR( Reason ) 
    """
    result = self.__getInfo( 'Instance', instanceID )
    if not result['OK']:
      return result
    info = result[ 'Value' ]
    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Image' )

    imageID = info[ idName ]

    imageStatus = self.__getStatus( 'Image', imageID )
    if not imageStatus['OK']:
      return imageStatus

    return DIRAC.S_OK( imageID )

  def __getOldInstanceIDs( self, secondsIdle, states ):
    """
    Return list of instance IDs that have not updated after the given time stamp
    they are required to be in one of the given states
    """
    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Instance' )
    sqlCond = []
    sqlCond.append( 'TIMESTAMPDIFF( SECOND, `LastUpdate`, UTC_TIMESTAMP() ) > % d' % secondsIdle )
    sqlCond.append( 'Status IN ( "%s" )' % '", "'.join( states ) )
    cmd = 'SELECT %s from `%s` WHERE %s' % \
          ( idName, tableName, " AND ".join( sqlCond ) )
    return self._query( cmd )

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

    if len( imageID['Value'] ) <> 1:
      return DIRAC.S_ERROR( 'Image name "%s" is not unique' % imageName )

    imageID = imageID['Value'][0][0]
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

    ( tableName, validStates, idName ) = self.__getTypeTuple( object )

    currentState = currentState['Value']
    if currentState == state:
      cmd = 'UPDATE `%s` SET LastUpdate = UTC_TIMESTAMP() WHERE %s = %s' % \
          ( tableName, idName, id )

      ret = self._update( cmd )
      if not ret['OK']:
        return ret
      return DIRAC.S_OK( state )

    if not currentState in allowedStates:
      msg = 'Transition ( %s -> %s ) not allowed' % ( currentState, state )
      return DIRAC.S_ERROR( msg )

    cmd = 'UPDATE `%s` SET Status = "%s", LastUpdate = UTC_TIMESTAMP() WHERE %s = %s' % \
        ( tableName, state, idName, id )

    ret = self._update( cmd )
    if not ret['OK']:
      return ret

    return DIRAC.S_OK( state )

  def __setInstanceIPs( self, instanceID, publicIP, privateIP ):
    """
    Update parameters for an instanceID reporting as running
    """
    values = self._escapeValues( [ publicIP, privateIP] )
    if not values['OK']:
      return S_ERROR( "Cannot escape values: %s" % str( values ) )
    ( publicIP, privateIP ) = values['Value']

    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Instance' )
    cmd = 'UPDATE `%s` SET PublicIP = %s, PrivateIP = %s WHERE %s = %s' % \
             ( tableName, publicIP, privateIP, idName, instanceID )

    return self._update( cmd )

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

  def __getImageID( self, imageName ):
    """
    For a given imageName return corresponding ID
    Will insert the image in New Status if it does not exits
    """
    ( tableName, validStates, idName ) = self.__getTypeTuple( 'Image' )
    imageID = self._getFields( tableName, [ idName ], ['Name'], [imageName] )
    if not imageID['OK']:
      return imageID

    if len( imageID['Value'] ) > 1:
      return DIRAC.S_ERROR( 'Image name "%s" is not unique' % imageName )
    if len( imageID['Value'] ) == 0:
      # The image does not exits in DB, has to be inserted
      imageID = 0
    else:
      # The image exits in DB, has to match
      imageID = imageID['Value'][0][0]

    result = getImageDict( imageName )
    if not result['OK']:
      return result
    imageDict = result[ 'Value' ]
    flavor = imageDict['Flavor']
    requirements = DIRAC.DEncode.encode( imageDict['Requirements'] )

    if imageID:
      ret = self._getFields( tableName, [idName], ['Name', 'Flavor', 'Requirements'],
                                                  [imageName, flavor, requirements] )
      if not ret['OK']:
        return ret
      if not ret['Value']:
        return DIRAC.S_ERROR( 'Image "%s" in DB but it does not match' % imageName )
      else:
        return DIRAC.S_OK( imageID )

    ret = self._insert( tableName, ['Name', 'Flavor', 'Requirements', 'Status', 'LastUpdate'],
                                     [imageName, flavor, requirements, validStates[0], DIRAC.Time.toString()] )
    if ret['OK'] and 'lastRowId' in ret:
      id = ret['lastRowId']
      ret = self._getFields( tableName, [idName], ['Name', 'Flavor', 'Requirements'],
                                                  [imageName, flavor, requirements] )
      if not ret['OK']:
        return ret
      if not ret['Value'] or id <> ret['Value'][0][0]:
        result = self.__getInfo( 'Image', id )
        if result['OK']:
          image = result[ 'Value' ]
          self.log.error( 'Trying to insert Name: "%s", Flavor: "%s", Requirements: "%s"' %
                                            ( imageName, flavor, requirements ) )
          self.log.error( 'But inserted     Name: "%s", Flavor: "%s", Requirements: "%s"' %
                                            ( image['Name'], image['Flavor'], image['Requirements'] ) )
        return self.__setError( 'Image', id, 'Failed to insert new Image' )
      return DIRAC.S_OK( id )
    return DIRAC.S_ERROR( 'Failed to insert new Image' )

  def __addInstanceHistory( self, instanceID, status, load = 0.0, jobs = 0,
                            transferredFiles = 0, transferredBytes = 0 ):
    """
    Insert a History Record
    """
    try:
      load = float( load )
    except:
      return DIRAC.S_ERROR( "Load has to be a float value" )
    try:
      jobs = int( jobs )
    except:
      return DIRAC.S_ERROR( "Jobs has to be an integer value" )
    try:
      transferredFiles = int( transferredFiles )
    except:
      return DIRAC.S_ERROR( "Transferred files has to be an integer value" )

    self._insert( 'vm_History' , [ 'VMInstanceID', 'Status', 'Load',
                                   'Update', 'Jobs', 'TransferredFiles',
                                   'TransferredBytes' ],
                                 [ instanceID, status, load,
                                   DIRAC.Time.toString(), jobs,
                                   transferredFiles, transferredBytes ] )
    return

  def __setLastLoadAndUptime( self, instanceID, load, uptime ):
    sqlUpdate = "UPDATE `vm_Instances` SET `Uptime` = %f, `Load` = %f WHERE `VMInstanceID` = %d" % ( uptime,
                                                                                                     load,
                                                                                                     instanceID )
    self._update( sqlUpdate )
    return DIRAC.S_OK()

  def __getInfo( self, object, id ):
    """
    Return dictionary with info for Images and Instances by ID
    """
    ( tableName, validStates, idName ) = self.__getTypeTuple( object )
    if not tableName:
      return DIRAC.S_ERROR( 'Unknown DB object: %s' % object )
    fields = self.tablesDesc[ tableName ]['Fields']
    ret = self._getFields( tableName , fields, [idName], [id] )
    if not ret['OK']:
      return ret
    if not ret['Value']:
      return DIRAC.S_ERROR( 'Unknown %s = %s' % ( idName, id ) )

    data = {}
    values = ret['Value'][0]
    fields = fields.keys()
    for i in range( len( fields ) ):
      data[fields[i]] = values[i]

    return DIRAC.S_OK( data )


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

    cmd = 'UPDATE `%s` SET Status = "%s", ErrorMessage = "%s", LastUpdate = UTC_TIMESTAMP() WHERE %s = %s' % ( tableName,
                                                                                 validStates[-1],
                                                                                 reason,
                                                                                 idName,
                                                                                 id )
    ret = self._update( cmd )
    if not ret['OK']:
      return ret

    return DIRAC.S_ERROR( reason )

  #Monitoring functions

  def getInstancesContent( self, selDict, sortList, start = 0, limit = 0 ):
    """
    Function to get the contents of the db
      parameters are a filter to the db
    """
    #Main fields
    tables = ( "`vm_Images` AS img", "`vm_Instances` AS inst" )
    imageFields = ( 'VMImageID', 'Name', 'Flavor' )
    instanceFields = ( 'VMInstanceID', 'Name', 'UniqueID', 'VMImageID',
                       'Status', 'PublicIP', 'Status', 'ErrorMessage', 'LastUpdate' )

    fields = [ 'img.%s' % f for f in imageFields ] + [ 'inst.%s' % f for f in instanceFields ]
    sqlQuery = "SELECT %s FROM %s" % ( ", ".join( fields ), ", ".join( tables ) )
    sqlCond = [ 'img.VMImageID = inst.VMImageID' ]
    for field in selDict:
      if field in instanceFields:
        sqlField = "inst.%s" % field
      elif field in imageFields:
        sqlField = "img.%s" % field
      elif field in fields:
        sqlField = field
      else:
        continue
      value = selDict[ field ]
      if type( value ) in ( types.StringType, types.UnicodeType ):
        value = [ str( value ) ]
      sqlCond.append( " OR ".join( [ "%s=%s" % ( sqlField, self._escapeString( str( value ) )[ 'Value' ] ) for value in selDict[field] ] ) )
    sqlQuery += " WHERE %s" % " AND ".join( sqlCond )
    if sortList:
      sqlSortList = []
      for sorting in sortList:
        if sorting[0] in instanceFields:
          sqlField = "inst.%s" % sorting[0]
        elif sorting[0] in imageFields:
          sqlField = "img.%s" % sorting[0]
        elif sorting[0] in fields:
          sqlField = sorting[0]
        else:
          continue
        direction = sorting[1].upper()
        if direction not in ( "ASC", "DESC" ):
          continue
        sqlSortList.append( "%s %s" % ( sqlField, direction ) )
      if sqlSortList:
        sqlQuery += " ORDER BY %s" % ", ".join( sqlSortList )
    if limit:
      sqlQuery += " LIMIT %d,%d" % ( start, limit )
    retVal = self._query( sqlQuery )
    if not retVal[ 'OK' ]:
      return retVal
    data = []
    #Total records
    for record in retVal[ 'Value' ]:
      record = list( record )
      data.append( record )
    totalRecords = len( data )
    sqlQuery = "SELECT COUNT( VMInstanceID ) FROM %s WHERE %s" % ( ", ".join( tables ),
                                                                   " AND ".join( sqlCond ) )
    retVal = self._query( sqlQuery )
    if retVal[ 'OK' ]:
      totalRecords = retVal[ 'Value' ][0][0]
    #Get load
    running = 0
    statusPos = fields.index( 'inst.Status' )
    instanceIDPos = fields.index( 'inst.VMInstanceID' )
    for record in data:
      if record[ statusPos ] == 'Running':
        running += 1
    #sqlQuery = "SELECT VMInstanceID, SUM(`Load`)/COUNT(`Load`) from `vm_History` WHERE VMInstanceID in ( SELECT VMInstanceID from `vm_Instances` WHERE `Status` = 'Running' ) AND `Status` = 'Running' GROUP BY VMInstanceID ORDER BY `Update` DESC limit 1, %d" % ( running * 2 )
    sqlQuery = 'SELECT `VMInstanceID`, SUM( `Load` ) / COUNT( `Load` ) from  ( SELECT `VMInstanceID`, `Load` from `vm_History` WHERE Status = "Running" order by `Update` DESC limit % d ) as a GROUP BY `VMInstanceID`' % int( running * 3 )
    result = self._query( sqlQuery )
    if not result[ 'OK' ]:
      return result
    histData = dict( result[ 'Value' ] )
    fields.append( "hist.Load" )
    #Running time
    sqlQuery = 'SELECT `VMInstanceID`, MAX( UNIX_TIMESTAMP( `Update` ) ) - MIN( UNIX_TIMESTAMP( `Update` ) ) FROM `vm_History` WHERE Status = "Running" GROUP BY `VMInstanceID`';
    result = self._query( sqlQuery )
    if not result[ 'OK' ]:
      return result
    runningTime = dict( result[ 'Value' ] )
    fields.append( "hist.RunningTime" )
    #Append to data
    for record in data:
      instID = record[ instanceIDPos ]
      if instID in histData:
        record.append( "%.2f" % histData[ instID ] )
      else:
        record.append( "" )
      if instID in runningTime:
        record.append( int( runningTime[ instID ] ) )
      else:
        record.append( 0 )
    #return
    return DIRAC.S_OK( { 'ParameterNames' : fields,
                         'Records' : data,
                         'TotalRecords' : totalRecords } )

  def getHistoryForInstanceID( self, instanceId ):
    try:
      instanceId = int( instanceId )
    except:
      return DIRAC.S_ERROR( "Instance Id has to be a number!" )
    fields = ( 'Status', 'Load', 'Update', 'Jobs', 'TransferredFiles', 'TransferredBytes' )
    sqlFields = [ '`%s`' % f for f in fields ]
    sqlQuery = "SELECT %s FROM `vm_History` WHERE VMInstanceId=%d" % ( ", ".join( sqlFields ), instanceId )
    retVal = self._query( sqlQuery )
    if not retVal[ 'OK' ]:
      return retVal
    return DIRAC.S_OK( { 'ParameterNames' : fields,
                         'Records' : retVal[ 'Value' ] } )

  def getInstanceCounters( self, groupField = "Status", selDict = {} ):
    validFields = VirtualMachineDB.tablesDesc[ 'vm_Instances' ][ 'Fields' ]
    if groupField not in validFields:
      return S_ERROR( "%s is not a valid field" % groupField )
    sqlCond = []
    for field in selDict:
      if field not in validFields:
        return S_ERROR( "%s is not a valid field" % field )
      value = selDict[ field ]
      if type( value ) not in ( types.DictType, types.TupleType ):
        value = ( value, )
      value = [ self._escapeString( str( v ) )[ 'Value' ] for v in values ]
      sqlCond.append( "`%s` in (%s)" % ( field, ", ".join( value ) ) )
    sqlQuery = "SELECT `%s`, COUNT( `%s` ) FROM `vm_Instances`" % ( groupField, groupField )
    if sqlCond:
      sqlQuery += " WHERE %s" % " AND ".join( sqlCond )
    sqlQuery += " GROUP BY `%s`" % groupField
    result = self._query( sqlQuery )
    if not result[ 'OK' ]:
      return result
    return DIRAC.S_OK( dict( result[ 'Value' ] ) )

  def getHistoryValues( self, averageBucket, selDict = {}, fields2Get = False, timespan = 0 ):
    try:
      timespan = max( 0, int( timespan ) )
    except:
      return S_ERROR( "Timespan has to be an integer" )

    cumulativeFields = [ 'Jobs', 'TransferredFiles', 'TransferredBytes' ]
    validDataFields = [ 'Load', 'Jobs', 'TransferredFiles', 'TransferredBytes' ]
    allValidFields = VirtualMachineDB.tablesDesc[ 'vm_History' ][ 'Fields' ]
    if not fields2Get:
      fields2Get = list( validDataFields )
    for field in fields2Get:
      if field not in validDataFields:
        return S_ERROR( "%s is not a valid data field" % field )

    paramFields = fields2Get
    try:
      bucketSize = int( averageBucket )
    except:
      return S_ERROR( "Average bucket has to be an integer" )
    sqlGroup = "FROM_UNIXTIME(UNIX_TIMESTAMP( `Update` ) - UNIX_TIMESTAMP( `Update` ) mod %d)" % bucketSize
    sqlFields = [ '`VMInstanceID`', sqlGroup ] #+ [ "SUM(`%s`)/COUNT(`%s`)" % ( f, f ) for f in fields2Get ]
    for f in fields2Get:
      if f in cumulativeFields:
        sqlFields.append( "MAX(`%s`)" % f )
      else:
        sqlFields.append( "SUM(`%s`)/COUNT(`%s`)" % ( f, f ) )

    sqlGroup = "%s, VMInstanceID" % sqlGroup
    paramFields = [ 'Update' ] + fields2Get
    sqlCond = []
    for field in selDict:
      if field not in allValidFields:
        return S_ERROR( "%s is not a valid field" % field )
      value = selDict[ field ]
      if type( value ) not in ( types.ListType, types.TupleType ):
        value = ( value, )
      value = [ self._escapeString( str( v ) )[ 'Value' ] for v in value ]
      sqlCond.append( "`%s` in (%s)" % ( field, ", ".join( value ) ) )
    if timespan > 0:
      sqlCond.append( "TIMESTAMPDIFF( SECOND, `Update`, UTC_TIMESTAMP() ) < %d" % timespan )
    sqlQuery = "SELECT %s FROM `vm_History`" % ", ".join( sqlFields )
    if sqlCond:
      sqlQuery += " WHERE %s" % " AND ".join( sqlCond )
    sqlQuery += " GROUP BY %s ORDER BY `Update` ASC" % sqlGroup
    result = self._query( sqlQuery )
    if not result[ 'OK' ]:
      return result
    dbData = result[ 'Value' ]
    #Need ext?
    requireExtension = set()
    for i in range( len( fields2Get ) ):
      f = fields2Get[i]
      if f in cumulativeFields:
        requireExtension.add( i )

    if requireExtension:
      rDates = []
      for row in dbData:
        if row[1] not in rDates:
          rDates.append( row[1] )
      vmData = {}
      for row in dbData:
        vmID = row[0]
        if vmID not in vmData:
          vmData[ vmID ] = {}
        vmData[ vmID ][ row[1] ] = row[2:]
      rDates.sort()

      dbData = []
      for vmID in vmData:
        prevValues = False
        for rDate in rDates:
          if rDate not in vmData[ vmID ]:
            if prevValues:
              instValues = [ rDate ]
              for i in range( len( prevValues ) ):
                instValues.append( prevValues[ i ] )
              dbData.append( instValues )
          else:
            row = vmData[ vmID ][ rDate ]
            prevValues = []
            for i in range( len ( row ) ):
              if i in requireExtension:
                prevValues.append( row[i] )
              else:
                prevValues.append( 0 )

            instValues = [ rDate ]
            for i in range( len( row ) ):
              instValues.append( row[ i ] )
            dbData.append( instValues )
    else:
      #If we don't require extension just strip vmName
      dbData = [ row[1:] for row in dbData ]

    #Final sum
    sumData = {}
    for record in dbData:
      recDate = record[0]
      rawData = record[1:]
      if recDate not in sumData:
        sumData[ recDate ] = [ 0.0 for f in rawData ]
      for i in range( len( rawData ) ):
        sumData[ recDate ][i] += float( rawData[i] )
    finalData = []
    if len( sumData ) > 0:
      firstValues = sumData[ sorted( sumData )[0] ]
      for date in sorted( sumData ):
        finalData.append( [ date ] )
        values = sumData[ date ]
        for i in range( len( values ) ):
          if i in requireExtension:
            finalData[-1].append( max( 0, values[i] - firstValues[i] ) )
          else:
            finalData[-1].append( values[i] )

    return DIRAC.S_OK( { 'ParameterNames' : paramFields,
                         'Records' : finalData } )

  def getRunningInstancesHistory( self, timespan = 0, bucketSize = 900 ):
    try:
      bucketSize = max( 300, int( bucketSize ) )
    except:
      return DIRAC.S_ERROR( "Bucket has to be an integer" )
    try:
      timespan = max( 0, int( timespan ) )
    except:
      return DIRAC.S_ERROR( "Timespan has to be an integer" )

    groupby = "FROM_UNIXTIME(UNIX_TIMESTAMP( `Update` ) - UNIX_TIMESTAMP( `Update` ) mod %d )" % bucketSize
    sqlFields = [ groupby, "COUNT( DISTINCT( `VMInstanceID` ) )" ]
    sqlQuery = "SELECT %s FROM `vm_History`" % ", ".join( sqlFields )
    sqlCond = [ "`Status` = 'Running'" ]
    if timespan > 0:
      sqlCond.append( "TIMESTAMPDIFF( SECOND, `Update`, UTC_TIMESTAMP() ) < %d" % timespan )
    sqlQuery += " WHERE %s" % " AND ".join( sqlCond )
    sqlQuery += " GROUP BY %s ORDER BY `Update` ASC" % groupby
    return self._query( sqlQuery )

def getImageDict( imageName ):
  """
  Return from CS a Dictionary with Image definition
  """
  imagesCSPath = '/Resources/VirtualMachines/Images'
  definedImages = DIRAC.gConfig.getSections( imagesCSPath )
  if not definedImages['OK']:
    return definedImages

  if imageName not in definedImages['Value']:
    return DIRAC.S_ERROR( 'Image "%s" not defined' % imageName )

  imageCSPath = '%s/%s' % ( imagesCSPath, imageName )

  imageDict = {}
  flavor = DIRAC.gConfig.getValue( '%s/Flavor' % imageCSPath , '' )
  if not flavor:
    return DIRAC.S_ERROR( 'Missing Flavor for image "%s"' % imageName )
  for option, value in DIRAC.gConfig.getOptionsDict( imageCSPath )['Value'].items():
    imageDict[option] = value
  imageRequirementsDict = DIRAC.gConfig.getOptionsDict( '%s/Requirements' % imageCSPath )
  if not imageRequirementsDict['OK']:
    return DIRAC.S_ERROR( 'Missing Requirements for image "%s"' % imageName )
  if 'CPUTime' in imageRequirementsDict['Value']:
    imageRequirementsDict['Value']['CPUTime'] = int( imageRequirementsDict['Value']['CPUTime'] )
  imageDict['Requirements'] = imageRequirementsDict['Value']

  return DIRAC.S_OK( imageDict )

