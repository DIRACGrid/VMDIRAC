########################################################################
# $HeadURL$
# File :   VirtualMachineDB.py
# Author : Ricardo Graciani
# occi and multi endpoint author : Victor Mendez
########################################################################
""" VirtualMachineDB class is a front-end to the virtual machines DB

  Life cycle of VMs Images in DB
  - New:       Inserted by Director (Name - Status = New ) if not existing when launching a new instance
  - Validated: Declared by VMMonitoring Server when an Instance reports back correctly
  - Error:     Declared by VMMonitoring Server when an Instance reports back wrong requirements

  Life cycle of VMs Instances in DB
  - New:       Inserted by Director before launching a new instance, to check if image is valid
  - Submitted: Inserted by Director (adding UniqueID) when launches a new instance
  - Wait_ssh_context:     Declared by Director for submitted instance wich need later contextualization using ssh (VirtualMachineContextualization will check)
  - Contextualizing:     on the waith_ssh_context path is the next status before Running
  - Running:   Declared by VMMonitoring Server when an Instance reports back correctly (add LastUpdate, publicIP and privateIP)
  - Stopping:  Declared by VMManager Server when an Instance has been deleted outside of the VM (f.e "Delete" button on Browse Instances)
  - Halted:    Declared by VMMonitoring Server when an Instance reports halting
  - Stalled:   Declared by VMManager Server when detects Instance no more running
  - Error:     Declared by VMMonitoring Server when an Instance reports back wrong requirements or reports as running when Halted

  New Instances can be launched by Director if VMImage is not in Error Status.

  Instance UniqueID: for KVM it could be the MAC, for Amazon the returned InstanceID(i-5dec3236), for Occi returned the VMID

  Life cycle of VMs RunningPods in DB
  - New:       Inserted by VM Scheduler (RunningPod - Status = New ) if not existing when launching a new instance
  - Unactive:  Declared by VMScheduler Server when out of campaign dates
  - Active:    Declared by VMScheduler Server when withing of campaign dates
  - Error:     For compatibility with common private functions

"""

import types

# DIRAC
from DIRAC                import gConfig, S_ERROR, S_OK
from DIRAC.Core.Base.DB   import DB
from DIRAC.Core.Utilities import DEncode, Time

__RCSID__ = "$Id: VirtualMachineDB.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

class VirtualMachineDB( DB ):

  # When checking the Status on the DB it must be one of these values, if not, the last one (Error) is set
  # When declaring a new Status, it will be set to Error if not in the list
  validImageStates    = [ 'New', 'Validated', 'Error' ]
  validInstanceStates = [ 'New', 'Submitted', 'Wait_ssh_context', 'Contextualizing', 
                          'Running', 'Stopping', 'Halted', 'Stalled', 'Error' ]
  validRunningPodStates = [ 'New', 'Unactive', 'Active', 'Error' ]


  # In seconds !
  stallingInterval = 60 * 40 

  # When attempting a transition it will be checked if the current state is allowed 
  allowedTransitions = { 'Image' : {
                                       'Validated' : [ 'New', 'Validated' ],
                                   },
                        'Instance' : {
                                       'Wait_ssh_context' : [ 'New' ],
                                       'Submitted' : [ 'New' ],
                                       'Contextualizing' : [ 'Wait_ssh_context' ],
                                       'Running' : [ 'Submitted', 'Contextualizing', 'Running', 'Stalled' ],
                                       'Stopping' : [ 'Running', 'Stalled' ],
                                       'Halted' : [ 'New','Running', 'Stopping', 'Stalled' ],
                                       'Stalled': [ 'New', 'Submitted', 'Wait_ssh_context', 
                                                    'Contextualizing', 'Running' ],
                                      },
                        'RunningPod' : {
                                       'Active' : [ 'New', 'Active', 'Unactive' ],
                                       'Unactive' : [ 'New', 'Active', 'Unactive' ],
                                   }
                       }

  tablesDesc = {}

  tablesDesc[ 'vm_Images' ] = { 'Fields' : { 'VMImageID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                             'Name' : 'VARCHAR(255) NOT NULL',
                                             'Status' : 'VARCHAR(16) NOT NULL',
                                             'LastUpdate' : 'DATETIME',
                                             'ErrorMessage' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                           },
                               'PrimaryKey' : 'VMImageID',
                             }

  tablesDesc[ 'vm_Instances' ] = { 'Fields' : { 'InstanceID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                                'RunningPod' : 'VARCHAR(255) NOT NULL',
                                                'Name' : 'VARCHAR(255) NOT NULL',
                                                'Endpoint' : 'VARCHAR(32) NOT NULL',
                                                'UniqueID' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                                'VMImageID' : 'INTEGER UNSIGNED NOT NULL',
                                                'Status' : 'VARCHAR(32) NOT NULL',
                                                'LastUpdate' : 'DATETIME',
                                                'PublicIP' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                                'PrivateIP' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                                'ErrorMessage' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                                'MaxAllowedPrice' : 'FLOAT DEFAULT NULL',
                                                'Uptime' : 'INTEGER UNSIGNED DEFAULT 0',
                                                'Load' : 'FLOAT DEFAULT 0',
                                                'Jobs' : 'INTEGER UNSIGNED NOT NULL DEFAULT 0'
                                             },
                                   'PrimaryKey' : 'InstanceID',
                                   'Indexes': { 'Status': [ 'Status' ] },
                                 }

  tablesDesc[ 'vm_History' ] = { 'Fields' : { 'InstanceID' : 'INTEGER UNSIGNED NOT NULL',
                                              'Status' : 'VARCHAR(32) NOT NULL',
                                              'Load' : 'FLOAT NOT NULL',
                                              'Jobs' : 'INTEGER UNSIGNED NOT NULL DEFAULT 0',
                                              'TransferredFiles' : 'INTEGER UNSIGNED NOT NULL DEFAULT 0',
                                              'TransferredBytes' : 'BIGINT UNSIGNED NOT NULL DEFAULT 0',
                                              'Update' : 'DATETIME'
                                            },
                                 'Indexes': { 'InstanceID': [ 'InstanceID' ] },
                               }


  tablesDesc[ 'vm_RunningPods' ] = { 'Fields' : { 'RunningPodID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                              'RunningPod' : 'VARCHAR(32) NOT NULL',
                                              'CampaignStartDate' : 'DATETIME',
                                              'CampaignEndDate' : 'DATETIME',
                                              'Status' : 'VARCHAR(32) NOT NULL',
                                              'LastUpdate' : 'DATETIME',
                                              'ErrorMessage' : 'VARCHAR(255) NOT NULL DEFAULT ""'
                                            },
                                   'PrimaryKey' : 'RunningPodID',
                                   'Indexes': { 'RunningPod': [ 'RunningPod', 'Status' ]
                                          }
                               }


  #######################
  # VirtualDB constructor
  #######################

  def __init__( self, maxQueueSize = 10 ):
    
    DB.__init__( self, 'VirtualMachineDB', 'WorkloadManagement/VirtualMachineDB', maxQueueSize )
    if not self._MySQL__initialized:
      raise Exception( 'Can not connect to VirtualMachineDB, exiting...' )

    result = self.__initializeDB()
    if not result[ 'OK' ]:
      raise Exception( 'Can\'t create tables: %s' % result[ 'Message' ] )

  #######################
  # Public Functions
  #######################

  def setRunningPodStatus( self, runningPodName ):
    """
    Set Status of a given runningPod depending in date interval
    returns:
    S_OK(Status) if Status is valid and not Error
    S_ERROR(ErrorMessage) otherwise
    """
    tableName, validStates, idName = self.__getTypeTuple( 'RunningPod' )
   
    runningPodID = self._getFields( tableName, [ idName ], [ 'RunningPod' ], [ runningPodName ] )
    if not runningPodID[ 'OK' ]:
      return runningPodID
    runningPodID = runningPodID[ 'Value' ][0][0]

    if not runningPodID:
      return S_ERROR( 'Running pod %s not found in DB' % runningPodName )

    # The runningPod exits in DB set status

    runningPodDict = self.getRunningPodDict( runningPodName )
    if not runningPodDict[ 'OK' ]:
      return runningPodDict

    runningPodDict = runningPodDict[ 'Value' ]
    startdate=Time.fromString(runningPodDict['CampaignStartDate'])
    enddate=Time.fromString(runningPodDict['CampaignEndDate'])
    currentdate=Time.date()
    if currentdate<startdate:
      runningPodState='Unactive'
    elif currentdate>enddate:
      runningPodState='Unactive'
    else:
      runningPodState='Active'

    return self.__setState( 'RunningPod', runningPodID, runningPodState )

  def getRunningPodStatus( self, runningPodName):
    """
    Check Status of a given runningPod
    returns:
    S_OK(Status) if Status is valid and not Error
    S_ERROR(ErrorMessage) otherwise
    """
    tableName, validStates, idName = self.__getTypeTuple( 'RunningPod' )
   
    runningPodID = self._getFields( tableName, [ idName ], [ 'RunningPod' ], [ runningPodName ] )
    if not runningPodID[ 'OK' ]:
      return runningPodID
    runningPodID = runningPodID[ 'Value' ][0][0]

    if not runningPodID:
      return S_ERROR( 'Running pod %s not found in DB' % runningPodName )

    # The runningPod exits in DB set status

    return self.__getStatus( 'RunningPod', runningPodID )

  def getRunningPodDict( self, runningPodName ):
    """
    Return from CS a Dictionary with RunningPod definition
    """
    
    #FIXME: this MUST not be on the DB module !! 
    #FIXME: isn't checking for Image
    
    runningPodsCSPath = '/Resources/VirtualMachines/RunningPods'
    
    definedRunningPods = gConfig.getSections( runningPodsCSPath )
    if not definedRunningPods[ 'OK' ]:
      return definedRunningPods

    if runningPodName not in definedRunningPods['Value']:
      return S_ERROR( 'RunningPod "%s" not defined' % runningPodName )

    runningPodCSPath = '%s/%s' % ( runningPodsCSPath, runningPodName )

    runningPodDict = {}

    cloudEndpoints = gConfig.getValue( '%s/CloudEndpoints' % runningPodCSPath , '' )
    if not cloudEndpoints:
      return S_ERROR( 'Missing CloudEndpoints for RunnningPod "%s"' % runningPodName )
    
    for option, value in gConfig.getOptionsDict( runningPodCSPath )['Value'].items():
      runningPodDict[option] = value
      
    runningPodRequirementsDict = gConfig.getOptionsDict( '%s/Requirements' % runningPodCSPath )
    if not runningPodRequirementsDict[ 'OK' ]:
      return S_ERROR( 'Missing Requirements for RunningPod "%s"' % runningPodName )
    if 'CPUTime' in runningPodRequirementsDict[ 'Value' ]:
      runningPodRequirementsDict['Value']['CPUTime'] = int( runningPodRequirementsDict['Value']['CPUTime'] )
    if 'OwnerGroup' in runningPodRequirementsDict[ 'Value' ]:
      runningPodRequirementsDict['Value']['OwnerGroup'] = runningPodRequirementsDict['Value']['OwnerGroup'].split(', ')

    runningPodDict['Requirements'] = runningPodRequirementsDict['Value']

    return S_OK( runningPodDict )

  def insertRunningPod( self, runningPodName ):
    """
    Insert a RunningPod record
    If RunningPod name already exists then update CampaignStartDate, CampaignEndDate
    to be called by VMScheduler on creation of RunningPod record
    """
    tableName, validStates, idName = self.__getTypeTuple( 'RunningPod' )
    
    runningPodDict = self.getRunningPodDict( runningPodName )
    if not runningPodDict[ 'OK' ]:
      return runningPodDict
    runningPodDict = runningPodDict[ 'Value' ]

    runningPodID = self._getFields( tableName, [ idName ], [ 'RunningPod' ], [ runningPodName ] )
    if not runningPodID[ 'OK' ]:
      return runningPodID
    #runningPodID = runningPodID[ 'Value' ][0][0]

    if runningPodID[ 'Value' ]:
      runningPodID = runningPodID[ 'Value' ][0][0]
      if runningPodID > 0:
        # updating CampaignStartDate, CampaignEndDate
        sqlUpdate = 'UPDATE `%s` SET CampaignStartDate = "%s", CampaignEndDate = "%s" WHERE %s = %s' % \
          ( tableName, runningPodDict['CampaignStartDate'], runningPodDict['CampaignEndDate'], idName, runningPodID )
        return self._update( sqlUpdate )

    # The runningPod does not exits in DB, has to be inserted

    fields = [ 'RunningPod', 'CampaignStartDate', 'CampaignEndDate', 'Status']
    values = [ runningPodName, runningPodDict['CampaignStartDate'], runningPodDict['CampaignEndDate'], 'New']

    return self._insert( tableName , fields, values )



  def checkImageStatus( self, imageName, runningPodName = "" ):
    """ 
    Check Status of a given image
    Will insert a new Image in the DB if it does not exits
    returns:
      S_OK(Status) if Status is valid and not Error 
      S_ERROR(ErrorMessage) otherwise
    """
    ret = self.__getImageID( imageName, runningPodName )
    if not ret[ 'OK' ]:
      return ret
    return self.__getStatus( 'Image', ret[ 'Value' ] )

  def insertInstance( self, imageName, instanceName, endpoint, runningPodName ):
    """ 
    Check Status of a given image
    Will insert a new Instance in the DB
    returns:
      S_OK( InstanceID ) if new Instance is properly inserted 
      S_ERROR(ErrorMessage) otherwise
    """
    imageStatus = self.checkImageStatus( imageName, runningPodName )
    if not imageStatus[ 'OK' ]:
      return imageStatus

    return self.__insertInstance( imageName, instanceName, endpoint, runningPodName )

  def setInstanceUniqueID( self, instanceID, uniqueID ):
    """
    Assign a uniqueID to an instance
    """
    result = self.__getInstanceID( uniqueID )
    if result[ 'OK' ]:
      return S_ERROR( 'UniqueID is not unique: %s' % uniqueID )

    result = self._escapeString( uniqueID )
    if not result[ 'OK' ]:
      return result
    uniqueID = result[ 'Value' ]
    
    try:
      instanceID = int( instanceID )
    except ValueError:
    #except Exception, e:
      #FIXME: do we really want to raise an Exception ?
      #raise e
      return S_ERROR( "instanceID has to be a number" )
    
    tableName, _validStates, idName = self.__getTypeTuple( 'Instance' )
    
    sqlUpdate = "UPDATE `%s` SET UniqueID = %s WHERE %s = %d" % ( tableName, uniqueID, idName, instanceID )
    return self._update( sqlUpdate )

  def getUniqueID( self, instanceID ):
    """
    For a given dirac instanceID get the corresponding cloud endpoint uniqueID
    """
    tableName, _validStates, idName = self.__getTypeTuple( 'Instance' )

    sqlQuery = "SELECT UniqueID FROM `%s` WHERE %s = %s" % ( tableName, idName, instanceID )
    uniqueID = self._query( sqlQuery )

    if not uniqueID[ 'OK' ]:
      return uniqueID
    uniqueID = uniqueID[ 'Value' ]

    if not uniqueID:
      return S_ERROR( ' Unregistered VM, uniqueID not found for instanceID %s' % (instanceID) )

    return S_OK( uniqueID[ 0 ][ 0 ] )

  def getInstanceID( self, uniqueID ):
    """
    Public interface for  __getInstanceID
    """
    return self.__getInstanceID( uniqueID )

  def declareInstanceSubmitted( self, uniqueID ):
    """
    After submission of the instance the Director should declare the submitted Status
    """
    instanceID = self.__getInstanceID( uniqueID )
    if not instanceID[ 'OK' ]:
      return instanceID
    instanceID = instanceID[ 'Value' ]

    status = self.__setState( 'Instance', instanceID, 'Submitted' )
    if status[ 'OK' ]:
      self.__addInstanceHistory( instanceID, 'Submitted' )

    return status

  def declareInstanceWait_ssh_context ( self, uniqueID ):
    """
    After new instance the Director should declare the waiting for ssh contextualization Status
    """
    instanceID = self.__getInstanceID( uniqueID )
    if not instanceID[ 'OK' ]:
      return instanceID
    instanceID = instanceID[ 'Value' ]

    status = self.__setState( 'Instance', instanceID, 'Wait_ssh_context' )
    if status[ 'OK' ]:
      self.__addInstanceHistory( instanceID, 'Wait_ssh_context' )

    return status

  def declareInstanceContextualizing ( self, uniqueID ):
    """
    After new instance the Director should declare the waiting for ssh contextualization Status
    """
    instanceID = self.__getInstanceID( uniqueID )
    if not instanceID[ 'OK' ]:
      return instanceID
    instanceID = instanceID['Value']

    status = self.__setState( 'Instance', instanceID, 'Contextualizing' )
    if status[ 'OK' ]:
      self.__addInstanceHistory( instanceID, 'Contextualizing' )

    return status

  def setPublicIP( self, instanceID, publicIP ):
    """
    Update publicIP when used for contextualization previus to declareInstanceRunning
    """
    result = self._escapeString( publicIP )
    if not result[ 'OK' ]:
      return result
    publicIP = result[ 'Value' ]
    
    try:
      instanceID = int( instanceID )
    except ValueError:
      return S_ERROR( "instanceID has to be an integer value" )

    tableName, _validStates, idName = self.__getTypeTuple( 'Instance' )
    sqlUpdate = 'UPDATE `%s` SET PublicIP = %s WHERE %s = %d' %  ( tableName, publicIP, idName, instanceID )

    return self._update( sqlUpdate )

  def declareInstanceRunning( self, uniqueID, publicIP, privateIP = "" ):
    """
    Declares an instance Running and sets its associated info (uniqueID, publicIP, privateIP)
    Returns S_ERROR if:
      - instanceName does not have a "Submitted" or "Contextualizing" entry 
      - uniqueID is not unique
    """
    instanceID = self.__getInstanceID( uniqueID )
    if not instanceID[ 'OK' ]:
      return instanceID
    instanceID = instanceID[ 'Value' ]

    self.__setInstanceIPs( instanceID, publicIP, privateIP )

    status = self.__setState( 'Instance', instanceID, 'Running' )
    if status[ 'OK' ]:
      self.__addInstanceHistory( instanceID, 'Running' )

    return self.getAllInfoForUniqueID( uniqueID )

  def declareInstanceStopping( self, instanceID ):
    """
    From "Stop" buttom of Browse Instance
    Declares "Stopping" the instance, next heat-beat from VM will recibe a stop response to do an ordenate termination 
    It returns S_ERROR if the status is not OK
    """
    status = self.__setState( 'Instance', instanceID, 'Stopping' )
    if status[ 'OK' ]:
      self.__addInstanceHistory( instanceID, 'Stopping' )

    return status

  def getInstanceStatus( self, instanceID ):
    """
    By dirac instanceID
    """
    tableName, validStates, idName = self.__getTypeTuple( 'Instance' )
    if not tableName:
      return S_ERROR( 'Unknown DB object Instance' )

    ret = self.__getStatus( 'Instance', instanceID )
    if not ret[ 'OK' ]:
      return ret

    if not ret[ 'Value' ]:
      return S_ERROR( 'Unknown InstanceID = %s' % ( instanceID ) )

    status = ret[ 'Value' ]
    if not status in validStates:
      return self.__setError( 'Instances', instanceID, 'Invalid Status: %s' % status )

    return S_OK( status )

  def recordDBHalt( self, instanceID, load ):
    """
    Insert the heart beat info from a halting instance
    Declares "Halted" the instance and the image 
    It returns S_ERROR if the status is not OK
    """
    status = self.__setState( 'Instance', instanceID, 'Halted' )
    if status[ 'OK' ]:
      self.__addInstanceHistory( instanceID, 'Halted', load )

    return status

  def declareInstanceHalting( self, uniqueID, load ):
    """
    Insert the heart beat info from a halting instance
    Declares "Halted" the instance and the image 
    It returns S_ERROR if the status is not OK
    """
    instanceID = self.__getInstanceID( uniqueID )
    if not instanceID[ 'OK' ]:
      return instanceID
    instanceID = instanceID[ 'Value' ]

    status = self.__setState( 'Instance', instanceID, 'Halted' )
    if status[ 'OK' ]:
      self.__addInstanceHistory( instanceID, 'Halted', load )

    return status

  def declareStalledInstances( self ):
    """
    Check last Heart Beat for all Running instances and declare them Stalled if older than interval
    """
    oldInstances = self.__getOldInstanceIDs( self.stallingInterval, 
                                             self.allowedTransitions[ 'Instance' ][ 'Stalled' ] )
    if not oldInstances[ 'OK' ]:
      return oldInstances

    stallingInstances = []

    if not oldInstances[ 'Value' ]:
      return S_OK( stallingInstances )

    for instanceID in oldInstances['Value']:
      instanceID = instanceID[ 0 ]
      stalled    = self.__setState( 'Instance', instanceID, 'Stalled' )
      if not stalled[ 'OK' ]:
        continue
      
      self.__addInstanceHistory( instanceID, 'Stalled' )
      stallingInstances.append( instanceID )

    return S_OK( stallingInstances )


  def instanceIDHeartBeat( self, uniqueID, load, jobs, transferredFiles, transferredBytes, uptime ):
    """
    Insert the heart beat info from a running instance
    It checks the status of the instance and the corresponding image
    Declares "Running" the instance and the image 
    It returns S_ERROR if the status is not OK
    """
    instanceID = self.__getInstanceID( uniqueID )
    if not instanceID[ 'OK' ]:
      return instanceID
    instanceID = instanceID[ 'Value' ]

    result = self.__runningInstance( instanceID, load, jobs, transferredFiles, transferredBytes )
    if not result[ 'OK' ]:
      return result

    self.__setLastLoadJobsAndUptime( instanceID, load, jobs, uptime )

    status = self.__getStatus( 'Instance', instanceID )
    if not status[ 'OK' ]:
      return result
    status = status[ 'Value' ]

    if status == 'Stopping':
      return S_OK( 'stop' )
    return S_OK()

  def getPublicIpFromInstance( self, uniqueId ):
    """
    For a given instance uniqueId it returns the asociated PublicIP in the instance table, 
    thus the ImageName of such instance 
    Using _getFields( self, tableName, outFields = None, inFields = None, inValues = None, limit = 0, conn = None )
    """
    tableName, _validStates, _idName = self.__getTypeTuple( 'Instance' )
    
    publicIP = self._getFields( tableName, [ 'PublicIP' ], [ 'UniqueID' ], [ uniqueId ] )
    if not publicIP[ 'OK' ]:
      return publicIP
    publicIP = publicIP[ 'Value' ] 

    if not publicIP:
      return S_ERROR( 'Unknown %s = %s' % ( 'UniqueID', uniqueId ) )

    return S_OK( publicIP[ 0 ][ 0 ] )

  def getEndpointFromInstance( self, uniqueId ):
    """
    For a given instance uniqueId it returns the asociated Endpoint in the instance 
    table, thus the ImageName of such instance 
    Using _getFields( self, tableName, outFields = None, inFields = None, inValues = None, limit = 0, conn = None )
    """
    tableName, _validStates, _idName = self.__getTypeTuple( 'Instance' )
    
    endpoint = self._getFields( tableName, [ 'Endpoint' ], [ 'UniqueID' ], [ uniqueId ] )
    if not endpoint[ 'OK' ]:
      return endpoint
    endpoint = endpoint[ 'Value' ]

    if not endpoint:
      return S_ERROR( 'Unknown %s = %s' % ( 'UniqueID', uniqueId ) )

    return S_OK( endpoint[ 0 ][ 0 ] )

  def getImageNameFromInstance( self, uniqueId ):
    """
    For a given uniqueId it returns the asociated Name in the instance table, thus the ImageName of such instance 
    Using _getFields( self, tableName, outFields = None, inFields = None, inValues = None, limit = 0, conn = None )
    """
    tableName, _validStates, _idName = self.__getTypeTuple( 'Instance' )
    
    imageName = self._getFields( tableName, [ 'Name' ], [ 'UniqueID' ], [ uniqueId ] )
    if not imageName[ 'OK' ]:
      return imageName
    imageName = imageName[ 'Value' ]

    if not imageName:
      return S_ERROR( 'Unknown %s = %s' % ( 'UniqueID', uniqueId ) )

    return S_OK( imageName[ 0 ][ 0 ] )

  def getInstancesByStatus( self, status ):
    """
    Get dictionary of Image Names with InstanceIDs in given status 
    """
    if status not in self.validInstanceStates:
      return S_ERROR( 'Status %s is not known' % status )

    # InstanceTuple
    tableName, _validStates, _idName = self.__getTypeTuple( 'Instance' )

    runningInstances = self._getFields( tableName, [ 'VMImageID', 'UniqueID' ], ['Status'], [status] )
    if not runningInstances[ 'OK' ]:
      return runningInstances
    runningInstances = runningInstances[ 'Value' ]
    
    instancesDict = {}
    imagesDict    = {}
    
    # ImageTuple
    tableName, _validStates, idName = self.__getTypeTuple( 'Image' )  
    
    for imageID, uniqueID in runningInstances:
      
      if not imageID in imagesDict:       
        
        imageName = self._getFields( tableName, [ 'Name' ], [ idName ], [ imageID ] )
        if not imageName[ 'OK' ]:
          continue
        imagesDict[ imageID ] = imageName[ 'Value' ][ 0 ][ 0 ]
        
      if not imagesDict[ imageID ] in instancesDict:
        instancesDict[ imagesDict[ imageID ] ] = []
      instancesDict[ imagesDict[ imageID ] ].append( uniqueID )

    return S_OK( instancesDict )

  def getInstancesInfoByStatus( self, status ):
    """
    Get from Instances fields UniqueID, Endpoint, PublicIP, RunningPod  for instances in the given status 
    """
    if status not in self.validInstanceStates:
      return S_ERROR( 'Status %s is not known' % status )

    tableName, _validStates, _idName = self.__getTypeTuple( 'Instance' )

    runningInstances = self._getFields( tableName, [ 'UniqueID', 'Endpoint', 'PublicIP', 'RunningPod' ], 
                                        [ 'Status' ], [ status ] )
    if not runningInstances[ 'OK' ]:
      return runningInstances
    runningInstances = runningInstances[ 'Value' ]
    
    return S_OK( runningInstances )

  def getInstancesByStatusAndEndpoint( self, status, endpoint ):
    """
    Get dictionary of Image Names with InstanceIDs in given status 
    """
    if status not in self.validInstanceStates:
      return S_ERROR( 'Status %s is not known' % status )

    # InstanceTuple
    tableName, _validStates, _idName = self.__getTypeTuple( 'Instance' )

    runningInstances = self._getFields( tableName, [ 'VMImageID', 'UniqueID' ], 
                                        [ 'Status', 'Endpoint' ], [ status, endpoint ] )
    if not runningInstances[ 'OK' ]:
      return runningInstances
    runningInstances = runningInstances[ 'Value' ]
    
    instancesDict = {}
    imagesDict    = {}
    
    # InstanceTuple
    tableName, _validStates, idName = self.__getTypeTuple( 'Image' )
    
    for imageID, uniqueID in runningInstances:
      if not imageID in imagesDict:
        
        imageName = self._getFields( tableName, [ 'Name' ], [ idName ], [ imageID ] )
        if not imageName[ 'OK' ]:
          continue
        imagesDict[ imageID ] = imageName[ 'Value' ][ 0 ][ 0 ]
        
      if not imagesDict[ imageID ] in instancesDict:
        instancesDict[ imagesDict[ imageID ] ] = []
      instancesDict[ imagesDict[ imageID ] ].append( uniqueID )

    return S_OK( instancesDict )

  def getAllInfoForUniqueID( self, uniqueID ):
    """
    Get all fields for a uniqueID
    """
    instanceID = self.__getInstanceID( uniqueID )
    if not instanceID[ 'OK' ]:
      return instanceID
    instanceID = instanceID[ 'Value' ]
    
    instData = self.__getInfo( 'Instance', instanceID )
    if not instData[ 'OK' ]:
      return instData
    instData = instData[ 'Value' ]
    
    imgData = self.__getInfo( 'Image', instData[ 'VMImageID' ] )
    if not imgData[ 'OK' ]:
      return imgData
    imgData = imgData[ 'Value' ]
    
    return S_OK( { 'Image' : imgData, 'Instance' : instData } )

  #############################
  # Monitoring Public Functions
  #############################

  def getInstancesContent( self, selDict, sortList, start = 0, limit = 0 ):
    """
    Function to get the contents of the db
      parameters are a filter to the db
    """
    #Main fields
    tables = ( "`vm_Images` AS img", "`vm_Instances` AS inst")
    imageFields = ( 'VMImageID', 'Name')
    instanceFields = ( 'RunningPod', 'InstanceID', 'Endpoint', 'Name', 'UniqueID', 'VMImageID',
                       'Status', 'PublicIP', 'Status', 'ErrorMessage', 'LastUpdate', 'Load', 'Uptime', 'Jobs' )

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
    sqlQuery = "SELECT COUNT( InstanceID ) FROM %s WHERE %s" % ( ", ".join( tables ),
                                                                   " AND ".join( sqlCond ) )
    retVal = self._query( sqlQuery )
    if retVal[ 'OK' ]:
      totalRecords = retVal[ 'Value' ][0][0]
    #return
    return S_OK( { 'ParameterNames' : fields,
                         'Records' : data,
                         'TotalRecords' : totalRecords } )

  def getHistoryForInstanceID( self, instanceId ):
    try:
      instanceId = int( instanceId )
    except ValueError:
      return S_ERROR( "Instance Id has to be a number!" )
    
    fields    = ( 'Status', 'Load', 'Update', 'Jobs', 'TransferredFiles', 'TransferredBytes' )
    sqlFields = [ '`%s`' % f for f in fields ]
    
    sqlQuery = "SELECT %s FROM `vm_History` WHERE InstanceId=%d" % ( ", ".join( sqlFields ), instanceId )
    retVal = self._query( sqlQuery )
    if not retVal[ 'OK' ]:
      return retVal
    return S_OK( { 'ParameterNames' : fields, 'Records' : retVal[ 'Value' ] } )

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
      value = [ self._escapeString( str( v ) )[ 'Value' ] for v in value ]
      sqlCond.append( "`%s` in (%s)" % ( field, ", ".join( value ) ) )
    sqlQuery = "SELECT `%s`, COUNT( `%s` ) FROM `vm_Instances`" % ( groupField, groupField )
    
    if sqlCond:
      sqlQuery += " WHERE %s" % " AND ".join( sqlCond )
    sqlQuery += " GROUP BY `%s`" % groupField
    
    result = self._query( sqlQuery )
    if not result[ 'OK' ]:
      return result
    return S_OK( dict( result[ 'Value' ] ) )

  def getHistoryValues( self, averageBucket, selDict = {}, fields2Get = False, timespan = 0 ):
    try:
      timespan = max( 0, int( timespan ) )
    except ValueError:
      return S_ERROR( "Timespan has to be an integer" )

    cumulativeFields = [ 'Jobs', 'TransferredFiles', 'TransferredBytes' ]
    validDataFields  = [ 'Load', 'Jobs', 'TransferredFiles', 'TransferredBytes' ]
    allValidFields   = VirtualMachineDB.tablesDesc[ 'vm_History' ][ 'Fields' ]
    
    if not fields2Get:
      fields2Get = list( validDataFields )
    for field in fields2Get:
      if field not in validDataFields:
        return S_ERROR( "%s is not a valid data field" % field )

    #paramFields = fields2Get
    try:
      bucketSize = int( averageBucket )
    except ValueError:
      return S_ERROR( "Average bucket has to be an integer" )
    
    sqlGroup = "FROM_UNIXTIME(UNIX_TIMESTAMP( `Update` ) - UNIX_TIMESTAMP( `Update` ) mod %d)" % bucketSize
    sqlFields = [ '`InstanceID`', sqlGroup ] #+ [ "SUM(`%s`)/COUNT(`%s`)" % ( f, f ) for f in fields2Get ]
    for field in fields2Get:
      if field in cumulativeFields:
        sqlFields.append( "MAX(`%s`)" % field )
      else:
        sqlFields.append( "SUM(`%s`)/COUNT(`%s`)" % ( field, field ) )

    sqlGroup    = "%s, InstanceID" % sqlGroup
    paramFields = [ 'Update' ] + fields2Get
    sqlCond     = []
    
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

    return S_OK( { 'ParameterNames' : paramFields,
                         'Records' : finalData } )

  def getRunningInstancesHistory( self, timespan = 0, bucketSize = 900 ):
    
    try:
      bucketSize = max( 300, int( bucketSize ) )
    except ValueError:
      return S_ERROR( "Bucket has to be an integer" )
    
    try:
      timespan = max( 0, int( timespan ) )
    except ValueError:
      return S_ERROR( "Timespan has to be an integer" )

    groupby   = "FROM_UNIXTIME(UNIX_TIMESTAMP( `Update` ) - UNIX_TIMESTAMP( `Update` ) mod %d )" % bucketSize
    sqlFields = [ groupby, "COUNT( DISTINCT( `InstanceID` ) )" ]
    sqlQuery  = "SELECT %s FROM `vm_History`" % ", ".join( sqlFields )
    sqlCond   = [ "`Status` = 'Running'" ]
    
    if timespan > 0:
      sqlCond.append( "TIMESTAMPDIFF( SECOND, `Update`, UTC_TIMESTAMP() ) < %d" % timespan )
    sqlQuery += " WHERE %s" % " AND ".join( sqlCond )
    sqlQuery += " GROUP BY %s ORDER BY `Update` ASC" % groupby
    
    return self._query( sqlQuery )

  def getRunningInstancesBEPHistory( self, timespan = 0, bucketSize = 900 ):
    try:
      bucketSize = max( 300, int( bucketSize ) )
    except ValueError:
      return S_ERROR( "Bucket has to be an integer" )
    try:
      timespan = max( 0, int( timespan ) )
    except ValueError:
      return S_ERROR( "Timespan has to be an integer" )

    groupby   = "FROM_UNIXTIME(UNIX_TIMESTAMP( h.`Update` ) - UNIX_TIMESTAMP( h.`Update` ) mod %d )" % bucketSize
    sqlFields = [ groupby, " i.Endpoint, COUNT( DISTINCT( h.`InstanceID` ) ) " ]
    sqlQuery  = "SELECT %s FROM `vm_History` h, `vm_Instances` i" % ", ".join( sqlFields )
    sqlCond   = [ " h.InstanceID = i.InstanceID AND h.`Status` = 'Running'" ]
    
    if timespan > 0:
      sqlCond.append( "TIMESTAMPDIFF( SECOND, `Update`, UTC_TIMESTAMP() ) < %d" % timespan )
    sqlQuery += " WHERE %s" % " AND ".join( sqlCond )
    sqlQuery += " GROUP BY %s , EndPoint ORDER BY `Update` ASC" % groupby
    
    return self._query( sqlQuery )

  def getRunningInstancesByRunningPodHistory( self, timespan = 0, bucketSize = 900 ):
    try:
      bucketSize = max( 300, int( bucketSize ) )
    except ValueError:
      return S_ERROR( "Bucket has to be an integer" )
    try:
      timespan = max( 0, int( timespan ) )
    except ValueError:
      return S_ERROR( "Timespan has to be an integer" )

    groupby   = "FROM_UNIXTIME(UNIX_TIMESTAMP( h.`Update` ) - UNIX_TIMESTAMP( h.`Update` ) mod %d )" % bucketSize
    sqlFields = [ groupby, " i.RunningPod, COUNT( DISTINCT( h.`InstanceID` ) ) " ]
    sqlQuery  = "SELECT %s FROM `vm_History` h, `vm_Instances` i" % ", ".join( sqlFields )
    sqlCond   = [ " h.InstanceID = i.InstanceID AND h.`Status` = 'Running'" ]
    
    if timespan > 0:
      sqlCond.append( "TIMESTAMPDIFF( SECOND, `Update`, UTC_TIMESTAMP() ) < %d" % timespan )
    sqlQuery += " WHERE %s" % " AND ".join( sqlCond )
    sqlQuery += " GROUP BY %s , RunningPod ORDER BY `Update` ASC" % groupby
    
    return self._query( sqlQuery )

  def getRunningInstancesByImageHistory( self, timespan = 0, bucketSize = 900 ):
    try:
      bucketSize = max( 300, int( bucketSize ) )
    except ValueError:
      return S_ERROR( "Bucket has to be an integer" )
    try:
      timespan = max( 0, int( timespan ) )
    except ValueError:
      return S_ERROR( "Timespan has to be an integer" )

    groupby   = "FROM_UNIXTIME(UNIX_TIMESTAMP( h.`Update` ) - UNIX_TIMESTAMP( h.`Update` ) mod %d )" % bucketSize
    sqlFields = [ groupby, " ins.Name, COUNT( DISTINCT( h.`InstanceID` ) ) " ]
    sqlQuery  = "SELECT %s FROM `vm_History` h, `vm_Images` img, `vm_Instances` ins" % ", ".join( sqlFields )
    sqlCond   = [ " h.InstanceID = ins.InstanceID AND img.VMImageID = ins.VMImageID AND h.`Status` = 'Running'" ]
    
    if timespan > 0:
      sqlCond.append( "TIMESTAMPDIFF( SECOND, `Update`, UTC_TIMESTAMP() ) < %d" % timespan )
    sqlQuery += " WHERE %s" % " AND ".join( sqlCond )
    sqlQuery += " GROUP BY %s , ins.Name ORDER BY `Update` ASC" % groupby
    
    return self._query( sqlQuery )

  #######################
  # Private Functions
  #######################

  def __initializeDB( self ):
    """
    Create the tables
    """
    tables = self._query( "show tables" )
    if not tables[ 'OK' ]:
      return tables

    tablesInDB = [ table[0] for table in tables[ 'Value' ] ]
    
    tablesToCreate = {}
    for tableName in self.tablesDesc:
      if not tableName in tablesInDB:
        tablesToCreate[ tableName ] = self.tablesDesc[ tableName ]

    return self._createTables( tablesToCreate )

  def __getTypeTuple( self, element ):
    """
    return tuple of (tableName, validStates, idName) for object
    """
    # defaults
    tableName, validStates, idName = '', [], ''
        
    if element == 'Image':
      tableName   = 'vm_Images'
      validStates = self.validImageStates
      idName      = 'VMImageID'
    elif element == 'Instance':
      tableName   = 'vm_Instances'
      validStates = self.validInstanceStates
      idName      = 'InstanceID'
    elif element == 'RunningPod':
      tableName = 'vm_RunningPods'
      validStates = self.validRunningPodStates
      idName = 'RunningPodID'

    return ( tableName, validStates, idName )

  def __insertInstance( self, imageName, instanceName, endpoint, runningPodName ):
    """
    Attempts to insert a new Instance for the given Image in a given Endpoint of a runningPodName
    """
    image = self.__getImageID( imageName, runningPodName )
    if not image[ 'OK' ]:
      return image
    imageID = image[ 'Value' ]

    tableName, validStates, _idName = self.__getTypeTuple( 'Instance' )

    fields = [ 'RunningPod', 'Name', 'Endpoint', 'VMImageID', 'Status', 'LastUpdate' ]
    values = [ runningPodName, instanceName, endpoint, imageID, validStates[ 0 ], Time.toString() ]
    
    runningPodDict = self.getRunningPodDict( runningPodName )
    if not runningPodDict[ 'OK' ]:
      return runningPodDict
    runningPodDict = runningPodDict[ 'Value' ]
    
    if 'MaxAllowedPrice' in runningPodDict:
      fields.append( 'MaxAllowedPrice' )
      values.append( runningPodDict[ 'MaxAllowedPrice' ] )

    instance = self._insert( tableName , fields, values )
    if not instance[ 'OK' ]:
      return instance

    if 'lastRowId' in instance:
      self.__addInstanceHistory( instance[ 'lastRowId' ], validStates[ 0 ] )
      return S_OK( instance[ 'lastRowId' ] )   

    return S_ERROR( 'Failed to insert new Instance' )

  def __runningInstance( self, instanceID, load, jobs, transferredFiles, transferredBytes ):
    """
    Checks image status, set it to running and set instance status to running
    """
    # Check the Image is OK
    imageID = self.__getImageForRunningInstance( instanceID )
    if not imageID[ 'OK' ]:
      self.__setError( 'Instance', instanceID, imageID[ 'Message' ] )
      return imageID
    imageID = imageID[ 'Value' ]

    # Update Instance to Running
    stateInstance = self.__setState( 'Instance', instanceID, 'Running' )
    if not stateInstance[ 'OK' ]:
      return stateInstance

    # Update Image to Validated
    stateImage = self.__setState( 'Image', imageID, 'Validated' )
    if not stateImage[ 'OK' ]:
      self.__setError( 'Instance', instanceID, stateImage[ 'Message' ] )
      return stateImage

    # Add History record
    self.__addInstanceHistory( instanceID, 'Running', load, jobs, transferredFiles, transferredBytes )
    return S_OK()

  def __getImageForRunningInstance( self, instanceID ):
    """
    Looks for imageID for a given instanceID. 
    Check image Transition to Running is allowed
    Returns:
      S_OK( imageID )
      S_ERROR( Reason ) 
    """
    info = self.__getInfo( 'Instance', instanceID )
    if not info[ 'OK' ]:
      return info
    info = info[ 'Value' ]
    
    _tableName, _validStates, idName = self.__getTypeTuple( 'Image' )

    imageID = info[ idName ]

    imageStatus = self.__getStatus( 'Image', imageID )
    if not imageStatus[ 'OK' ]:
      return imageStatus

    return S_OK( imageID )

  def __getOldInstanceIDs( self, secondsIdle, states ):
    """
    Return list of instance IDs that have not updated after the given time stamp
    they are required to be in one of the given states
    """
    tableName, _validStates, idName = self.__getTypeTuple( 'Instance' )
    
    sqlCond = []
    sqlCond.append( 'TIMESTAMPDIFF( SECOND, `LastUpdate`, UTC_TIMESTAMP() ) > % d' % secondsIdle )
    sqlCond.append( 'Status IN ( "%s" )' % '", "'.join( states ) )
    
    sqlSelect = 'SELECT %s from `%s` WHERE %s' % ( idName, tableName, " AND ".join( sqlCond ) )
          
    return self._query( sqlSelect )

  def __getSubmittedInstanceID( self, imageName ):
    """
    Retrieve and InstanceID associated to a submitted Instance for a given Image
    """
    tableName, _validStates, idName = self.__getTypeTuple( 'Image' )
    
    imageID = self._getFields( tableName, [ idName ], ['Name'], [imageName] )
    if not imageID[ 'OK' ]:
      return imageID
    imageID = imageID[ 'Value' ]

    if not imageID:
      return S_ERROR( 'Unknown Image = %s' % imageName )

    #FIXME: <> is obsolete
    if len( imageID ) <> 1:
      return S_ERROR( 'Image name "%s" is not unique' % imageName )

    imageID     = imageID[ 0 ][ 0 ]
    imageIDName = idName

    tableName, _validStates, idName = self.__getTypeTuple( 'Instance' )

    instanceID = self._getFields( tableName, [ idName ], [ imageIDName, 'Status' ], 
                                  [ imageID, 'Submitted' ] )
    if not instanceID[ 'OK' ]:
      return instanceID
    instanceID = instanceID[ 'Value' ]

    if not instanceID:
      return S_ERROR( 'No Submitted instance of "%s" found' % imageName )

    return S_OK( instanceID[ 0 ][ 0 ] )

  def __setState( self, element, iD, state ):
    """
    Attempt to set element in state, checking if transition is allowed
    """
    
    knownStates = self.allowedTransitions[ element ].keys()
    if not state in knownStates:
      return S_ERROR( 'Transition to %s not possible' % state )

    allowedStates = self.allowedTransitions[ element ][ state ]

    currentState = self.__getStatus( element, iD )
    if not currentState[ 'OK' ]:
      return currentState
    currentState = currentState[ 'Value' ]

    if not currentState in allowedStates:
      msg = 'Transition ( %s -> %s ) not allowed' % ( currentState, state )
      if currentState == "Halted":
        val_state = "halt"
      elif currentState == "Stopping":
        val_state = "stop"
      else:
        val_state = currentState
      return {'OK': False, "Message": msg, 'State': val_state }

    tableName, _validStates, idName = self.__getTypeTuple( element )
    
    if currentState == state:
      sqlUpdate = 'UPDATE `%s` SET LastUpdate = UTC_TIMESTAMP() WHERE %s = %s' % ( tableName, idName, iD )

    else:
      sqlUpdate = 'UPDATE `%s` SET Status = "%s", LastUpdate = UTC_TIMESTAMP() WHERE %s = %s' % \
          ( tableName, state, idName, iD )
      

    ret = self._update( sqlUpdate )
    if not ret[ 'OK' ]:
      return ret
    return S_OK( state )


  def __setInstanceIPs( self, instanceID, publicIP, privateIP ):
    """
    Update parameters for an instanceID reporting as running
    """
    values = self._escapeValues( [ publicIP, privateIP ] )
    if not values[ 'OK' ]:
      return S_ERROR( "Cannot escape values: %s" % str( values ) )
    publicIP, privateIP = values[ 'Value' ]

    tableName, _validStates, idName = self.__getTypeTuple( 'Instance' )
    sqlUpdate = 'UPDATE `%s` SET PublicIP = %s, PrivateIP = %s WHERE %s = %s' % \
                ( tableName, publicIP, privateIP, idName, instanceID )

    return self._update( sqlUpdate )

  def __getInstanceID( self, uniqueID ):
    """
    For a given uniqueID of an instance return associated internal InstanceID 
    """
    tableName, _validStates, idName = self.__getTypeTuple( 'Instance' )
    
    instanceID = self._getFields( tableName, [ idName ], [ 'UniqueID' ], [ uniqueID ] )
    if not instanceID[ 'OK' ]:
      return instanceID
    instanceID = instanceID[ 'Value' ]

    if not instanceID:
      return S_ERROR( 'Unknown %s = %s' % ( 'UniqueID', uniqueID ) )

    return S_OK( instanceID[ 0 ][ 0 ] )

  def __getImageID( self, imageName, runningPodName ):
    """
    For a given imageName return corresponding ID
    Will insert the image in New Status if it does not exits, 
    """
    tableName, validStates, idName = self.__getTypeTuple( 'Image' )
    imageID = self._getFields( tableName, [ idName ], [ 'Name' ], [ imageName ] )
    if not imageID[ 'OK' ]:
      return imageID
    imageID = imageID[ 'Value' ]

    if len( imageID ) > 1:
      return S_ERROR( 'Image name "%s" is not unique' % imageName )
    if len( imageID ) == 0:
      # The image does not exits in DB, has to be inserted
      imageID = 0
    else:
      # The image exits in DB, has to match
      imageID = imageID[ 0 ][ 0 ]

    if imageID:
      ret = self._getFields( tableName, [ idName ], [ 'Name' ],
                                                  [ imageName ] )
      if not ret[ 'OK' ]:
        return ret
      if not ret[ 'Value' ]:
        return S_ERROR( 'Image "%s" in DB but it does not match' % imageName )
      else:
        return S_OK( imageID )

    ret = self._insert( tableName, [ 'Name', 'Status', 'LastUpdate' ],
                                   [ imageName, validStates[ 0 ], Time.toString() ] )

    if ret[ 'OK' ] and 'lastRowId' in ret:
    
      rowID = ret[ 'lastRowId' ]
    
      ret = self._getFields( tableName, [idName], ['Name'], [imageName] )
      if not ret[ 'OK' ]:
        return ret 
      
      if not ret[ 'Value' ] or rowID <> ret[ 'Value' ][ 0 ][ 0 ]:
        result = self.__getInfo( 'Image', rowID )
        if result[ 'OK' ]:
          image = result[ 'Value' ]
          self.log.error( 'Trying to insert Name: "%s"' % ( imageName ) )
          self.log.error( 'But inserted     Name: "%s"' % ( image['Name'] ) )
        return self.__setError( 'Image', rowID, 'Failed to insert new Image' )
      return S_OK( rowID )
    
    return S_ERROR( 'Failed to insert new Image' )

  def __addInstanceHistory( self, instanceID, status, load = 0.0, jobs = 0,
                            transferredFiles = 0, transferredBytes = 0 ):
    """
    Insert a History Record
    """
    try:
      load = float( load )
    except ValueError:
      return S_ERROR( "Load has to be a float value" )
    try:
      jobs = int( jobs )
    except ValueError:
      return S_ERROR( "Jobs has to be an integer value" )
    try:
      transferredFiles = int( transferredFiles )
    except ValueError:
      return S_ERROR( "Transferred files has to be an integer value" )

    self._insert( 'vm_History' , [ 'InstanceID', 'Status', 'Load',
                                   'Update', 'Jobs', 'TransferredFiles',
                                   'TransferredBytes' ],
                                 [ instanceID, status, load,
                                   Time.toString(), jobs,
                                   transferredFiles, transferredBytes ] )
    return

  def __setLastLoadJobsAndUptime( self, instanceID, load, jobs, uptime ):
    if not uptime:
      sqlQuery = "SELECT MAX( UNIX_TIMESTAMP( `Update` ) ) - MIN( UNIX_TIMESTAMP( `Update` ) ) FROM `vm_History` WHERE InstanceID = %d GROUP BY InstanceID" % instanceID
      result = self._query( sqlQuery )
      if result[ 'OK' ] and len( result[ 'Value' ] ) > 0:
        uptime = int( result[ 'Value' ][0][0] )
    sqlUpdate = "UPDATE `vm_Instances` SET `Uptime` = %d, `Jobs`= %d, `Load` = %f WHERE `InstanceID` = %d" % ( uptime,
                                                                                                     jobs,
                                                                                                     load,
                                                                                                     instanceID )
    self._update( sqlUpdate )
    return S_OK()

  def __getInfo( self, element, iD ):
    """
    Return dictionary with info for Images and Instances by ID
    """
    tableName, _validStates, idName = self.__getTypeTuple( element )
    if not tableName:
      return S_ERROR( 'Unknown DB object: %s' % element )
    
    fields = self.tablesDesc[ tableName ][ 'Fields' ]
    ret    = self._getFields( tableName , fields, [ idName ], [ iD ] )
    if not ret[ 'OK' ]:
      return ret
    if not ret[ 'Value' ]:
      return S_ERROR( 'Unknown %s = %s' % ( idName, iD ) )

    data   = {}
    values = ret[ 'Value' ][ 0 ]
    fields = fields.keys()
    
    for i in xrange( len( fields ) ):
      data[ fields[ i ] ] = values[ i ]

    return S_OK( data )

  def __getStatus( self, element, iD ):
    """
    Check and return status of Images and Instances by ID
    returns:
      S_OK(Status) if Status is valid and not Error 
      S_ERROR(ErrorMessage) otherwise
    """
    tableName, validStates, idName = self.__getTypeTuple( element )
    if not tableName:
      return S_ERROR( 'Unknown DB object: %s' % element )

    ret = self._getFields( tableName, [ 'Status', 'ErrorMessage' ], [ idName ], [ iD ] )
    if not ret[ 'OK' ]:
      return ret

    if not ret[ 'Value' ]:
      return S_ERROR( 'Unknown %s = %s' % ( idName, iD ) )

    status, msg = ret[ 'Value' ][ 0 ]
    if not status in validStates:
      return self.__setError( element, iD, 'Invalid Status: %s' % status )
    if status == validStates[ -1 ]:
      return S_ERROR( msg )

    return S_OK( status )

  def __setError( self, element, iD, reason ):
    """
    """
    ( tableName, validStates, idName ) = self.__getTypeTuple( element )
    if not tableName:
      return S_ERROR( 'Unknown DB object: %s' % element )

    sqlUpdate = 'UPDATE `%s` SET Status = "%s", ErrorMessage = "%s", LastUpdate = UTC_TIMESTAMP() WHERE %s = %s'
    sqlUpdate = sqlUpdate % ( tableName, validStates[ -1 ], reason, idName, iD )
    ret = self._update( sqlUpdate )
    if not ret[ 'OK' ]:
      return ret

    return S_ERROR( reason )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

