########################################################################
# $HeadURL$
# File :   VMDirector.py
# Author : Ricardo Graciani
# Author : Victor Mendez, multisite
# Author : Victor Fernandez, CloudStack submit implementation
########################################################################

# DIRAC
from DIRAC import alarmMail, errorMail, gConfig, gLogger, S_ERROR, S_OK

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.ServerUtils import virtualMachineDB

__RCSID__ = "$Id: VMDirector.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"
FROM_MAIL = "dirac.project@gmail.com"

class VMDirector:
  def __init__( self, submitPool ):

    self.log = gLogger.getSubLogger( '%sDirector' % submitPool )

    self.errorMailAddress = errorMail
    self.alarmMailAddress = alarmMail
    self.mailFromAddress  = FROM_MAIL


  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for Cloud Director
    """

    self.runningPods = {}

    # csSection comming from VirtualMachineScheduler call
    self.configureFromSection( csSection )
    # reload will add SubmitPool to csSection to get the RunningPods and Images of a Director
    self.reloadConfiguration( csSection, submitPool )

    self.log.info( '===============================================' )
    self.log.info( 'Configuration:' )
    self.log.info( '' )
    self.log.info( 'Images:' )
    if self.runningPods:
      self.log.info( ', '.join( self.runningPods ) )
    else:
      self.log.info( ' None' )

  def reloadConfiguration( self, csSection, submitPool ):
    """
     For the SubmitPool
    """
    mySection = csSection + '/' + submitPool
    self.configureFromSection( mySection )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    self.log.debug( 'Configuring from %s' % mySection )
    self.errorMailAddress = gConfig.getValue( mySection + '/ErrorMailAddress' , self.errorMailAddress )
    self.alarmMailAddress = gConfig.getValue( mySection + '/AlarmMailAddress' , self.alarmMailAddress )
    self.mailFromAddress  = gConfig.getValue( mySection + '/MailFromAddress'  , self.mailFromAddress )

    # following will do something only when call from reload including SubmitPool as mySection
    requestedRunningPods = gConfig.getValue( mySection + '/RunningPods', self.runningPods.keys() )

    for runningPodName in requestedRunningPods:
      self.log.verbose( 'Trying to configure RunningPod:', runningPodName )
      if runningPodName in self.runningPods:
        continue
      runningPodDict = virtualMachineDB.getRunningPodDict( runningPodName )
      if not runningPodDict['OK']:
        self.log.error('Error in RunningPodDict: %s' % runningPodDict['Message'])
        return runningPodDict
      self.log.verbose( 'Trying to configure RunningPodDict:', runningPodDict )
      runningPodDict = runningPodDict[ 'Value' ]
      for option in ['Image', 'MaxInstances', 'CPUPerInstance', 'Priority', 'CloudEndpoints']:
        if option not in runningPodDict.keys():
          self.log.error( 'Missing option in "%s" RunningPod definition:' % runningPodName, option )
          continue
        
      self.runningPods[runningPodName] = {}
      self.runningPods[runningPodName]['Image']            = runningPodDict['Image']
      self.runningPods[runningPodName]['RequirementsDict'] = runningPodDict['Requirements']
      self.runningPods[runningPodName]['MaxInstances']     = int( runningPodDict['MaxInstances'] )
      self.runningPods[runningPodName]['CPUPerInstance']   = int( runningPodDict['CPUPerInstance'] )
      self.runningPods[runningPodName]['Priority']         = int( runningPodDict['Priority'] )
      self.runningPods[runningPodName]['CloudEndpoints']   = runningPodDict['CloudEndpoints']

  def submitInstance( self, imageName, endpoint, numVMsToSubmit, runningPodName ):
    """
    """
    # warning: instanceID is the DIRAC instance id, while uniqueID is unique for a particular endpoint
    self.log.info( '*** Preparing to submitting VM of image: ', imageName )
    self.log.info( '******* num of VMs to sumbit: ', numVMsToSubmit )
    self.log.info( '******* of running pod: ', runningPodName )
    self.log.info( '******* destination: ', endpoint )
    if runningPodName not in self.runningPods:
      return S_ERROR( 'Unknown Running Pod: %s' % runningPodName )

    for numVM in range(1,numVMsToSubmit+1):
      self.log.info( '********** Preparing to submitting VM number %s of %s VMs' % ( numVM, numVMsToSubmit ) )

      dictVMSubmitted = {}
      dictVMDBrecord = {}

      # FIRST, insert the instance into the DB !
      newInstance = virtualMachineDB.insertInstance( imageName, imageName, endpoint, runningPodName )
      if not newInstance[ 'OK' ]:
        return newInstance
      instanceID = newInstance[ 'Value' ]

      runningRequirementsDict = self.runningPods[runningPodName]['RequirementsDict']
      cpuTime = runningRequirementsDict['CPUTime']
      if not cpuTime:
        return S_ERROR( 'Unknown CPUTime in Requirements of the RunningPod %s' % runningPodName )

      dictVMSubmitted = self._submitInstance( imageName, endpoint, cpuTime, instanceID )
      if not dictVMSubmitted[ 'OK' ]:
        return dictVMSubmitted

      #########CloudStack2 adn CloudStack3 drivers have the bug of a single VM creation produces two VMs
      #########To deal with this CloudStack preaty feature we first startNewInstance inside 
      #########VMDIRECTOR._submitInstance, and second we declare two VMs 
      #########CloudStack check to preaty feature
      driver = gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( endpoint, "cloudDriver" ) )
      if driver == "CloudStack":
        virtualMachineDB.insertInstance( imageName, imageName, endpoint, runningPodName )

      if driver == "nova-1.1" or driver =="rocci-1.1":
        ( uniqueID, publicIP ) = dictVMSubmitted['Value']
        dictVMDBrecord = virtualMachineDB.setPublicIP( instanceID, publicIP )
        if not dictVMDBrecord['OK']:
          return dictVMDBrecord
      else: 
        uniqueID = dictVMSubmitted['Value']


      dictVMDBrecord = virtualMachineDB.setInstanceUniqueID( instanceID, uniqueID )
      if not dictVMDBrecord['OK']:
        return dictVMDBrecord

      #########CloudStack check to preaty feature
      if driver == "CloudStack":
        virtualMachineDB.setInstanceUniqueID( str( int( instanceID ) + 1 ), str( int( uniqueID ) - 1 ) )

      # check contextMethod and update status if need ssh contextualization:
      contextMethod = gConfig.getValue( "/Resources/VirtualMachines/Images/%s/%s" % ( imageName, "contextMethod" ) )
      if contextMethod == 'ssh':
        dictVMDBrecord = virtualMachineDB.declareInstanceWait_ssh_context( uniqueID )
        if not dictVMDBrecord['OK']:
          return dictVMDBrecord
      else:
        dictVMDBrecord = virtualMachineDB.declareInstanceSubmitted( uniqueID )
        if not dictVMDBrecord['OK']:
          return dictVMDBrecord

      #########CloudStack check to preaty feature
      if driver == "CloudStack":
        dictVMDBrecord = virtualMachineDB.declareInstanceSubmitted( str( int( uniqueID ) - 1 ) )

    return S_OK( imageName )

  def exceptionCallBack( self, threadedJob, exceptionInfo ):
    self.log.exception( 'Error in VM Instance Submission:', lExcInfo = exceptionInfo )
