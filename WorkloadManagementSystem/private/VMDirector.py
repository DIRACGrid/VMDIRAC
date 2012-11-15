########################################################################
# $HeadURL$
# File :   VMDirector.py
# Author : Ricardo Graciani
# Author : Victor Mendez, multisite
# Author : Victor Fernandez, CloudStack submit implementation
########################################################################
__RCSID__ = "$Id: VMDirector.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

import DIRAC
FROM_MAIL = "dirac.project@gmail.com"

from VMDIRAC.WorkloadManagementSystem.Client.ServerUtils import virtualMachineDB

class VMDirector:
  def __init__( self, submitPool ):

    self.log = DIRAC.gLogger.getSubLogger( '%sDirector' % submitPool )

    self.errorMailAddress = DIRAC.errorMail
    self.alarmMailAddress = DIRAC.alarmMail
    self.mailFromAddress = FROM_MAIL


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
    self.errorMailAddress = DIRAC.gConfig.getValue( mySection + '/ErrorMailAddress'     , self.errorMailAddress )
    self.alarmMailAddress = DIRAC.gConfig.getValue( mySection + '/AlarmMailAddress'     , self.alarmMailAddress )
    self.mailFromAddress = DIRAC.gConfig.getValue( mySection + '/MailFromAddress'      , self.mailFromAddress )


    # following will do something only when call from reload including SubmitPool as mySection
    requestedRunningPods = DIRAC.gConfig.getValue( mySection + '/RunningPods', self.runningPods.keys() )

    for runningPodName in requestedRunningPods:
      self.log.verbose( 'Trying to configure RunningPod:', runningPodName )
      if runningPodName in self.runningPods:
        continue
      runningPodDict = virtualMachineDB.getRunningPodDict( runningPodName )
      if not runningPodDict['OK']:
        return runningPodDict
      self.log.verbose( 'Trying to configure RunningPodDict:', runningPodDict )
      runningPodDict = runningPodDict[ 'Value' ]
      for option in ['Image', 'MaxInstances', 'CPUPerInstance', 'Priority', 'CloudEndpoints']:
        if option not in runningPodDict.keys():
          self.log.error( 'Missing option in "%s" RunningPod definition:' % runningPodName, option )
          continue
      self.runningPods[runningPodName] = {}
      self.runningPods[runningPodName]['Image'] = runningPodDict['Image']
      self.runningPods[runningPodName]['RequirementsDict'] = runningPodDict['Requirements']
      self.runningPods[runningPodName]['MaxInstances'] = int( runningPodDict['MaxInstances'] )
      self.runningPods[runningPodName]['CPUPerInstance'] = int( runningPodDict['CPUPerInstance'] )
      self.runningPods[runningPodName]['Priority'] = int( runningPodDict['Priority'] )
      self.runningPods[runningPodName]['CloudEndpoints'] = runningPodDict['CloudEndpoints']

  def submitInstance( self, imageName, workDir, endpoint, runningPodName ):
    """
    """
    self.log.info( '*** Preparint to submitting image: ', imageName )
    self.log.info( '******* of running pod: ', runningPodName )
    self.log.info( '******* destination: ', endpoint )
    if runningPodName not in self.runningPods:
      return DIRAC.S_ERROR( 'Unknown Running Pod: %s' % runningPodName )
    retDict = virtualMachineDB.insertInstance( imageName, imageName, endpoint, runningPodName )

    #########CloudStack2 adn CloudStack3 drivers have the bug of a single VM creation produces two VMs
    #########To deal with this CloudStack preaty feature we first startNewInstance inside VMDIRECTOR._submitInstance, and second we declare two VMs (retDict and retDict2)
    #########CloudStack check to preaty feature
    driver = DIRAC.gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( endpoint, "driver" ) )
    if ( driver == "CloudStack" ):
      retDict2 = virtualMachineDB.insertInstance( imageName, imageName, endpoint, runningPodName )

    if not retDict['OK']:
      return retDict
    instanceID = retDict['Value']
    retDict = self._submitInstance( imageName, workDir, endpoint )
    if not retDict['OK']:
      return retDict
    uniqueID = retDict[ 'Value' ]
    retDict = virtualMachineDB.setInstanceUniqueID( instanceID, uniqueID )

    #########CloudStack check to preaty feature
    if ( driver == "CloudStack" ):
      retDict2 = virtualMachineDB.setInstanceUniqueID( str( int( instanceID ) + 1 ), str( int( uniqueID ) - 1 ) )

    if not retDict['OK']:
      return retDict
    retDict = virtualMachineDB.declareInstanceSubmitted( uniqueID )

    #########CloudStack check to preaty feature
    if ( driver == "CloudStack" ):
      retDict2 = virtualMachineDB.declareInstanceSubmitted( str( int( uniqueID ) - 1 ) )

    if not retDict['OK']:
      return retDict
    return DIRAC.S_OK( imageName )

  def exceptionCallBack( self, threadedJob, exceptionInfo ):
    self.log.exception( 'Error in VM Instance Submission:', lExcInfo = exceptionInfo )
