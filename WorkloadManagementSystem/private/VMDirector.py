########################################################################
# $HeadURL: https://dirac-grid.googlecode.com/svn/BelleDIRAC/trunk/BelleDIRAC/WorkloadManagementSystem/private/VMDirector.py $
# File :   KVMDirector.py
# Author : Ricardo Graciani
########################################################################
__RCSID__ = "$Id: VMDirector.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

import DIRAC
FROM_MAIL = "dirac.project@gmail.com"

from BelleDIRAC.WorkloadManagementSystem.Client.ServerUtils import virtualMachineDB

class VMDirector:
  def __init__( self, submitPool ):

    if submitPool == self.Flavor:
      self.log = DIRAC.gLogger.getSubLogger( '%sPilotDirector' % self.Flavor )
    else:
      self.log = DIRAC.gLogger.getSubLogger( '%sPilotDirector/%s' % ( self.Flavor, submitPool ) )

    self.errorMailAddress = DIRAC.errorMail
    self.alarmMailAddress = DIRAC.alarmMail
    self.mailFromAddress = FROM_MAIL


  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for Amazon Director
    """
    self.images = {}

    self.configureFromSection( csSection )
    self.reloadConfiguration( csSection, submitPool )

    self.log.info( '===============================================' )
    self.log.info( 'Configuration:' )
    self.log.info( '' )
    self.log.info( 'Images:' )
    if self.images:
      self.log.info( ', '.join( self.images ) )
    else:
      self.log.info( ' None' )

  def reloadConfiguration( self, csSection, submitPool ):
    """
     Common Configuration can be overwriten for each GridMiddleware
    """
    mySection = csSection + '/' + self.Flavor
    self.configureFromSection( mySection )
    """
     And Again for each SubmitPool
    """
    mySection = csSection + '/' + submitPool
    self.configureFromSection( mySection )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    from BelleDIRAC.WorkloadManagementSystem.DB.VirtualMachineDB               import getImageDict
    self.log.debug( 'Configuring from %s' % mySection )
    self.errorMailAddress = DIRAC.gConfig.getValue( mySection + '/ErrorMailAddress'     , self.errorMailAddress )
    self.alarmMailAddress = DIRAC.gConfig.getValue( mySection + '/AlarmMailAddress'     , self.alarmMailAddress )
    self.mailFromAddress = DIRAC.gConfig.getValue( mySection + '/MailFromAddress'      , self.mailFromAddress )


    requestedImages = DIRAC.gConfig.getValue( mySection + '/Images', self.images.keys() )

    for imageName in requestedImages:
      self.log.verbose( 'Trying to configure Image:', imageName )
      if imageName in self.images:
        continue
      imageDict = getImageDict( imageName )
      if not imageDict['OK']:
        return imageDict
      if self.Flavor <> imageDict['Flavor']:
        continue
      for option in ['MaxInstances', 'CPUPerInstance', 'Priority']:
        if option not in imageDict.keys():
          self.log.error( 'Missing option in "%s" image definition:' % imageName, option )
          continue
      self.images[imageName] = {}
      self.images[imageName]['RequirementsDict'] = imageDict['Requirements']
      self.images[imageName]['MaxInstances'] = int( imageDict['MaxInstances'] )
      self.images[imageName]['CPUPerInstance'] = int( imageDict['CPUPerInstance'] )
      self.images[imageName]['Priority'] = int( imageDict['Priority'] )

  def submitInstance( self, imageName, workDir ):
    """
    """
    self.log.info( 'Submitting', imageName )
    if imageName not in self.images:
      return DIRAC.S_ERROR( 'Unknown Image: %s' % imageName )
    retDict = virtualMachineDB.insertInstance( imageName, imageName )
    if not retDict['OK']:
      return retDict
    instanceID = retDict['Value']
    retDict = self._submitInstance( imageName, workDir )
    if not retDict['OK']:
      return retDict
    retDict = virtualMachineDB.declareInstanceSubmitted( instanceID )
    if not retDict['OK']:
      return retDict
    return DIRAC.S_OK( imageName )

  def exceptionCallBack( self, threadedJob, exceptionInfo ):
    self.log.exception( 'Error in VM Instance Submission:', lExcInfo=exceptionInfo )
