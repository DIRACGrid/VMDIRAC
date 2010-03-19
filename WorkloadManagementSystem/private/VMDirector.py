########################################################################
# $HeadURL: https://dirac-grid.googlecode.com/svn/BelleDIRAC/trunk/BelleDIRAC/WorkloadManagementSystem/private/VMDirector.py $
# File :   KVMDirector.py
# Author : Ricardo Graciani
########################################################################
__RCSID__ = "$Id: VMDirector.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

import DIRAC
FROM_MAIL = "dirac.project@gmail.com"


class VMDirector:
  def __init__( self, submitPool ):

    if submitPool == self.Flavor:
      self.log = DIRAC.gLogger.getSubLogger( '%sPilotDirector' % self.Flavor )
    else:
      self.log = DIRAC.gLogger.getSubLogger( '%sPilotDirector/%s' % ( self.Flavor, submitPool ) )

    self.imagesCSPath = '/DIRAC/VirtualMachines'

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
      if imageName in self.images:
        continue
      imageDict = getImageDict( imageName )
      if not imageDict['OK']:
        return imageDict
      if self.Flavor <> imageDict['Flavor']:
        continue
      imageCSPath = '%s/%s' % ( self.imagesCSPath, imageName )
      self.images[imageName] = {}
      self.images[imageName]['RequirementsDict'] = imageDict['Requirements']
      self.images[imageName]['MaxInstances'] = DIRAC.gConfig.getValue( '%s/MaxInstances' % imageCSPath, 0 )
      self.images[imageName]['CPUPerInstance'] = DIRAC.gConfig.getValue( '%s/CPUPerInstance' % imageCSPath, 86400 )
      self.images[imageName]['Priority'] = DIRAC.gConfig.getValue( '%s/Priority' % imageCSPath, 1. )

      print DIRAC.DEncode.encode( imageDict['Requirements'] )


