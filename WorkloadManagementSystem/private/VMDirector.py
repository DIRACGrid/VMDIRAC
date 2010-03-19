########################################################################
# $HeadURL: https://dirac-grid.googlecode.com/svn/BelleDIRAC/trunk/BelleDIRAC/WorkloadManagementSystem/private/VMDirector.py $
# File :   KVMDirector.py
# Author : Ricardo Graciani
########################################################################
__RCSID__ = "$Id: VMDirector.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

import DIRAC
FROM_MAIL          = "dirac.project@gmail.com"


class VMDirector:
  def __init__( self, submitPool ):

    if submitPool == self.Flavor:
      self.log = DIRAC.gLogger.getSubLogger('%sPilotDirector' % self.Flavor)
    else:
      self.log = DIRAC.gLogger.getSubLogger( '%sPilotDirector/%s' % (self.Flavor, submitPool ) )

    self.imagesCSPath = '/DIRAC/VirtualMachines'

    self.errorMailAddress     = DIRAC.errorMail
    self.alarmMailAddress     = DIRAC.alarmMail
    self.mailFromAddress      = FROM_MAIL
    

  def configure(self, csSection, submitPool ):
    """
     Here goes common configuration for Amazon Director
    """
    self.images = []
    self.imagesRequirementsDict = {}

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

  def reloadConfiguration(self, csSection, submitPool):
    """
     Common Configuration can be overwriten for each GridMiddleware
    """
    mySection   = csSection+'/'+self.Flavor
    self.configureFromSection( mySection )
    """
     And Again for each SubmitPool
    """
    mySection   = csSection+'/'+submitPool
    self.configureFromSection( mySection )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    self.log.debug( 'Configuring from %s' % mySection )
    self.errorMailAddress     = DIRAC.gConfig.getValue( mySection+'/ErrorMailAddress'     , self.errorMailAddress )
    self.alarmMailAddress     = DIRAC.gConfig.getValue( mySection+'/AlarmMailAddress'     , self.alarmMailAddress )
    self.mailFromAddress      = DIRAC.gConfig.getValue( mySection+'/MailFromAddress'      , self.mailFromAddress )


    requestedImages        = DIRAC.gConfig.getValue( mySection+'/Images', self.images )
    definedImages = DIRAC.gConfig.getSections( self.imagesCSPath )

    if definedImages['OK']:
      for image in definedImages['Value']:
        if image in requestedImages and image not in self.images:
          imageCSPath = '%s/%s' % ( self.imagesCSPath, image )
          if self.Flavor in DIRAC.gConfig.getValue( '%s/Flavors' % imageCSPath , []):
            imageRequirementsDict = DIRAC.gConfig.getOptionsDict('%s/Requirements' % imageCSPath )
            if imageRequirementsDict['OK']:
              self.images.append(image)
              self.imagesRequirementsDict[image] = imageRequirementsDict['Value']

              print imageRequirementsDict['Value']
              print DIRAC.DEncode.encode( imageRequirementsDict['Value'] )

    
