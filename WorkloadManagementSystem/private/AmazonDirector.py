########################################################################
# $HeadURL: https://dirac-grid.googlecode.com/svn/BelleDIRAC/trunk/BelleDIRAC/WorkloadManagementSystem/private/AmazonDirector.py $
# File :   AmazonDirector.py
# Author : Ricardo Graciani
########################################################################
__RCSID__ = "$Id: AmazonDirector.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

from BelleDIRAC.WorkloadManagementSystem.private.VMDirector import VMDirector

class AmazonDirector( VMDirector ):
  def __init__( self, submitPool ):
    self.Flavor = 'Amazon'
    VMDirector.__init__( self, submitPool )

  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for Amazon Director
    """

    VMDirector.configure( self, csSection, submitPool )
    self.reloadConfiguration( csSection, submitPool )

    self.log.info( '===============================================' )
    self.log.info( 'Configuration:' )
    self.log.info( '' )
    self.log.info( 'Images:' )
    if self.images:
      self.log.info( ', '.join( self.images ) )
    else:
      self.log.info( ' None' )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    VMDirector.configureFromSection( self, mySection )
