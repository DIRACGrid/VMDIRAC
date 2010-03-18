########################################################################
# $HeadURL$
# File :   AmazonDirector.py
# Author : Ricardo Graciani
########################################################################
__RCSID__ = "$Id$"

from BelleDIRAC.WorkloadManagementSystem.private.VMDirector import VMDirector

class AmazonDirector( VMDirector ):
  def __init__( self, submitPool ):
    self.Flavor = 'Amazon'
    VMDirector.__init__( self, submitPool )
  
  def configure(self, csSection, submitPool ):
    """
     Here goes common configuration for Amazon Director
    """

    VMDirector.configure( self, csSection, submitPool )
    self.reloadConfiguration( csSection, submitPool )
    
    print
    print self.imagesRequirementsDict
    print

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    VMDirector.configureFromSection( self, mySection )
