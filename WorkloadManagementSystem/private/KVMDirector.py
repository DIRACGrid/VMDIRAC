########################################################################
# $HeadURL$
# File :   KVMDirector.py
# Author : Ricardo Graciani
########################################################################
__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.private.VMDirector import VMDirector

class KVMDirector( VMDirector ):
  def __init__( self, submitPool ):
    self.Flavor = 'KVM'
    VMDirector.__init__( self, submitPool )

  def configure(self, csSection, submitPool ):
    """
     Here goes common configuration for Amazon Director
    """

    VMDirector.configure( self, csSection, submitPool )
    self.reloadConfiguration( csSection, submitPool )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    VMDirector.configureFromSection( self, mySection )
