########################################################################
# $HeadURL$
# File :   KVMDirector.py
# Author : Ricardo Graciani
########################################################################
__RCSID__ = "$Id: KVMDirector.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

from DIRAC import S_OK, S_ERROR
from VMDIRAC.WorkloadManagementSystem.private.VMDirector import VMDirector

class KVMDirector( VMDirector ):
  def __init__( self, submitPool ):
    self.Flavor = 'KVM'
    VMDirector.__init__( self, submitPool )

  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for KVM Director
    """

    VMDirector.configure( self, csSection, submitPool )
    self.reloadConfiguration( csSection, submitPool )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    VMDirector.configureFromSection( self, mySection )

  def _submitInstance( self, imageName, workDir ):
    """
      Real backend method to submit a new Instance of a given Image
    """
    return S_OK()
