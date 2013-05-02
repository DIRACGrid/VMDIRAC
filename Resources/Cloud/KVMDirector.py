########################################################################
# $HeadURL$
# File :   KVMDirector.py
# Author : Ricardo Graciani
########################################################################

# DIRAC
from DIRAC import S_OK

#VMDIRAC
from VMDIRAC.Resources.Cloud.VMDirector import VMDirector

__RCSID__ = "$Id: KVMDirector.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

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

  def _submitInstance( self, imageName, workDir, instanceID ):
    """
      Real backend method to submit a new Instance of a given Image
    """
    return S_OK()
