########################################################################
# $HeadURL$
# File :   CloudStackDirector.py
# Author : Victor Fernandez
########################################################################
__RCSID__ = "$Id: CloudStackDirector.py 16 2012-02-15 11:39:29Z victor.fernandez.albor@gmail.com $"

from DIRAC import S_OK, S_ERROR
from VMDIRAC.WorkloadManagementSystem.private.VMDirector import VMDirector
from VMDIRAC.WorkloadManagementSystem.Client.CloudStackImage import CloudStackImage

class CloudStackDirector( VMDirector ):
  def __init__( self, submitPool ):
    self.Flavor = 'CloudStack'
    VMDirector.__init__( self, submitPool )

  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for CloudStack Director
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
    csi = CloudStackImage( imageName )
    result = csi.startNewInstance()
    if not result[ 'OK' ]:
      return result
    return S_OK( result[ 'Value' ] )
