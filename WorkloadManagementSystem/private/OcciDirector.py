########################################################################
# $HeadURL$
# File :   OcciDirector.py
# Author : Victor Mendez
########################################################################

from DIRAC import S_OK, S_ERROR
from DIRACVM.WorkloadManagementSystem.private.VMDirector import VMDirector
from DIRACVM.WorkloadManagementSystem.Client.OcciImage import OcciImage

class OcciDirector( VMDirector ):
  def __init__( self, submitPool ):
    self.Flavor = 'Occi'
    VMDirector.__init__( self, submitPool )

  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for Occi Director
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
    oima = OcciImage( imageName )
    result = oima.startNewInstance()
    if not result[ 'OK' ]:
      return result
    idInstance = result['Value']
    return S_OK( idInstance )
