########################################################################
# $HeadURL$
# File :   AmazonDirector.py
# Author : Ricardo Graciani
########################################################################

from DIRAC import S_OK
from VMDIRAC.Resources.Cloud.VMDirector                  import VMDirector
from VMDIRAC.WorkloadManagementSystem.Client.AmazonImage import AmazonImage

__RCSID__ = "$Id: AmazonDirector.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

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

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    VMDirector.configureFromSection( self, mySection )

  def _submitInstance( self, imageName, workDir, instanceID ):
    """
      Real backend method to submit a new Instance of a given Image
    """
    ami = AmazonImage( imageName )
    result = ami.startNewInstances()
    if not result[ 'OK' ]:
      return result
    return S_OK( result[ 'Value' ][0] )
