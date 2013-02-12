########################################################################
# $HeadURL$
# File :   CloudDirector.py
# Author : Victor Mendez
########################################################################

from DIRAC import gLogger, S_OK, S_ERROR, gConfig, rootPath
from VMDIRAC.WorkloadManagementSystem.private.VMDirector import VMDirector
from VMDIRAC.WorkloadManagementSystem.Client.OcciImage import OcciImage
from VMDIRAC.WorkloadManagementSystem.Client.AmazonImage import AmazonImage
from VMDIRAC.WorkloadManagementSystem.Client.CloudStackImage import CloudStackImage
from VMDIRAC.WorkloadManagementSystem.Client.NovaImage import NovaImage
# aqui conf automatica de modulos (adri)

class CloudDirector( VMDirector ):
  def __init__( self, submitPool ):

    VMDirector.__init__( self, submitPool )

  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for Cloud Director
    """

    VMDirector.configure( self, csSection, submitPool )
    # fin Flavor esto no se usa:
    #self.reloadConfiguration( csSection, submitPool )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    VMDirector.configureFromSection( self, mySection )

  def _submitInstance( self, imageName, workDir, endpoint ):
    """
      Real backend method to submit a new Instance of a given Image
      It has the decision logic of sumbission to the multi-endpoint, from the available from a given imageName, first approach: FirstFit 
      It checks wether are free slots by requesting status of sended VM to a cloud endpoint
    """

    driver = gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( endpoint, 'driver' ), "" )

    if driver == 'Amazon':
      ami = AmazonImage( imageName, endpoint )
      result = ami.startNewInstances()
      if not result[ 'OK' ]:
        return result
      idInstance = result['Value']
      return S_OK( idInstance )

    if driver == 'CloudStack':
      csi = CloudStackImage( imageName , endpoint )
      result = csi.startNewInstance()
      if not result[ 'OK' ]:
        return result
      idInstance = result['Value']
      return S_OK( idInstance )

    if driver == 'Occi':
      instanceType = gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( endpoint, 'instanceType' ), "" )
      imageDriver = gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( endpoint, 'imageDriver' ), "" )
      oima = OcciImage( imageName, endpoint )
      result = oima.startNewInstance( instanceType, imageDriver )
      if not result[ 'OK' ]:
        return result
      idInstance = result['Value']
      return S_OK( idInstance )

    if driver == 'Nova':
      instanceType = gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( endpoint, 'instanceType' ), "" )
      nima = NovaImage( imageName, endpoint )
      result = nima.startNewInstance( instanceType )
      if not result[ 'OK' ]:
        return result
      return ( result.VMnode.id, result.public_ip )


    return DIRAC.S_ERROR( 'Unknown DIRAC Cloud driver %s' % driver )

