########################################################################
# $HeadURL$
# File :   CloudDirector.py
# Author : Victor Mendez
########################################################################

#DIRAC
from DIRAC import S_OK, S_ERROR, gConfig

#VMDIRAC
from VMDIRAC.Resources.Cloud.VMDirector                      import VMDirector
from VMDIRAC.WorkloadManagementSystem.Client.AmazonImage     import AmazonImage
from VMDIRAC.WorkloadManagementSystem.Client.CloudStackImage import CloudStackImage
from VMDIRAC.WorkloadManagementSystem.Client.NovaImage       import NovaImage
from VMDIRAC.WorkloadManagementSystem.Client.OcciImage       import OcciImage
# aqui conf automatica de modulos (adri)

__RCSID__ = '$Id: $'

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

  def _submitInstance( self, imageName, endpoint, CPUTime, instanceID ):
    """
      Real backend method to submit a new Instance of a given Image
      It has the decision logic of sumbission to the multi-endpoint, from the available from a given imageName, first approach: FirstFit 
      It checks wether are free slots by requesting status of sended VM to a cloud endpoint
    """

    endpointsPath = "/Resources/VirtualMachines/CloudEndpoints"

    driver = gConfig.getValue( "%s/%s/%s" % ( endpointsPath, endpoint, 'cloudDriver' ), "" )

    if driver == 'Amazon':
      ami = AmazonImage( imageName, endpoint )
      result = ami.startNewInstances()
      if not result[ 'OK' ]:
        return result
      idInstance = result['Value'][0]
      return S_OK( idInstance )

    if driver == 'CloudStack':
      csi = CloudStackImage( imageName , endpoint )
      result = csi.startNewInstance()
      if not result[ 'OK' ]:
        return result
      idInstance = result['Value']
      return S_OK( idInstance )


    if ( driver == 'occi-0.9' or driver == 'occi-0.8' or driver =='rocci-1.1'):
      oima   = OcciImage( imageName, endpoint )
      connOcci = oima.connectOcci()
      if not connOcci[ 'OK' ]:
        return connOcci
      result = oima.startNewInstance( CPUTime )
      if not result[ 'OK' ]:
        return result
      idInstance = result['Value']
      return S_OK( idInstance )

    if driver == 'nova-1.1':
      nima     = NovaImage( imageName, endpoint )
      connNova = nima.connectNova()
      if not connNova[ 'OK' ]:
        return connNova
      result = nima.startNewInstance( instanceID )
      if not result[ 'OK' ]:
        return result
      return S_OK( result['Value'] )


    return S_ERROR( 'Unknown DIRAC Cloud driver %s' % driver )

