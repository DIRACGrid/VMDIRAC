""" StratusLabImage

  Class used by DIRAC to control virtual machine instances on StratusLab
  cloud infrastructures.  This provides just the core interface methods for
  DIRAC, the real work is done within the StratusLabClient class.

  Author: Charles Loomis
  
"""


#DIRAC
from DIRAC import gLogger, S_ERROR


# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.StratusLabClient import StratusLabClient
from VMDIRAC.WorkloadManagementSystem.Utilities.Configuration import StratusLabConfiguration, ImageConfiguration

#from stratuslab.dirac.StratusLabEndpointConfiguration import StratusLabEndpointConfiguration


class StratusLabImage( object ):
  """
  Provides interface for managing virtual machine instances of a
  particular appliance on a StratusLab cloud infrastructure.
  """

  def __init__( self, imageElementName, endpointElementName ):
    """
    Creates an instance that will manage appliances of a given type
    on a specific cloud infrastructure.  Separate instances must be
    created for different cloud infrastructures and different
    appliances.
    The configuration is validated only when the connect() method is
    called.  This method MUST be called before any of the other
    methods.

    :Parameters:
      **imageElementName** - `string`
        element name in CS:/Resources/VirtualMachines/Images describing
        the type of appliance (image) to instantiate

      **endpointElementName** - `string`
        element name in CS:/Resources/VirtualMachines/CloudEndpoint
        giving the configuration parameters for the StratusLab cloud
        endpoint

    """

    self.log = gLogger.getSubLogger( 'StratusLabImage_%s_%s: ' % ( endpointElementName, imageElementName ) )

    self._imageConfig    = ImageConfiguration( imageElementName, endpointElementName )
    self._endpointConfig = StratusLabConfiguration( endpointElementName )

    self._impl = None


  def connect(self):
    """
    Tests the connection to the StratusLab cloud infrastructure.  Validates
    the configuration and then makes a request to list active virtual
    machine instances to ensure that the connection works.

    :return: S_OK | S_ERROR
    """

    result = self._imageConfig.validate()
    self._logResult(result, 'image configuration check')
    if not result['OK']:
      return result

    result = self._endpointConfig.validate()
    self._logResult( result, 'endpoint configuration check' )
    if not result[ 'OK' ]:
      return result

    try:
      self._impl = StratusLabClient( self._endpointConfig, self._imageConfig )
    except Exception, e:
      return S_ERROR( e )

    result = self._impl.check_connection()
    return self._logResult( result, 'connect' )


  def startNewInstance( self, vmdiracInstanceID = '' ):
    """
    Create a new instance of the given appliance.  If successful, returns
    a tuple with the instance identifier (actually the node object itself)
    and the public IP address of the instance.
    The returned instance identifier (node object) must be treated as an
    opaque identifier for the instance.  It must be passed back to the other
    methods in the class without modification!

    :return: S_OK(node, public_IP) | S_ERROR
    """

    result = self._impl.create( vmdiracInstanceID )
    return self._logResult( result, 'startNewInstance' )

  def getInstanceStatus( self, instanceId ):
    """
    Given the instance ID, returns the status.

    :Parameters:
      **instanceId** - `node`
        instance ID returned by the create() method, actually a Libcloud node object

    :return: S_OK | S_ERROR
    """

    result = self._impl.status( instanceId )
    return self._logResult( result, 'getInstanceStatus: %s' % instanceId )

  def stopInstance( self, instanceId, public_ip = None ):
    """
    Destroys (kills) the given instance.  The public_ip parameter is ignored.

    :Parameters:
      **instanceId** - `node`
        instance ID returned by the create() method, actually a Libcloud node object
      **public_ip** - `string`
        ignored

    :return: S_OK | S_ERROR
    """

    result = self._impl.terminate( instanceId, public_ip )
    return self._logResult( result, 'stopInstance: %s' % instanceId )

  def contextualizeInstance( self, instanceId, public_ip ):
    """
    This method is not a regular method in the sense that is not generic at all.
    It will be called only of those VMs which need after-booting contextualisation,
    for the time being, just ssh contextualisation.

    :Parameters:
      **instanceId** - `node`
        instance ID returned by the create() method, actually a Libcloud node object
      **public_ip** - `string`
        public IP of the VM, needed for asynchronous contextualisation

    :return: S_OK(instanceId) | S_ERROR
    """

    result = self._impl.contextualize( instanceId, public_ip )
    return self._logResult( result, 'contextualizeInstance: %s, %s' % ( instanceId, public_ip ) )

  def _logResult( self, result, msg ):
    """
    Checks if the return value is an error.  If so it logs it as an error along with the
    message.  If not, it just logs a success message as 'info'.  In both cases, it
    returns the result so that it can be returned by the caller.
    """

    if not result[ 'OK' ]:
      self.log.error( msg )
      self.log.error( result[ 'Message' ] )
    else:
      self.log.info( 'OK: %s' % msg )

    return result
  
#...............................................................................
#EOF
  
