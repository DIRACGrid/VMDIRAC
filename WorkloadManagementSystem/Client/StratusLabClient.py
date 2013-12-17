""" StratusLabClient

  Detailed implementation of the StratusLab methods for the StratusLabClient
  class. Uses the Libcloud API to connect to the StratusLab services.
  
  Author: Charles Loomis 
  
"""


import os
import tempfile

from ConfigParser import SafeConfigParser
from StringIO     import StringIO
from contextlib   import closing

# FIXME: Hack, hack, hack...
# ensures that StratusLab Libcloud driver is loaded before use
from libcloud.compute.providers import set_driver
set_driver( 'stratuslab',
            'stratuslab.libcloud.compute_driver',
            'StratusLabNodeDriver' )
from libcloud.compute.base      import NodeAuthSSHKey
from libcloud.compute.providers import get_driver

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.SshContextualize import SshContextualize


class StratusLabClient( object ):
  """ Implementation of the StratusLabImage functionality. """


  def __init__( self, endpointConfiguration, imageConfiguration ):
    """
    Initializes this class with the applianceIdentifier (Stratuslab Marketplace
    image identifier), the cloud infrastructure, and the resource requirements
    (size) of the instances.

    NOTE: This constructor will raise an exception if there is a problem with
    any of the configuration, either when creating the Libcloud driver or with
    the given parameters.

    :Parameters:
      **endpointConfiguration** - `StratusLabEndpointConfiguration`
        object containing the configuration for a StratusLab endpoint; this
        object must have been validated before calling this constructor
      **imageConfiguration** - `ImageConfiguration`
        object containing the configuration for the appliance (image) to be
        instantiated; this object must have been validated before calling
        this constructor

    """

    self.log = gLogger.getSubLogger(self.__class__.__name__)

    # Create the configuration file for the StratusLab driver.
    self.endpointConfig = endpointConfiguration.config()
    path = StratusLabClient._create_stratuslab_config( self.endpointConfig )
    try:
      # Obtain instance of StratusLab driver.
      StratusLabDriver = get_driver( 'stratuslab' )
      self._driver     = StratusLabDriver( 'unused-key', stratuslab_user_config = path )

    finally:
      try:
        os.remove(path)
      except:
        pass

    self.imageConfig    = imageConfiguration.config()

    self.image          = self._get_image( self.imageConfig[ 'bootImageName' ] )
    self.size           = self._get_size( self.imageConfig[ 'flavorName' ] )
    self.context_method = self.imageConfig[ 'contextMethod' ]
    self.context_config = self.imageConfig[ 'contextConfig' ]
    self.location       = self._get_location()


  def check_connection(self):
    """
    Checks the connection by trying to list the running machine instances (nodes).
    Note that listing the running nodes is not a standard Libcloud function.

    :return: S_OK | S_ERROR
    """

    try:
      _ = self._driver.list_nodes()
      return S_OK()
    except Exception, errmsg:
      return S_ERROR( errmsg )

  def create( self, vmdiracInstanceID = '' ):
    """
    This creates a new virtual machine instance based on the appliance identifier
    and cloud identifier defined when this object was created.

    Successful creation returns a tuple with the node object returned from the
    StratusLab Libcloud driver and the public IP address of the instance.

    NOTE: The node object should be treated as an opaque identifier by the
    called and returned unmodified when calling the other methods of this class.

    :return: S_OK( ( node, publicIP ) ) | S_ERROR
    """

    # Get ssh key.
    home = os.path.expanduser( '~' )
    ssh_public_key_path = os.path.join( home, '.ssh', 'id_dsa.pub' )

    with open(ssh_public_key_path) as f:
      pubkey = NodeAuthSSHKey(f.read())

    # Create the new instance, called a 'node' for Libcloud.
    try:
      node = self._driver.create_node( name = vmdiracInstanceID,
                                       size = self.size,
                                       location = self.location,
                                       image = self.image,
                                       auth = pubkey )
      public_ips = node.public_ips
      if len( public_ips ) > 0:
        public_ip = public_ips[ 0 ]
      else:
        public_ip = None

      return S_OK( ( node, public_ip ) )
    except Exception, e:
      return S_ERROR( e )

  def status( self, node ):
    """
    Return the state of the given node.  This converts the Libcloud states (0-4)
    to their DIRAC string equivalents.  Note that this is not a reversible mapping.

    :Parameters:
      **node** - `string`
        node object returned from the StratusLab Libcloud driver

    :return: S_OK( status ) | S_ERROR
    """

    state = node.state

    # reversed from libcloud
    STATE_MAP = { 0: 'RUNNING',
                  1: 'REBOOTING',
                  2: 'TERMINATED',
                  3: 'PENDING',
                  4: 'UNKNOWN' }

    if not state in STATE_MAP:
      return S_ERROR( 'invalid node state (%s) detected' % state )

    return S_OK( STATE_MAP[ state ] )

  def terminate( self, node, public_ip = '' ):
    """
    Terminates the node with the given instanceId.

    :Parameters:
      **node** - `node`
        node object returned from the StratusLab Libcloud driver
      **public_ip** - `string`
        parameter is ignored

    :return: S_OK | S_ERROR
    """

    try:
      if node:
        node.destroy()
      return S_OK()
    except Exception, e:
      return S_ERROR( e )

  def contextualize( self, node, public_ip ):
    """
    Contextualize the given instance.  This is currently a no-op.

    This must return S_OK(node) on success!

    :Parameters:
      **node** - `node`
        node object returned from the StratusLab Libcloud driver
      **public_ip** - `string`
        public IP assigned to the node if any

    :return: S_OK(node) | S_ERROR
    """

    self._driver.wait_until_running( [ node ] )

    context_choices = { 
                        'ssh'  : self._ssh_contextualization,
                        'none' : self._noop_contextualization
                      }

    try:
      context_function = context_choices[ self.context_method ]
    except KeyError, e:
      return S_ERROR( 'invalid context method: %s' % self.context_method )

    try:
      result = context_function( node, public_ip )
      if not result[ 'OK' ]:
        return result
    except Exception, e:
      return S_ERROR( 'error running context function: %s' % e )

    return S_OK( node )

  def _get_location( self ):
    locations = self._driver.list_locations()
    if len( locations ) > 0:
      return locations[ 0 ]
    raise Exception( 'location cannot be found' )

  def _get_image( self, applianceIdentifier ):
    images = self._driver.list_images()
    for image in images:
      if image.id == applianceIdentifier:
        return image
    raise Exception( 'image for %s cannot be found' % applianceIdentifier )

  def _get_size( self, sizeIdentifier ):
    sizes = self._driver.list_sizes()
    for size in sizes:
      if size.id == sizeIdentifier:
        return size
    raise Exception( 'size for %s cannot be found' % sizeIdentifier )

  @staticmethod
  def _create_stratuslab_config( endpoint_params ):
    """
    The argument, endpoint_params, is a dictionary with the configuration for
    the StratusLab Libcloud API.  These parameters are written to a
    temporary file within a [default] section.

    This function returns the name of the temporary file created.  The
    caller of this function is responsible for deleting this file.
    """
    parser = SafeConfigParser()
    for key, value in endpoint_params.items():
      if key.startswith( 'ex_' ):
        config_key = key.replace( 'ex_', '', 1 )
        parser.set( None, config_key, value )

    # Directly defining a 'default' section is not allowed, so
    # get around this by replacing [DEFAULT] with [default].
    with closing( StringIO() ) as mem_buffer:
      parser.write( mem_buffer )
      cfg = mem_buffer.getvalue().replace( '[DEFAULT]', '[default]', 1 )

    _, path = tempfile.mkstemp( text = True )
    with open(path, 'w') as f:
      f.write( cfg )

    return path

  def _ssh_contextualization(self, node, public_ip, cpuTime ):

    return SshContextualize().contextualise( self.imageConfig, self.endpointConfig,
                                             uniqueId = str( node ), 
                                             publicIp = public_ip,
                                             cpuTime  = cpuTime ) 

  def _noop_contextualization( self, node, public_ip ):
    return S_OK()

#...............................................................................
#EOF
