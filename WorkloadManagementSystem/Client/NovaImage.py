# $HeadURL$
"""
  NovaImage
  
  The NovaImage provides the functionality required to use
  a OpenStack cloud infrastructure, with NovaAPI DIRAC driver
  Authentication is provided by user/password attributes
"""
# File   :   NovaImage.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )

# DIRAC
from DIRAC import gLogger, S_OK

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.Nova11           import NovaClient
from VMDIRAC.WorkloadManagementSystem.Utilities.Configuration import NovaConfiguration, ImageConfiguration

__RCSID__ = '$Id: $'

class NovaImage:
  """
  NovaImage class.
  """

  def __init__( self, imageName, endPoint ):
    """
    Constructor: uses NovaConfiguration to parse the endPoint CS configuration
    and ImageConfiguration to parse the imageName CS configuration. 
    
    :Parameters:
      **imageName** - `string`
        imageName as defined on CS:/Resources/VirtualMachines/Images
        
      **endPoint** - `string`
        endPoint as defined on CS:/Resources/VirtualMachines/CloudEndpoint 
    
    """
    # logger
    self.log       = gLogger.getSubLogger( 'NovaImage %s: ' % imageName )
    
    self.imageName = imageName
    self.endPoint  = endPoint 
    
    # their config() method returns a dictionary with the parsed configuration
    # they also provide a validate() method to make sure it is correct 
    self.__imageConfig = ImageConfiguration( imageName )    
    self.__novaConfig  = NovaConfiguration( endPoint )
    
    # this object will connect to the server. Better keep it private.                 
    self.__clinova   = None


  def connectNova( self ):
    """
    Method that issues the connection with the OpenStack server. In order to do
    it, validates the CS configurations. For the time being, we authenticate
    with user / password. It gets it and passes all information to the NovaClient
    which will check the connection.
     
    :return: S_OK | S_ERROR
    """
    
    # Before doing anything, make sure the configurations make sense
    # ImageConfiguration
    validImage = self.__imageConfig.validate()
    if not validImage[ 'OK' ]:
      return validImage
    # EndpointConfiguration
    validNova = self.__novaConfig.validate()
    if not validNova[ 'OK' ]:
      return validNova
    
    # Get authentication configuration
    user, secret = self.__novaConfig.authConfig()

    # Create the libcloud and novaclient objects in NovaClient.Nova11
    self.__clinova = NovaClient( user, secret, self.__novaConfig.config(), self.__imageConfig.config() )

    # Check connection to the server
    result = self.__clinova.check_connection()
    if not result[ 'OK' ]:
      self.log.error( "connectNova" )
      self.log.error( result[ 'Message' ] )
    else:
      self.log.info( "Successful connection check" )
      
    return result
   
  def startNewInstance( self, vmdiracInstanceID ):
    """
    Once the connection is stablished using the `connectNova` method, we can boot
    nodes. To do so, the config in __imageConfig and __novaConfig applied to
    NovaClient initialization is applied.
    
    :return: S_OK | S_ERROR
    """
    
    self.log.info( "Booting %s / %s" % ( self.__imageConfig.config()[ 'bootImageName' ],
                                         self.__novaConfig.config()[ 'ex_force_auth_url' ] ) )

    result = self.__clinova.create_VMInstance( vmdiracInstanceID )

    if not result[ 'OK' ]:
      self.log.error( "startNewInstance" )
      self.log.error( result[ 'Message' ] )
    return result

  def getInstanceStatus( self, uniqueId ):
    """
    Given the node ID, returns the status. Bear in mind, is the translation of
    the status done by libcloud and then reversed to a string. Its possible values
    are: RUNNING, REBOOTING, TERMINATED, PENDING, UNKNOWN.
    
    :Parameters:
      **uniqueId** - `string`
        node ID, given by the OpenStack service       
    
    :return: S_OK | S_ERROR
    """
    
    result = self.__clinova.getStatus_VMInstance( uniqueId )
    
    if not result[ 'OK' ]:
      self.log.error( "getInstanceStatus: %s" % uniqueId )
      self.log.error( result[ 'Message' ] )
    
    return result  
    
  def stopInstance( self, uniqueId, public_ip ):
    """
    Method that destroys the node and if using floating IPs and frees the floating
    IPs if any.
    
    :Parameters:
      **uniqueId** - `string`
        node ID, given by the OpenStack service   
      **public_ip** - `string`
        public IP of the VM, needed for some setups ( floating IP ).   
    
    :return: S_OK | S_ERROR
    """

    # FIXME: maybe it makes sense to get the public_ip in Nova11 and encapsulate it.
    # FIXME: after all, is an implementation detail.

    result = self.__clinova.terminate_VMinstance( uniqueId, public_ip )
    
    if not result[ 'OK' ]:
      self.log.error( "stopInstance: %s, %s" % ( uniqueId, public_ip ) )
      self.log.error( result[ 'Message' ] )
    
    return result

  def contextualizeInstance( self, uniqueId, public_ip, cpuTime ):
    """
    This method is not a regular method in the sense that is not generic at all.
    It will be called only of those VMs which need after-booting contextualisation,
    for the time being, just ssh contextualisation.
    On the other hand, one can say this is the most generic method because you don't
    need any particular "golden" image, like HEPiX, just whatever linux image with 
    available ssh connectivity
        
    :Parameters:
      **uniqueId** - `string`
        node ID, given by the OpenStack service   
      **public_ip** - `string`
        public IP of the VM, needed for asynchronous contextualisation
        
    
    :return: S_OK | S_ERROR
    """

    # FIXME: maybe is worth hiding the public_ip attribute and getting it on
    # FIXME: the contextualize step. 

    result = self.__clinova.contextualize_VMInstance( uniqueId, public_ip, cpuTime )
    
    if not result[ 'OK' ]:
      self.log.error( "contextualizeInstance: %s, %s" % ( uniqueId, public_ip ) )
      self.log.error( result[ 'Message' ] )
      return result

    return S_OK( uniqueId )
      
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
