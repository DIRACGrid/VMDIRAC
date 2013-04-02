# $HeadURL$
"""
  NovaImage
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

  def __init__( self, imageName, endpoint ):
    """
    The NovaImage provides the functionality required to use
    a OpenStack cloud infrastructure, with NovaAPI DIRAC driver
    Authentication is provided by user/password attributes
    """
    
    self.log       = gLogger.getSubLogger( 'NovaImage %s: ' % imageName )
    self.imageName = imageName
    self.endpoint  = endpoint 
        
    self.__novaConfig  = NovaConfiguration( endpoint )
    self.__imageConfig = ImageConfiguration( imageName )      
           
    self.__clinova   = None

  def connectNova( self ):

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

    self.__clinova = NovaClient( user, secret, self.__novaConfig.config(), self.__imageConfig.config() )

    result = self.__clinova.check_connection()
    if not result[ 'OK' ]:
      self.log.error( "connectNova" )
      self.log.error( result[ 'Message' ] )
      
    return result
   
  def startNewInstance( self ):
    """
    Wrapping the image creation
    """
    
    _msg = "Starting new instance for image: %s; to endpoint %s DIRAC driver of nova endpoint"
    self.log.info( _msg % ( self.__imageConfig[ 'bootImageName' ], self.endpoint ) )
    
    result = self.__clinova.create_VMInstance( self.__imageConfig.config() )

    if not result[ 'OK' ]:
      self.log.error( "startNewInstance" )
      self.log.error( result[ 'Message' ] )
    return result

  def contextualizeInstance( self, uniqueId, public_ip ):
    """
    Wrapping the contextualization
    With ssh method, contextualization is asyncronous operation
    """

    result = self.__clinova.contextualize_VMInstance( uniqueId, public_ip )
    
    if not result[ 'OK' ]:
      self.log.error( "contextualizeInstance: %s, %s" % ( uniqueId, public_ip ) )
      self.log.error( result[ 'Message' ] )
      return result

    return S_OK( uniqueId )

  def getInstanceStatus( self, uniqueId ):
    """
    Wrapping the get status of the uniqueId VM from the endpoint
    """
    
    result = self.__clinova.getStatus_VMInstance( uniqueId )
    
    if not result[ 'OK' ]:
      self.log.error( "getInstanceStatus: %s" % uniqueId )
      self.log.error( result[ 'Message' ] )
    
    return result  
    
  def stopInstance( self, uniqueId, public_ip ):
    """
    Simple call to terminate a VM based on its id
    """

    #request = self.__clinova.terminate_VMinstance( uniqueId, self.cloudEndpointDict[ 'osIpPool' ], public_ip )
    result = self.__clinova.terminate_VMinstance( uniqueId, public_ip )
    
    if not result[ 'OK' ]:
      self.log.error( "stopInstance: %s, %s" % ( uniqueId, public_ip ) )
      self.log.error( result[ 'Message' ] )
    
    return result
      
#    if request.returncode != 0:
#      __errorStatus = "Can't delete VM instance %s, IP %s, from endpoint %s: %s"
#      __errorStatus = __errorStatus % ( uniqueId, public_ip, 
#                                        self.endpoint, request.stderr )
#      self.log.error( __errorStatus )
#      return S_ERROR( __errorStatus )
#
#    return S_OK( request.stderr )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF