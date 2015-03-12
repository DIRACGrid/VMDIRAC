# $HeadURL$
"""
  Amazon Image
  
  The AmazonImages provides the functionality required to use AWS EC2.
  Authentication is provided by  AccessKey/SecretKey
"""
# File   :   AmazonImage.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )

import boto
from boto.ec2.regioninfo import RegionInfo
from boto.exception import EC2ResponseError
import time
import types
from urlparse import urlparse

# DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR


# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.AmazonInstance import AmazonInstance
from VMDIRAC.WorkloadManagementSystem.Utilities.Configuration import AmazonConfiguration, ImageConfiguration

__RCSID__ = '$Id: $'

class AmazonImage:
  """
  AmazonImage class.
  """

  def __init__( self, imageName, endPoint ):
    """
    Constructor: uses AmazonConfiguration to parse the endPoint CS configuration
    and ImageConfiguration to parse the imageName CS configuration. 
    
    :Parameters:
      **imageName** - `string`
        imageName as defined on CS:/Resources/VirtualMachines/Images
        
      **endPoint** - `string`
        endPoint as defined on CS:/Resources/VirtualMachines/CloudEndpoint 
    
    """
    # logger
    self.log       = gLogger.getSubLogger( 'AmazonImage %s: ' % imageName )
    
    self.imageName = imageName
    self.endPoint  = endPoint 
    
    # their config() method returns a dictionary with the parsed configuration
    # they also provide a validate() method to make sure it is correct 
    self.__imageConfig = ImageConfiguration( imageName )    
    self.__amazonConfig  = AmazonConfiguration( endPoint )
    
    # this object will connect to the server. Better keep it private.                 
    self.__cliamazon   = None


  def connectAmazon( self ):
    """
    Method that issues the EC2 AWS connection in order 
    to validates the CS configurations.
     
    :return: S_OK | S_ERROR
    """
    
    # Before doing anything, make sure the configurations make sense
    # ImageConfiguration
    validImage = self.__imageConfig.validate()
    if not validImage[ 'OK' ]:
      return validImage
    # EndpointConfiguration
    validAmazon = self.__amazonConfig.validate()
    if not validAmazon[ 'OK' ]:
      return validAmazon

    # Get authentication configuration
    auth, accessKey, secretKey = self.__amazonConfig.authConfig()
    if not auth == 'secretaccesskey':
      self.__errorStatus = "auth not supported (currently secretaccesskey)"
      self.log.error( self.__errorStatus )
      return
   
    # Create the amazon client objects in AmazonClient
    self.__cliamazon = AmazonClient( accessKey=secretKey, accessKey=accessKey, endpointConfig=self.__amazonConfig.config(), imageConfig=self.__imageConfig.config() )

    # Check connection to the server
    result = self.__cliamazon.check_connection()
    if not result[ 'OK' ]:
      self.log.error( "connectAmazon" )
      self.log.error( result[ 'Message' ] )
    else:
      self.log.info( "Successful connection check" )
      
    return result
   
  def startNewInstance( self, vmdiracInstanceID, runningPodRequirements ):
    """
    Once the connection is stablished using the `connectAmazon` method, we can boot
    nodes. To do so, the config in __imageConfig and __amazonConfig applied to
    AmazonClient initialization is applied.
    
    :return: S_OK | S_ERROR
    """
    
    self.log.info( "Booting %s / %s" % ( self.__imageConfig.config()[ 'bootImageName' ],
                                         self.__amazonConfig.config()[ 'ex_force_auth_url' ] ) )

    result = self.__cliamazon.create_VMInstance( vmdiracInstanceID, runningPodRequirements )

    if not result[ 'OK' ]:
      self.log.error( "startNewInstance" )
      self.log.error( result[ 'Message' ] )
    return result

  def getInstanceStatus( self, uniqueId ):
    """
    Given the node ID and the current instanceName (AMI in Amazon terms), 
    returns the status. 
    
    :Parameters:
      **uniqueId** - `string`
        node ID, given by the OpenStack service       
    
    :return: S_OK | S_ERROR
    """
    
    result = self.__cliamazon.getStatus_VMInstance( uniqueId, self.imageName )
    
    if not result[ 'OK' ]:
      self.log.error( "getInstanceStatus: %s" % uniqueId )
      self.log.error( result[ 'Message' ] )
    
    return result  
    
  def stopInstance( self, uniqueId ):
    """
    Method that destroys the instance 
    
    :Parameters:
      **uniqueId** - `string`
    
    :return: S_OK | S_ERROR
    """

    result = self.__cliamazon.terminate_VMinstance( uniqueId )
    
    if not result[ 'OK' ]:
      self.log.error( "stopInstance: %s " % ( uniqueId ) )
      self.log.error( result[ 'Message' ] )
    
    return result
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
