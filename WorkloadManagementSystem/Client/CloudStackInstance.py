########################################################################
# $HeadURL$
# File :   CloudStackInstance.py
# Author : Victor Fernandez ( victormanuel.fernandez@usc.es )
########################################################################

import time
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from VMDIRAC.WorkloadManagementSystem.Client.CloudStackClient import CloudStackClient

class CloudStackInstance:

  def __init__( self, instanceId, URL, accessKey, secretKey ):
    self.__errorStatus = ""
    self.log = gLogger.getSubLogger( "CloudStackInstance id:%s" % instanceId )
    self.__instanceId = instanceId
    self.__csInstance = False
    self.__imageId = "unknown"
    self.__cloudStackURI = URL
    self.__csAccessKey = accessKey
    self.__csSecretKey = secretKey
    self.__cliCloudStack = CloudStackClient( self.__cloudStackURI, self.__csAccessKey, self.__csSecretKey )
    #Try connection
    request = check_connection( self.__cloudStackURI, self.__csAccessKey, self.__csSecretKey )
    if request.returncode != 0:
      self.__errorStatus = "Can't connect to CloudStack server %s\n%s" % ( self.__cloudStackURI, request.stdout )
      self.log.error( self.__errorStatus )
      return

    request = self.__cliCloudStack.get_image_ids_of_instance( self.__instanceId )

    if request.returncode != 0:
      self.__errorStatus = "Cannot find instance %s" % self.__instanceId
      self.log.error( self.__errorStatus )
      return

    self.__csInstance = request.stdout

    self.log.info( "CloudStack VM instance id %s initialized from  image(%s)" % ( self.__instanceId, self.__csInstance ) )
    return

  def getId( self ):
    return self.__instanceId

  def getImageId( self ):
    return self.__imageId

  def getState( self ):
    request = self.__cliCloudStack.get_status_VMinstance( self.__instanceId )
    if request.returncode != 0:
      self.__errorStatus = "Cannot get state of instance %s\n%s" % ( self.__instanceId, request.stdout )
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )
    return S_OK( request.stdout )

  def stopInstance( self ):
    request = self.__cliCloudStack.terminate_VMinstance( self.__instanceId )
    if request.returncode != 0:
      self.__errorStatus = "Can't delete VM instance ide %s from server %s\n%s" % ( self.__instanceId, self.__cloudStackURI, request.stdout )
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stdout )

