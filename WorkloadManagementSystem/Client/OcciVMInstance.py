########################################################################
# $HeadURL$
# File :   OcciVMInstance.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################

import time
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from VMDIRAC.WorkloadManagementSystem.Client.OcciClient import OcciClient


class OcciVMInstance:

  def __init__( self, instanceId, URL, User, Passwd ):
    self.__errorStatus = ""
    self.log = gLogger.getSubLogger( "OcciVMInstance id:%s" % instanceId )
    self.__instanceId = instanceId
    self.__bootImageId = "unknown"
    self.__hdcImageId = "unknown"
    self.__occiURI = URL
    self.__occiUser = User
    self.__occiPasswd = Passwd
    self.__cliocci = OcciClient(self.__occiURI, self.__occiUser, self.__occiPasswd)
    #Try connection
    request = check_connection(self.__occiURI, self._occiUser, self.__occiPasswd)
    if request.returncode != 0:
      self.__errorStatus = "Can't connect to OCCI server %s\n%s" % (self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return

    request = self.__cliocci.get_image_ids_of_instance( self.__instanceId )
   
    if request.returncode != 0:
      self.__errorStatus = "Cannot find instance %s" % self.__instanceId
      self.log.error( self.__errorStatus )
      return

    (self.__bootImageId, self.__hdcImageId) = request.stdout

    self.log.info( "OCCI VM instance id %s initialized from  image(%s,%s)" % ( self.__instanceId, self.__bootImageId, self.__hdcImageId ) )
    return

  def getId( self ):
    return self.__instanceId

  def getImagesId( self ):
    return (self.__bootImageId, self.__hdcImageId)

  def getState( self ):
    request = self.__cliocci.get_status_VMinstance( self.__instanceId )
    if request.returncode != 0:
      self.__errorStatus = "Cannot get state of instance %s\n%s" % (self.__instanceId, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus)
    return S_OK( request.stdout )

  def stopInstance( self ):
    request = self.__clientocci.terminate_VMinstance( self.__instanceId )
    if request.returncode != 0:
      self.__errorStatus = "Can't delete VM instance ide %s from server %s\n%s" % (self.__instanceId, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stdout )
