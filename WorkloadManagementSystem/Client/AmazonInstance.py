#!/usr/bin/env python

import boto
import time
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class AmazonInstance:

  def __init__( self, instanceId, accessKey, secretKey ):
    self.__errorStatus = ""
    self.log = gLogger.getSubLogger( "AIN:%s" % instanceId )
    self.__instanceId = instanceId
    self.__amInstance = False
    self.__amReservation = False
    self.__imageId = "unknown"
    self.__amAccessKey = accessKey
    self.__amSecretKey = secretKey
    #Try connection
    try:
      self.__conn = boto.connect_ec2( self.__amAccessKey, self.__amSecretKey )
    except Exception, e:
      self.__errorStatus = "Can't connect to EC2"
      self.log.error( self.__errorStatus )
      return

    reservations = self.__conn.get_all_instances( [ instanceId ] )
    for res in reservations:
      for instance in res.instances:
        if instance.id == self.__instanceId:
          self.__amInstance = instance
          self.__amReservation = res

    if not self.__amInstance:
      self.__errorStatus = "Cannot find instance %s" % self.__instanceId
      self.log.error( self.__errorStatus )
      return

    self.__imageId = self.__amInstance.image_id

    if not self.__errorStatus:
      self.log.info( "Amazon instance %s (%s) initialized" % ( self.__instanceId, self.__imageId ) )

  def getId( self ):
    return self.__instanceId

  def getImageId( self ):
    return self.__imageId

  def getState( self ):
    self.__amInstance.update()
    return self.__amInstance.state

  def stopInstance( self ):
    self.__conn.terminate_instances( [ self.__imageId ] )
