# $HeadURL$

import boto
from boto.ec2.regioninfo import RegionInfo
from urlparse import urlparse

# DIRAC
from DIRAC import gLogger

__RCSID__ = '$Id: $'

class AmazonInstance:

  def __init__( self, instanceId, accessKey, secretKey, regionName, endpointURL ):
    
    self.log = gLogger.getSubLogger( "AIN:%s" % instanceId )
    
    self.__errorStatus   = ""
    self.__instanceId    = instanceId
    self.__amInstance    = None
    self.__amReservation = None
    self.__imageId       = "unknown"
    self.__amAccessKey   = accessKey
    self.__amSecretKey   = secretKey
    self.__RegionName    = regionName
    self.__EndpointURL   = endpointURL
    
    #Try connection
    try:
      _debug = 1
      url = urlparse(self.__EndpointURL)
      _endpointHostname = url.hostname
      _port = url.port
      _path = url.path
      _regionName = self.__RegionName
      _region = RegionInfo(name=_regionName, endpoint=_endpointHostname)
      self.__conn = boto.connect_ec2(aws_access_key_id = self.__amAccessKey,
                       aws_secret_access_key = self.__amSecretKey,
                       is_secure = False,
                       region = _region,
                       path = _path,
                       port = _port,
                       debug = _debug)
    except Exception, e:
      self.__errorStatus = "Can't connect to EC2: " + str(e)
      self.log.error( self.__errorStatus )
      return

    reservations = self.__conn.get_all_instances( [ instanceId ] )
    for res in reservations:
      for instance in res.instances:
        if instance.id == self.__instanceId:
          self.__amInstance    = instance
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
