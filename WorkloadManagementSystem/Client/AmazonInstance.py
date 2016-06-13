# $HeadURL$
"""
  AmazonInstance
  
  driver to EC2 AWS endpoint using boto
  
"""
# File :   AmazonInstance.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )

import os
import time 

import boto
from boto.ec2.regioninfo import RegionInfo
from urlparse import urlparse

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.BuildCloudinitScript   import BuildCloudinitScript

__RCSID__ = '$Id: $'

class AmazonClient:
  """
  AmazonClient ( v1.1 )
  """

  def __init__( self, endpointConfig, imageConfig ):
    """
    Constructor of the AmazonClient
    
    :Parameters:
      **endpointConfig** - `dict`
        dictionary with the endpoint configuration ( WMS.Utilities.Configuration.AmazonConfiguration )
      **imageConfig** - `dict`
        dictionary with the image configuration ( WMS.Utilities.Configuration.ImageConfiguration )
    
    """
    # logger
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    
    self.endpointConfig = endpointConfig
    self.imageConfig    = imageConfig
 
    # Variables needed to contact the service  
    self.__accessKey         = endpointConfig.get( 'accessKey', None )
    self.__secretKey         = endpointConfig.get( 'secretKey', None )
    self.__endpointURL       = endpointConfig.get( 'endpointURL', None )
    self.__regionName        = endpointConfig.get( 'regionName', None )
    self.__vmImage = None
    
  def check_connection( self ):
    """
    Checks connection status 
    
    :return: S_OK | S_ERROR
    """
    #Try connection
    try:
      _debug = 0
      url = urlparse(self.__endpointURL)
      _endpointHostname = url.hostname
      _port = url.port
      _path = url.path
      _regionName = self.__regionName
      _region = RegionInfo(name=_regionName, endpoint=_endpointHostname)
      self.__conn = boto.connect_ec2(aws_access_key_id = self.__accessKey,
                       aws_secret_access_key = self.__secretKey,
                       is_secure = False,
                       region = _region,
                       path = _path,
                       port = _port,
                       debug = _debug)
    except Exception, e:
      self.__errorStatus = "Can't connect to EC2: " + str(e)
      return S_ERROR( self.__errorStatus )

    return S_OK()
 
  def create_VMInstance( self, runningPodRequirements ):
    """
    This creates a VM instance for the given boot image 
    and creates a cloudinit script, taken the given parameters.
    Successful creation returns instance VM 
    
    Boots a new node on the Amazon server defined by self.endpointConfig. The
    'personality' of the node is done by self.imageConfig. Both variables are
    defined on initialization phase.
    
    The node name has the following format:
    <bootImageName><contextMethod><time>
    
    It boots the node. 

    A reservation contains one or more instances (instance = vm), which each
    have their own status, IP address and other attributes

    VMs can have different virtualised hardware (instance_type), higher cpu/ram/disk results in an
    increased cost per hour. Valid types are (2010-02-08): 
    * t2.micro - 1 cores 1 GB 64-bit, low cost/free general purposes -testing-
    * m1.small - 1 core, 1.7GB, 32-bit, 160GB
    * m1.large - 2 cores, 7.5GB, 64-bit, 850GB
    * m1.xlarge - 4 cores, 15.0GB, 64-bit, 1690GB
    * m2.2xlarge -4 cores, 34.2GB, 64-bit, 850GB
    * m2.4xlarge -8 cores, 68.4GB, 64-bit, 1690GB
    * c1.medium - 2 cores, 1.70GB, 32-bit, 350GB
    * c1.xlarge - 8 cores, 7.0GB, 64-bit, 1690GB

    http://aws.amazon.com/ec2/instance-types/
    In a typical Amazon Web Services account, a maximum of 20 instances can run at once.
    Use http://aws.amazon.com/contact-us/ec2-request/ to request an increase for your account.

    :return: S_OK( nodeID ) | S_ERROR
    """

    # Common Image Attributes
    # local variables are named in Amazon speak
    AMI               = self.imageConfig[ 'bootImageName' ]
    instanceType      = self.imageConfig[ 'flavorName' ]
    contextMethod     = self.imageConfig[ 'contextMethod' ]
    maxAllowedPrice   = self.imageConfig[ 'maxAllowedPrice' ]
    keyName           = self.imageConfig[ 'keyName' ]
    cloudDriver       = self.endpointConfig[ 'cloudDriver' ]
    vmPolicy          = self.endpointConfig[ 'vmPolicy' ]
    vmStopPolicy      = self.endpointConfig[ 'vmStopPolicy' ]
    siteName          = self.endpointConfig[ 'siteName' ]
    
    vm_name = 'DIRAC' + contextMethod + str( time.time() )[0:10]

    self.log.info( "Trying to create node" )
    self.log.verbose( "name : %s" % vm_name )
    self.log.verbose( "image : %s" % AMI )
    self.log.verbose( "instanceType : %s" % instanceType )

    if not self.__vmImage:
      try:
        self.__vmImage = self.__conn.get_image( AMI )
      except EC2ResponseError, e:
        if e.status == 400:
          errmsg = "boto connection problem! Check connection properties."
        else:
          errmsg = "boto exception: "
        self.log.error( errmsg )
        return S_ERROR( errmsg+e.body)

    if contextMethod == 'cloudinit':
      cloudinitScript = BuildCloudinitScript();
      result = cloudinitScript.buildCloudinitScript(self.imageConfig, 
			self.endpointConfig, 
			runningPodRequirements = runningPodRequirements)
      if not result[ 'OK' ]:
        return result
      composedUserdataPath = result[ 'Value' ] 
      self.log.info( "cloudinitScript : %s" % composedUserdataPath )
      with open( composedUserdataPath, 'r' ) as userDataFile: 
        userdata = ''.join( userDataFile.readlines() )
      os.remove( composedUserdataPath )

    if maxAllowedPrice is None:
      maxAllowedPrice = 0

    if maxAllowedPrice == 0:
      self.log.info( "Requesting normal instances" )
      self.log.info( "Starting new instance for AMI %s (type %s)" % ( 
                                                       AMI,
                                                       instanceType ) )
      try:
          if contextMethod == 'cloudinit':
            if ( keyName == None or keyName == 'nouse' ):
              reservation = self.__vmImage.run( min_count = 1,
                                      max_count = 1,
                                      user_data = userdata,
                                      instance_type = instanceType )
            else:
              reservation = self.__vmImage.run( min_count = 1,
                                      max_count = 1,
                                      user_data = userdata,
                                      key_name= keyName,
                                      instance_type = instanceType )
          else:
            #adhoc instance
            if ( keyName == None or keyName == 'nouse' ):
              reservation = self.__vmImage.run( min_count = numImages,
                                      max_count = 1,
                                      instance_type = instanceType )
            else:
              reservation = self.__vmImage.run( min_count = numImages,
                                      max_count = 1,
                                      key_name= keyName,
                                      instance_type = instanceType )
      except Exception, e:
        return S_ERROR( "Could not start instance: %s" % str( e ) )

      # cheking instances terminanted at starting
 
      # giving time to EC2 remote system
      time.sleep( 10 )
      for instance in reservation.instances:
        instance.update()
        if instance.state == u'terminated':
          return S_ERROR( "New instance terminated while starting", "AMI: %s" % AMI )

      return S_OK( instance.id )

    else:
      # spot instance
      self.log.info( "Requesting spot instance" )
      self.log.info( "Starting new instance for AMI %s (type %s), max price %s" % ( 
                                                       AMI,
                                                       instanceType,
                                                       maxAllowedPrice ) )

      try:
          if contextMethod == 'cloudinit':
            spotInstanceRequest = self.__conn.request_spot_instances( 
				price = "%f" % maxAllowedPrice,
                                image_id = AMI,
                                count = 1,
                                user_data = userdata,
                                key_name= keyName,
                                instance_type = instanceType )
          else:
            #adhoc instance
            spotInstanceRequest = self.__conn.request_spot_instances( 
				price = "%f" % maxAllowedPrice,
                                image_id = AMI,
                                count = 1,
                                key_name= keyName,
                                instance_type = instanceType )

          self.log.verbose( "Got spot instance request")
      except Exception, e:
          return S_ERROR( "Could not start spot instance: %s" % str( e ) )
        
      # cheking instances terminanted at starting
 
      # giving time to EC2 remote system
      time.sleep( 10 )
 
      for sir in spotInstanceRequest:
        openSIR = self.__conn.get_all_spot_instance_requests( sir.id )
        sir = openSIR.pop()
        if sir.state == u'closed':
          return S_ERROR( "New instance terminated while starting", "AMI: %s" % AMI )

      return S_OK( sir.instance_id )


  def getStatus_VMInstance( self, uniqueId, AMI ):
    """
    Get the status for a given instance with Amazon in parameter uniqueID

    :Parameters:
      **uniqueId** - `string`
        openstack node id ( not uuid ! )

    :return: S_OK( status ) | S_ERROR
    """
    instances = []
    for res in self.__conn.get_all_instances():
      for instance in res.instances:
        if instance.image_id == AMI:
          instance.update()
          return S_OK( instance.state )

    return S_ERROR( "Can not get status of instance %s AMI: %s" % (uniqueId,AMI) )

  def terminate_VMinstance( self, uniqueID ):
    """
    Simple call to terminate a VM based on its id

    :Parameters:
      **uniqueId** - `string` or list to `string`

    :return: S_OK | S_ERROR
    """

    instancesList = [ uniqueID ]
    try:
      self.__conn.terminate_instances( instancesList )
    except Exception, error:
      return S_ERROR("Exception: %s" % str(error))

    return S_OK()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
