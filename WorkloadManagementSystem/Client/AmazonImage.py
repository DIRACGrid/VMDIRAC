# $HeadURL$

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
from VMDIRAC.WorkloadManagementSystem.Utilities.Configuration import ImageConfiguration

__RCSID__ = '$ID: $'

class AmazonImage:
  """
  An EC2Service provides the functionality of Amazon EC2 that is required to use it for infrastructure.
  Authentication is provided by a public-private keypair (access_key, secret_key) which is
  labelled by key_name and associated with a given Amazon Web Services account
  """
  
  def __init__( self, endpoint ):
    
    self.log = gLogger.getSubLogger( "EC2 Endpoint:%s" % endpoint )
    
    self.__vmImage     = False
    self.__instances   = []
    self.__errorStatus = ""
    #Get CloudEndpoint free slot on submission time
    self.__endpoint    = endpoint


    # Access key
    self.__amAccessKey = self.__getCSCloudEndpointOption( "AccessKey" )
    if not self.__amAccessKey:
      self.__errorStatus = "Can't find AccessKey for Endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    
    # Secret key
    self.__amSecretKey = self.__getCSCloudEndpointOption( "SecretKey" )
    if not self.__amSecretKey:
      self.__errorStatus = "Can't find SecretKey for Endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return

    # Endpoint URL
    self.__EndpointURL = self.__getCSCloudEndpointOption( "EndpointURL" )
    if not self.__EndpointURL:
      self.__errorStatus = "Can't find endpointURL for Endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return

    # RegionName
    self.__RegionName = self.__getCSCloudEndpointOption( "RegionName" )
    if not self.__RegionName:
      self.__infoStatus = "Can't find RegionName for Endpoint %s. Default name cloudEC2 will be used." % self.__endpoint
      self.log.info( self.__infoStatus )
      self.__RegionName = 'cloudEC2'

    #Try connection
    try:
      _debug = 0
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
      if not self.isConnected():
        raise Exception
    except Exception, e:
      self.__errorStatus = "Can't connect to EC2: " + str(e)
      self.log.error( self.__errorStatus )
      raise

    if not self.__errorStatus:
      self.log.info( "Amazon image for endpoint %s initialized" % endpoint )

  def __getAMI( self, imageName):
    imageAMI = self.__getCSImageOption( imageName, "AMI" )
    if not imageAMI:
      self.__errorStatus = "Can't find AMI for image %s" % imageName
      self.log.error( self.__errorStatus )
    return imageAMI

  def __getCSImageOption( self, imageName, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/Images/%s/%s" % ( imageName, option ), defValue )

  def __getCSCloudEndpointOption( self, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( self.__endpoint, option ), defValue )

  def __getMaxAllowedPrice( self, imageName):
    price = self.__getCSImageOption( "MaxAllowedPrice", 0.0 )
    if price:
      return price
    else:
      return False


  def startNewInstances( self, imageName, numImages = 1, instanceType = "", waitForConfirmation = False, 
                         forceNormalInstance = False ):
    """
    Prior to use, virtual machine images are uploaded to Amazon's Simple Storage Soltion and
    assigned an id ( image_id ), or AMI in Amazon - speak. In one request you can launch up to max
    vms, and if min instances can't be created the call will fail.
    
    A reservation contains one or more instances (instance = vm), which each
    have their own status, IP address and other attributes
  
    VMs can have different virtualised hardware (instance_type), higher cpu/ram/disk results in an
    increased cost per hour. Valid types are (2010-02-08):
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
    """


    if self.__errorStatus:
      return S_ERROR( self.__errorStatus )

    if not instanceType:
      instanceType = self.__getCSImageOption( 'InstanceType' , "m1.large" )

    if forceNormalInstance or not self.__getMaxAllowedPrice(imageName):
      return self.__startNormalInstances( imageName, numImages, instanceType, waitForConfirmation )

    self.log.info( "Requesting spot instances" )
    return self.__startSpotInstances( imageName, numImages, instanceType, waitForConfirmation )


  def __startNormalInstances( self, imageName, numImages, instanceType, waitForConfirmation ):
    imageAMI = self.__getAMI(imageName)
    self.log.info( "Starting %d new instances for AMI %s (type %s)" % ( numImages,
                                                                        imageAMI,
                                                                        instanceType ) )
    if not self.__vmImage:
      try:
        self.__vmImage = self.__conn.get_image( imageAMI )
      except EC2ResponseError, e:
        if e.status == 400:
          errmsg = "boto connection problem! Check connection properties."
        else:
          errmsg = "boto exception: "
        self.log.error( errmsg )
        return S_ERROR( errmsg+e.body)
    try:
      self.imageConfig = ImageConfiguration( imageName ).config()
      if self.imageConfig[ 'contextMethod' ] == 'amiconfig':
        userDataPath = self.imageConfig[ 'contextConfig' ].get( 'ex_userdata', None )
        keyname  = self.imageConfig[ 'contextConfig' ].get( 'ex_keyname' , None )
        userData = ""
        with open( userDataPath, 'r' ) as userDataFile: 
          userData = ''.join( userDataFile.readlines() )
        reservation = self.__vmImage.run( min_count = numImages,
                                        max_count = numImages,
                                        user_data = userData,
                                        key_name = keyname,
                                        instance_type = instanceType ) 
      else:
        reservation = self.__vmImage.run( min_count = numImages,
                                        max_count = numImages,
                                        instance_type = instanceType )
    except Exception, e:
      return S_ERROR( "Could not start instances: %s" % str( e ) )

    idList = []
    for instance in reservation.instances:
      if waitForConfirmation:
        instance.update()
        while instance.state != u'running':
          if instance.state == u'terminated':
            self.log.error( "New instance terminated while starting", "AMI: %s" % imageAMI )
            continue
          self.log.info( "Sleeping for 10 secs for instance %s (current state %s)" % ( instance, instance.state ) )
          time.sleep( 10 )
          instance.update()
        if instance.state != u'terminated':
          self.log.info( "Instance %s started" % instance.id )
      idList.append( instance.id )
    return S_OK( idList )


# TO DO: fix checking price
  def __startSpotInstances( self, imageName, numImages, instanceType, waitForConfirmation ):
    imageAMI = self.__getAMI(imageName)
    self.log.info( "Starting %d new spot instances for AMI %s (type %s)" % ( numImages,
                                                                        imageAMI,
                                                                        instanceType ) )
    try:
      spotInstanceRequests = self.__conn.request_spot_instances( price = "%f" % self.__vmMaxAllowedPrice,
                                                        image_id = imageAMI,
                                                        count = numImages,
                                                        instance_type = instanceType )
      self.log.verbose( "Got %d spot instance requests" % len( spotInstanceRequests ) )
    except Exception, e:
      return S_ERROR( "Could not start spot instances: %s" % str( e ) )

    idList = []
    openSIRs = spotInstanceRequests
    sirIDToCheck = [ sir.id for sir in openSIRs ]
    invalidSIRs = []
    while sirIDToCheck:
      time.sleep( 10 )
      self.log.verbose( "Refreshing SIRS %s" % ", ".join( sirIDToCheck ) )
      openSIRs = self.__conn.get_all_spot_instance_requests( sirIDToCheck )
      sirIDToCheck = []
      while openSIRs:
        sir = openSIRs.pop()
        self.log.verbose( "SIR %s is in state %s" % ( sir.id, sir.state ) )
        if sir.state == u'active' and 'instance_id' in dir( sir ):
          self.log.verbose( "SIR %s has instance %s" % ( sir.id, sir.instance_id ) )
          idList.append( sir.instance_id )
        elif sir.state == u'closed':
          invalidSIRs.append( sir.id )
        else:
          sirIDToCheck.append( sir.id )

    if idList:
      return S_OK( idList )
    return S_ERROR( "Could not start any spot instance. Failed SIRs : %s" % ", ".join( invalidSIRs ) )

  def stopInstances( self, instancesList ):
    """
    Simple call to terminate a VM based on its id
    """
    if type( instancesList ) in ( types.StringType, types.UnicodeType ):
      instancesList = [ instancesList ]
    try:
      self.__conn.terminate_instances( instancesList )
    except Exception, error:
      return S_ERROR("Exception: %s" % str(error))
    return S_OK()

  # Used nowhere !
  def getAllInstances( self, imageName ):
    """
    Get all instances for this image
    """
    imageAMI = __getAMI(imageName)
    instances = []
    for res in self.__conn.get_all_instances():
      for instance in res.instances:
        if instance.image_id == imageAMI:
          instances.append( AmazonInstance( instance.id, self.__amAccessKey, self.__amSecretKey, self.__RegionName, self.__EndpointURL ) )
    return instances

  # Used nowhere !
  def getAllRunningInstances( self, imageName ):
    """
    Get all running instances for this image
    """
    imageAMI = __getAMI(imageName)
    instances = []
    for res in self.__conn.get_all_instances():
      for instance in res.instances:
        if instance.image_id == imageAMI:
          instance.update()
          if instance.state == u'running':
            instances.append( AmazonInstance( instance.id, self.__amAccessKey, self.__amSecretKey, self.__RegionName, self.__EndpointURL ) )
    return instances

  def isConnected( self ):
    if not self.__conn.get_all_images():
      return False
    return True

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
