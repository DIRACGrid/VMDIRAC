# $HeadURL$

import boto
from boto.ec2.regioninfo import RegionInfo
import time
import types
from urlparse import urlparse

# DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.AmazonInstance import AmazonInstance

__RCSID__ = '$ID: $'

class AmazonImage:
  """
  An EC2Service provides the functionality of Amazon EC2 that is required to use it for infrastructure.
  Authentication is provided by a public-private keypair (access_key, secret_key) which is
  labelled by key_name and associated with a given Amazon Web Services account
  """
  
  def __init__( self, vmName, endpoint ):
    
    self.log = gLogger.getSubLogger( "AMI:%s" % vmName )
    
    self.__vmName      = vmName
    self.__vmImage     = False
    self.__instances   = []
    self.__errorStatus = ""
    #Get CloudEndpoint free slot on submission time
    self.__endpoint    = endpoint
    
    #FIXME: do we need to return ??
    #FIXME: DIRACImageName ??
    if not self.__endpoint:
      self.__errorStatus = "Can't find endpoint for image %s" % self.__DIRACImageName
      self.log.error( self.__errorStatus )
      return
    
    #Get AMI Id
    self.__vmAMI = self.__getCSImageOption( "AMI" )
    if not self.__vmAMI:
      self.__errorStatus = "Can't find AMI for image %s" % self.__vmName
      self.log.error( self.__errorStatus )
      return
    
    #Get Max allowed price
    self.__vmMaxAllowedPrice = self.__getCSImageOption( "MaxAllowedPrice", 0.0 )
    #Get Amazon credentials
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
      self.__errorStatus = "Can't find RegionName for Endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return

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
    except Exception, e:
      self.__errorStatus = "Can't connect to EC2: " + str(e)
      self.log.error( self.__errorStatus )
      raise

    #FIXME: we will never reach a point where __errorStatus has anything
    if not self.__errorStatus:
      self.log.info( "Amazon image %s initialized" % self.__vmName )

  def getKeys(self):
    print keys
    return keys

  def __getCSImageOption( self, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/Images/%s/%s" % ( self.__vmName, option ), defValue )

  def __getCSCloudEndpointOption( self, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( self.__endpoint, option ), defValue )

  def startNewInstances( self, numImages = 1, instanceType = "", waitForConfirmation = False, 
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
      
    if forceNormalInstance or not self.__vmMaxAllowedPrice:
      return self.__startNormalInstances( numImages, instanceType, waitForConfirmation )
    
    self.log.info( "Requesting spot instances" )
    return self.__startSpotInstances( numImages, instanceType, waitForConfirmation )


  def __startNormalInstances( self, numImages, instanceType, waitForConfirmation ):
    self.log.info( "Starting %d new instances for AMI %s (type %s)" % ( numImages,
                                                                        self.__vmAMI,
                                                                        instanceType ) )
    if not self.__vmImage:
      self.__vmImage = self.__conn.get_image( self.__vmAMI )
    try:
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
            self.log.error( "New instance terminated while starting", "AMI: %s" % self.__vmAMI )
            continue
          self.log.info( "Sleeping for 10 secs for instance %s (current state %s)" % ( instance, instance.state ) )
          time.sleep( 10 )
          instance.update()
        if instance.state != u'terminated':
          self.log.info( "Instance %s started" % instance.id )
      idList.append( instance.id )
    return S_OK( idList )

  def __startSpotInstances( self, numImages, instanceType, waitForConfirmation ):
    self.log.info( "Starting %d new spot instances for AMI %s (type %s)" % ( numImages,
                                                                        self.__vmAMI,
                                                                        instanceType ) )
    try:
      spotInstanceRequests = self.__conn.request_spot_instances( price = "%f" % self.__vmMaxAllowedPrice,
                                                        image_id = self.__vmAMI,
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
    self.__conn.terminate_instances( instancesList )

  def getAllInstances( self ):
    """
    Get all instances for this image
    """
    instances = []
    for res in self.__conn.get_all_instances():
      for instance in res.instances:
        if instance.image_id == self.__vmAMI:
          instances.append( AmazonInstance( instance.id, self.__amAccessKey, self.__amSecretKey, self.__RegionName, self.__EndpointURL ) )
    return instances

  def getAllRunningInstances( self ):
    """
    Get all running instances for this image
    """
    instances = []
    for res in self.__conn.get_all_instances():
      for instance in res.instances:
        if instance.image_id == self.__vmAMI:
          instance.update()
          if instance.state == u'running':
            instances.append( AmazonInstance( instance.id, self.__amAccessKey, self.__amSecretKey, self.__RegionName, self.__EndpointURL ) )
    return instances
  
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
