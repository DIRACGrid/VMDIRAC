#!/usr/bin/env python

import boto
import time
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

from BelleDIRAC.WorkloadManagementSystem.Client.AmazonInstance import AmazonInstance

class AmazonImage:

  """
  An EC2Service provides the functionality of Amazon EC2 that is required to use it for infrastructure.
  Authentication is provided by a public-private keypair (access_key, secret_key) which is
  labelled by key_name and associated with a given Amazon Web Services account
  """
  def __init__( self, vmName ):
    self.log = gLogger.getSubLogger( "AMI:%s" % vmName )
    self.__vmName = vmName
    self.__vmImage = False
    self.__instances = []
    self.__errorStatus = ""
    #Get Flavor
    self.__vmFlavor = self.__getCSImageOption( "Flavor" )
    if not self.__vmFlavor:
      self.__errorStatus = "Can't find Flavor for image %s" % self.__vmName
      self.log.error( self.__errorStatus )
      return
    #Get AMI Id
    self.__vmAMI = self.__getCSImageOption( "AMI" )
    if not self.__vmAMI:
      self.__errorStatus = "Can't find AMI for image %s" % self.__vmName
      self.log.error( self.__errorStatus )
      return
    #Get Amazon credentials
    # Access key
    self.__amAccessKey = self.__getCSFlavorOption( "AccessKey" )
    if not self.__amAccessKey:
      self.__errorStatus = "Can't find AccessKey for Flavor %s" % self.__vmFlavor
      self.log.error( self.__errorStatus )
      return
    # Secret key
    self.__amSecretKey = self.__getCSFlavorOption( "SecretKey" )
    if not self.__amSecretKey:
      self.__errorStatus = "Can't find SecretKey for Flavor %s" % self.__vmFlavor
      self.log.error( self.__errorStatus )
      return
    #Try connection
    try:
      self.__conn = boto.connect_ec2( self.__amAccessKey, self.__amSecretKey )
    except Exception, e:
      self.__errorStatus = "Can't connect to EC2"
      self.log.error( self.__errorStatus )
      return

    if not self.__errorStatus:
      self.log.info( "Amazon image %s initialized" % self.__vmName )

  def __getCSImageOption( self, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/Images/%s/%s" % ( self.__vmName, option ), defValue )

  def __getCSFlavorOption( self, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/Flavors/%s/%s" % ( self.__vmFlavor, option ), defValue )

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
  def startNewInstances( self, numImages = 1, instanceType = "", waitForConfirmation = False ):
    if self.__errorStatus:
      return S_ERROR( self.__errorStatus )
    if not self.__vmImage:
      self.__vmImage = self.__conn.get_image( self.__vmAMI )
    if not instanceType:
      instanceType = self.__getCSImageOption( 'InstanceType' , "m1.large" )
    self.log.info( "Starting %d new instances for AMI %s (type %s)" % ( numImages,
                                                                        self.__vmAMI,
                                                                        instanceType ) )
    reservation = self.__vmImage.run( min_count = numImages,
                                      max_count = numImages,
                                      instance_type = instanceType )
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
      idList.append( AmazonInstance( instance.id, self.__amAccessKey, self.__amSecretKey ) )
    return S_OK( idList )


  """
  Simple call to terminate a VM based on its id
  """
  def stopInstances( self, instancesList ):
    if type( instancesList ) in ( types.StringType, types.UnicodeType ):
      instancesList = [ instancesList ]
    print self.__conn.terminate_instances( instancesList )

  """
  Get all instances for this image
  """
  def getAllInstances( self ):
    instances = []
    for res in self.__conn.get_all_instances():
      for instance in res.instances:
        if instance.image_id == self.__vmAMI:
          instances.append( AmazonInstance( instance.id, self.__amAccessKey, self.__amSecretKey ) )
    return instances

  """
  Get all running instances for this image
  """
  def getAllRunningInstances( self ):
    instances = []
    for res in self.__conn.get_all_instances():
      for instance in res.instances:
        if instance.image_id == self.__vmAMI:
          instance.update()
          if instance.state == u'running':
            instances.append( AmazonInstance( instance.id, self.__amAccessKey, self.__amSecretKey ) )
    return instances
