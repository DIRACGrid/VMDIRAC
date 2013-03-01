########################################################################
# $HeadURL$
# File :   CloudStackImage.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################

#DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

#VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.CloudStackClient   import CloudStackClient
from VMDIRAC.WorkloadManagementSystem.Client.CloudStackInstance import CloudStackInstance

__RCSID__ = '$Id: $'

class CloudStackImage:
  """
  The CloudStack Image Interface provides the functionality required to use
  a CloudStack infrastructure.
  Authentication is provided by an CloudStack SecretKey/ApiKey attributes
  """

  def __init__( self, csImageName, endpoint ):
    self.__csImageName = csImageName
    self.log = gLogger.getSubLogger( "CloudStack Image Name: (%s) " % ( csImageName ) )
    # __instances list not used now 
    self.__instances = []
    self.__errorStatus = ""
    self.__endpoint = endpoint
    if not self.__endpoint:
      self.__errorStatus = "Can't find endpoint for image %s" % self.__DIRACImageName
      self.log.error( self.__errorStatus )
      return

    #Get CloudStack server URI
    self.__CloudStackURI = self.__getCSCloudEndpointOption( "CloudServer" )
    if not self.__CloudStackURI:
      self.__errorStatus = "Can't find the server CloudStack for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return

    #Get CloudStack SecretKey/ApiKey
    # SecretKey
    self.__secretKey = self.__getCSCloudEndpointOption( "SecretKey" )
    if not self.__secretKey:
      self.__errorStatus = "Can't find the secretKey for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # apiKey
    self.__apiKey = self.__getCSCloudEndpointOption( "apiKey" )
    if not self.__apiKey:
      self.__errorStatus = "Can't find the apiKey for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return

    # get the driver
    self.__driver = self.__getCSCloudEndpointOption( "driver" )
    if not self.__driver:
      self.__errorStatus = "Can't find the driver for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return

    #Get Opntions of Zone
    # zoneid
    self.__zoneid = self.__getCSCloudSiteOptionZones( "zoneid" )
    if not self.__zoneid:
      self.__errorStatus = "Can't find the zoneid for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # clusterid
    self.__clusterid = self.__getCSCloudSiteOptionZones( "clusterid" )
    if not self.__clusterid:
      self.__errorStatus = "Can't find the clusterid for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # podid
    self.__podid = self.__getCSCloudSiteOptionZones( "podid" )
    if not self.__podid:
      self.__errorStatus = "Can't find the podid for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return

    #Get Options of service
    # hypervisor
    self.__hypervisor = self.__getCSCloudSiteOptionServiceOffering( "hypervisor" )
    if not self.__hypervisor:
      self.__errorStatus = "Can't find the hypervisor for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # template
    self.__templateid = self.__getCSCloudSiteOptionServiceOffering( "templateid" )
    if not self.__templateid:
      self.__errorStatus = "Can't find the templateid for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # serviceOffering
    self.__serviceOfferingId = self.__getCSCloudSiteOptionServiceOffering( "serviceOfferingId" )
    if not self.__serviceOfferingId:
      self.__errorStatus = "Can't find the serviceOfferingId for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # ostypeid
    self.__ostypeid = self.__getCSCloudSiteOptionServiceOffering( "ostypeid" )
    if not self.__ostypeid:
      self.__errorStatus = "Can't find the ostypeid for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # osbits
    self.__osbits = self.__getCSCloudSiteOptionServiceOffering( "osbits" )
    if not self.__osbits:
      self.__errorStatus = "Can't find the osbits for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # volumeid
    self.__volumeid = self.__getCSCloudSiteOptionServiceOffering( "volumeid" )
    if not self.__volumeid:
      self.__errorStatus = "Can't find the volumeid for CloudEndpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return

    #check connection
    self.__clicloudstack = CloudStackClient( self.__CloudStackURI, self.__secretKey, self.__apiKey )
    request = self.__clicloudstack.check_connection( self.__CloudStackURI )

    if request.returncode != 0:
      self.__errorStatus = "Can't connect to CloudStack server %s\n%s" % ( self.__CloudStackURI, request.stdout )
      self.log.error( self.__errorStatus )
      return

    if not self.__errorStatus:
      self.log.info( "Available CloudStack server  %s" % self.__CloudStackURI )

  def __getCSImageOption( self, option, defValue = "" ):
    """
    Following we can see that every CSImageOption are related with the booting
    image
    """
    return gConfig.getValue( "/Resources/VirtualMachines/Images/%s/%s" % ( self.__csImageName, option ), defValue )

  def __getCSCloudEndpointOption( self, option, defValue = "" ):
    """
    One can correspond to many images, get the SecretKey, ApiKey other CloudStack server specific values
    """
    return gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( self.__endpoint, option ), defValue )

  def __getCSCloudSiteOptionServiceOffering( self, option, defValue = "" ):
    """
    Specific options of Instance creation
    """
    return gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/serviceOffering/%s" % ( self.__endpoint, option ), defValue )

  def __getCSCloudSiteOptionZones( self, option, defValue = "" ):
    """
    Specific options of CloudStack
    """
    return gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/optionsZones/%s" % ( self.__endpoint, option ), defValue )

  def startNewInstance( self, instanceType = "small" ):
    """
    Prior to use, virtual machine images are that are in the CloudStack server. 
    """
    if self.__errorStatus:
      return S_ERROR( self.__errorStatus )
    self.log.info( "Starting new instance for image boot: %s" % ( self.__csImageName ) )
    request = self.__clicloudstack.create_VMInstance( self.__CloudStackURI,
                                                      self.__zoneid,
                                                      self.__hypervisor,
                                                      self.__templateid,
                                                      self.__templateid,
                                                      self.__serviceOfferingId,
                                                      self.__volumeid )
    if request.returncode != 0:
      self.__errorStatus = "Can't create instance for boot image: %s at server %s. ERROR: %s" % ( self.__csImageName,
                                                                                                 self.__CloudStackURI,
                                                                                                 request.stdout )
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stdout )

  def stopInstance( self, VMinstanceId ):
    """
    Simple call to terminate a VM based on its id
    """
    request = self.__clicloudstack.terminate_VMinstance( VMinstanceId )
    if request.returncode != 0:
      self.__errorStatus = "Can't delete VM instance ide %s from server %s\n%s" % ( VMinstanceId, self.__CloudStackURI, request.stdout )
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stdout )

  def getAllInstances( self ):
    """
    Get all instances for this image
    """
    instances = []
    request = self.__clicloudstack.get_all_VMinstances( self.__serviceOfferingId )
    if request.returncode != 0:
      self.__errorStatus = "Error while get all instances of %s from server %s\n%s" % ( self.__bootImage, self.__CloudStackURI, request.stdout )
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    for instanceId in request.rlist:
      instances.append( CloudStackInstance ( instanceId, self.__CloudStackURI, self.__secretKey, self.__apiKey ) )
    return instances

  def getAllRunningInstances( self ):
    """
    Get all running instances for this image
    """
    instances = []
    request = self.__clicloudstack.get_running_VMinstances( self.__csImageName )
    if request.returncode != 0:
      self.__errorStatus = "Error while get the running instances of %s from server %s\n%s" % ( self.__bootImage, self.__CloudStackURI, request.stdout )
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    for instanceId in request.rlist:
      instances.append( CloudStackInstance ( instanceId, self.__CloudStackURI, self.__secretKey, self.__apiKey ) )
    return instances

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF