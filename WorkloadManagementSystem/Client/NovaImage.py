########################################################################
# $HeadURL$
# File :   NovaImage.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################


import time
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from VMDIRAC.WorkloadManagementSystem.Client.Nova11 import NovaClient

class NovaImage:

  def __init__( self, DIRACImageName, endpoint):
    """
    The NovaImage provides the functionality required to use
    a OpenStack cloud infrastructure, with NovaAPI DIRAC driver
    Authentication is provided by user/password attributes
    """
    self.__DIRACImageName = DIRACImageName
    self.__bootImageName = self.__getCSImageOption( "bootImageName" ) 
    self.log = gLogger.getSubLogger( "Image %s(%s): " % ( DIRACImageName, self.__bootImageName ) )
    self.__errorStatus = ""
    # Get the instance type name (openstack instance flavor size)
    self.__instanceType = self.__getCSCloudEndpointOption( "instanceType" ) 
    if not self.__instanceType:
      self.__instanceType = 'm1.small' 
    #Get CloudEndpoint on submission time
    self.__endpoint = endpoint
    if not self.__endpoint:
      self.__errorStatus = "Can't find endpoint for image %s" % self.__DIRACImageName
      self.log.error( self.__errorStatus )
      return
    # Get the contextualization method (adhoc/ssh) of the endpoint
    self.__contextMethod = self.__getCSCloudEndpointOption( "contextMethod" ) 
    if not ( self.__contextMethod == 'ssh' or self.__contextMethod == 'adhoc' ): 
      self.__errorStatus = "endpoint %s contextMethod %s not available, use adhoc or ssh" % (self.__endpoint, self.__contextMethod)
      self.log.error( self.__errorStatus )
      return
    # OpenStack base URL (not needed in most of openstack deployments which use Auth server, in this case value can be 'Auth')
    self.__osBaseURL = self.__getCSCloudEndpointOption( "osBaseURL" )
    if not self.__osBaseURL:
      self.__errorStatus = "Can't find the osBaseURL for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    #Get Auth endpoint
    self.__osAuthURL = self.__getCSCloudEndpointOption( "osAuthURL" )
    if not self.__osAuthURL:
      self.__errorStatus = "Can't find the server osAuthURL for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    #Get OpenStack user/password
    # user
    self.__osUserName = self.__getCSCloudEndpointOption( "osUserName" )
    if not self.__osUserName:
      self.__errorStatus = "Can't find the osUserName for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # password
    self.__osPasswd = self.__getCSCloudEndpointOption( "osPasswd" )
    if not self.__osPasswd:
      self.__errorStatus = "Can't find the osPasswd for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # OpenStack tenant name
    self.__osTenantName = self.__getCSCloudEndpointOption( "osTenantName" )
    if not self.__osTenantName:
      self.__errorStatus = "Can't find the osTenantName for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # OpenStack service region 
    self.__osServiceRegion = self.__getCSCloudEndpointOption( "osServiceRegion" )
    if not self.__osServiceRegion:
      self.__errorStatus = "Can't find the osServiceRegion for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # creating driver for connection to the endpoint and check connection
    self.__clinova = NovaClient(self.__osAuthURL, self.__osUserName, self.__osPasswd, self.__osTenantName, self.__osBaseURL, self,__osServiceRegion)
    request = self.__clinova.check_connection()
    if request.returncode != 0:
      self.__errorStatus = "Can't connect to OpenStack nova endpoint %s\n osAuthURL: %s\n%s" % (self.__osBaseURL, self.__osAuthURL, request.stderr)
      self.log.error( self.__errorStatus )
      return

    if not self.__errorStatus:
      self.log.info( "Available OpenStack nova endpoint  %s and Auth URL: %s" % (self.__osBaseURL, self.__osAuthURL) )

    #Get the boot OpenStack Image from URI server
    request = self.__clinova.get_image( self.__bootImageName )
    if request.returncode != 0:
      self.__errorStatus = "Can't get the boot image for %s from server %s\n and Auth URL: %s\n%s" % (self.__bootImageName, self.__osBaseURL, self.__osAuthURL, request.stderr)
      self.log.error( self.__errorStatus )
      return
    self.__bootImage = request.image

    if self.__contextMethod == 'ssh': 
      # the virtualmachine cert/key to be copy on the VM of a specific endpoint
      self.__vmCertPath = self.__getCSCloudEndpointOption( "vmCertPath" )
      if not self.__vmCertPath:
        self.__errorStatus = "Can't find the vmCertPath for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      self.__vmKeyPath = self.__getCSCloudEndpointOption( "vmKeyPath" )
      if not self.__vmKeyPath:
        self.__errorStatus = "Can't find the vmKeyPath for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the specific context path
      self.__vmSpecificContextPath = self.__getCSCloudEndpointOption( "vmSpecificContextPath" )
      if not self.__vmSpecificContextPath:
        self.__errorStatus = "Can't find the vmSpecificContextPath for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the specific context path
      self.__vmGeneralContextPath = self.__getCSCloudEndpointOption( "vmGeneralContextPath" )
      if not self.__vmGeneralContextPath:
        self.__errorStatus = "Can't find the vmGeneralContextPath for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the runsvdir run file forjobAgent 
      self.__vmRunJobAgent = self.__getCSCloudEndpointOption( "vmRunJobAgent" )
      if not self.__vmRunJobAgent:
        self.__errorStatus = "Can't find the vmRunJobAgent for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the runsvdir run file vmMonitorAgent 
      self.__vmRunVmMonitorAgent = self.__getCSCloudEndpointOption( "vmRunVmMonitorAgent" )
      if not self.__vmRunVmMonitorAgent:
        self.__errorStatus = "Can't find the vmRunVmMonitorAgent for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the runsvdir run.log file forjobAgent 
      self.__vmRunLogJobAgent = self.__getCSCloudEndpointOption( "vmRunLogJobAgent" )
      if not self.__vmRunLogJobAgent:
        self.__errorStatus = "Can't find the vmRunLogJobAgent for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the runsvdir run.log file vmMonitorAgent 
      self.__vmRunLogVmMonitorAgent = self.__getCSCloudEndpointOption( "vmRunLogVmMonitorAgent" )
      if not self.__vmRunLogVmMonitorAgent:
        self.__errorStatus = "Can't find the vmRunLogVmMonitorAgent for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # cvmfs http proxy:
      self.__cvmfs_http_proxy = self.__getCSCloudEndpointOption( "CVMFS_HTTP_PROXY" )
      if not self.__cvmfs_http_proxy:
        self.__errorStatus = "Can't find the CVMFS_HTTP_PROXY for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

    ## Additional Network pool
    self.__osIpPool = self.__getCSCloudEndpointOption( "osIpPool" )
    if not self.__osIpPool:
       self.__osIpPool = 'NO'

  def __getCSImageOption( self, option, defValue = "" ):
    """
    Following we can see that every CSImageOption are related with the booting
    image, the contextualized hdc image has no asociated options
    """
    return gConfig.getValue( "/Resources/VirtualMachines/Images/%s/%s" % ( self.__DIRACImageName, option ), defValue )

  def __getCSCloudEndpointOption( self, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( self.__endpoint, option ), defValue )

  def startNewInstance( self ):
    """
    Wrapping the image creation
    """
    if self.__errorStatus:
      return S_ERROR( self.__errorStatus )
    self.log.info( "Starting new instance for image (boot,hdc): %s,%s; to endpoint %s DIRAC driver of nova endpoint" % ( self.__bootImageName, self.__endpoint ) )
    request = self.__clinova.create_VMInstance( self.__bootImageName, self.__contextMethod, self.__instanceType, self.__bootImage, self.__osIpPool )
    if request.returncode != 0:
      self.__errorStatus = "Can't create instance for boot image (boot,hdc): %s/%s at server %s and Auth URL: %s \n%s" % (self.__bootImageName, self.__osBaseURL, self.__osAuthURL, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request )

  def contextualizeInstance( self, uniqueId, public_ip ):
    """
    Wrapping the contextualization
    With ssh method, contextualization is asyncronous operation
    """
    if self.__contextMethod =='ssh':
      request = self.__clinova.contextualize_VMInstance( public_ip, self.__vmCertPath, self.__vmKeyPath, self.__vmRunJobAgent, self.__vmRunVmMonitorAgent, self.__vmRunLogJobAgent, self.__vmRunLogVmMonitorAgent, self.__vmSpecificContextPath, self.__vmGeneralContextPath , self.__cvmfs_http_proxy )
      if request.returncode != 0:
        self.__errorStatus = "Can't contextualize VM id %s at endpoint %s: %s" % (uniqueId, self.__endpoint, request.stderr)
        self.log.error( self.__errorStatus )
        return S_ERROR( self.__errorStatus )

    return S_OK( uniqueId )

  def getInstanceStatus( uniqueId ):
    """
    Wrapping the get status of the uniqueId VM from the endpoint
    """
    request = self.__clinova.getStatus_VMInstance( uniqueId )
    if request.returncode != 0:
      self.__errorStatus = "Can't get status %s at endpoint %s: %s" % (uniqueId, self.__endpoint, request.stderr)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.status )

  def stopInstance( self, uniqueId, public_ip ):
    """
    Simple call to terminate a VM based on its id
    """

    request = self.__clinova.terminate_VMinstance( uniqueId, self.__osIpPool, public_ip )
    if request.returncode != 0:
      self.__errorStatus = "Can't delete VM instance %s from endpoint %s: %s" % (uniqueId, self.__endpoint, request.stderr)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stderr )