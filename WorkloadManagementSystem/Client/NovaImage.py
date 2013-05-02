########################################################################
# $HeadURL$
# File :   NovaImage.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################

# DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.Nova11 import NovaClient

__RCSID__ = '$Id: $'

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
    #Get CloudEndpoint on submission time
    self.__endpoint = endpoint
    if not self.__endpoint:
      self.__errorStatus = "Can't find endpoint for image %s" % self.__DIRACImageName
      self.log.error( self.__errorStatus )
      return
    # Get the CPUTime of the image to put on the VMs /LocalSite/CPUTime
    self.__cpuTime = self.__getCSImageOption( "cpuTime" ) 
    if not self.__cpuTime:
      self.__cpuTime = 1800
    # Get the contextualization method (adhoc/ssh) of the image
    self.__contextMethod = self.__getCSImageOption( "contextMethod" ) 
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
    # Site name (temporaly at Endpoint, but this sould be get it from Resources LHCbDIRAC like scheme)
    self.__siteName = self.__getCSCloudEndpointOption( "siteName" )
    if not self.__siteName:
      self.__errorStatus = "Can't find the siteName for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # scheduling policy of the endpoint elastic/static
    self.__vmPolicy = self.__getCSCloudEndpointOption( "vmPolicy" )
    if not ( self.__vmPolicy == 'elastic' or self.__vmPolicy == 'static' ): 
      self.__errorStatus = "Can't find valid vmPolicy (elastic/static) for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # CloudDriver to be passed to VM to match cloud manager depenadant operations
    self.__cloudDriver = self.__getCSCloudEndpointOption( "driver" )
    if not self.__cloudDriver:
      self.__errorStatus = "Can't find the driver for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # creating driver for connection to the endpoint and check connection
    self.__clinova = NovaClient(self.__osAuthURL, self.__osUserName, self.__osPasswd, self.__osTenantName, self.__osBaseURL, self.__osServiceRegion)
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
      self.__vmCertPath = self.__getCSImageOption( "vmCertPath" )
      if not self.__vmCertPath:
        self.__errorStatus = "Can't find the vmCertPath for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      self.__vmKeyPath = self.__getCSImageOption( "vmKeyPath" )
      if not self.__vmKeyPath:
        self.__errorStatus = "Can't find the vmKeyPath for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      self.__vmContextualizeScriptPath = self.__getCSImageOption( "vmContextualizeScriptPath" )
      if not self.__vmContextualizeScriptPath:
        self.__errorStatus = "Can't find the vmContextualizeScriptPath for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the cvmfs context URL
      self.__vmCvmfsContextURL = self.__getCSImageOption( "vmCvmfsContextURL" )
      if not self.__vmCvmfsContextURL:
        self.__errorStatus = "Can't find the vmCvmfsContextURL for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the specific context URL
      self.__vmDiracContextURL = self.__getCSImageOption( "vmDiracContextURL" )
      if not self.__vmDiracContextURL:
        self.__errorStatus = "Can't find the vmDiracContextURL for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the runsvdir run file forjobAgent URL
      self.__vmRunJobAgentURL = self.__getCSImageOption( "vmRunJobAgentURL" )
      if not self.__vmRunJobAgentURL:
        self.__errorStatus = "Can't find the vmRunJobAgentURL for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the runsvdir run file vmMonitorAgentURL 
      self.__vmRunVmMonitorAgentURL = self.__getCSImageOption( "vmRunVmMonitorAgentURL" )
      if not self.__vmRunVmMonitorAgentURL:
        self.__errorStatus = "Can't find the vmRunVmMonitorAgentURL for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the runsvdir run.log file forjobAgent URL 
      self.__vmRunLogJobAgentURL = self.__getCSImageOption( "vmRunLogJobAgentURL" )
      if not self.__vmRunLogJobAgentURL:
        self.__errorStatus = "Can't find the vmRunLogJobAgentURL for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # the runsvdir run.log file vmMonitorAgentURL 
      self.__vmRunLogVmMonitorAgentURL = self.__getCSImageOption( "vmRunLogVmMonitorAgentURL" )
      if not self.__vmRunLogVmMonitorAgentURL:
        self.__errorStatus = "Can't find the vmRunLogVmMonitorAgentURL for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # cvmfs http proxy:
      self.__cvmfs_http_proxy = self.__getCSCloudEndpointOption( "CVMFS_HTTP_PROXY" )
      if not self.__cvmfs_http_proxy:
        self.__errorStatus = "Can't find the CVMFS_HTTP_PROXY for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

    ## Additional Network pool
    self.__osIpPool = self.__getCSImageOption( "vmOsIpPool" )
    if not self.__osIpPool:
      self.__osIpPool = 'NO'

  def __getCSImageOption( self, option, defValue = "" ):
    """
    Following we can see that every CSImageOption are related with the booting image
    """
    return gConfig.getValue( "/Resources/VirtualMachines/Images/%s/%s" % ( self.__DIRACImageName, option ), defValue )

  def __getCSCloudEndpointOption( self, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( self.__endpoint, option ), defValue )

  def startNewInstance( self, instanceType ):
    """
    Wrapping the image creation
    """
    if self.__errorStatus:
      return S_ERROR( self.__errorStatus )
    self.log.info( "Starting new instance for image: %s; to endpoint %s DIRAC driver of nova endpoint" % ( self.__bootImageName, self.__endpoint ) )
    request = self.__clinova.create_VMInstance( self.__bootImageName, self.__contextMethod, instanceType, self.__bootImage, self.__osIpPool )
    if request.returncode != 0:
      self.__errorStatus = "Can't create instance for boot image: %s at server %s and Auth URL: %s \n%s" % (self.__bootImageName, self.__osBaseURL, self.__osAuthURL, request.stderr)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request )

  def contextualizeInstance( self, uniqueId, public_ip ):
    """
    Wrapping the contextualization
    With ssh method, contextualization is asyncronous operation
    """
    if self.__contextMethod =='ssh':
      request = self.__clinova.contextualize_VMInstance( uniqueId, public_ip, self.__contextMethod, self.__vmCertPath, self.__vmKeyPath, self.__vmContextualizeScriptPath, self.__vmRunJobAgentURL, self.__vmRunVmMonitorAgentURL, self.__vmRunLogJobAgentURL, self.__vmRunLogVmMonitorAgentURL, self.__vmCvmfsContextURL, self.__vmDiracContextURL , self.__cvmfs_http_proxy, self.__siteName, self.__cloudDriver, self.__cpuTime )
      if request.returncode != 0:
        self.__errorStatus = "Can't contextualize VM id %s at endpoint %s: %s" % (uniqueId, self.__endpoint, request.stderr)
        self.log.error( self.__errorStatus )
        return S_ERROR( self.__errorStatus )

    return S_OK( uniqueId )

  def getInstanceStatus( self, uniqueId ):
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
      self.__errorStatus = "Can't delete VM instance %s, IP %s, IpPool %s, from endpoint %s: %s" % (uniqueId, public_ip, self.__osIpPool, self.__endpoint, request.stderr)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stderr )
