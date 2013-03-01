########################################################################
# $HeadURL$
# File :   OcciImage.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################

#DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

#VMInstance operations is from VirtualMachineDB.Instances, instead of endpoint interfaced
# no more OcciVMInstnaces
#from VMDIRAC.WorkloadManagementSystem.Client.OcciVMInstance import OcciVMInstance
#occiClient dynamically below, depending on the driver
#from VMDIRAC.WorkloadManagementSystem.Client.OcciClient import OcciClient

__RCSID__ = '$Id: $'

class OcciImage:

  def __init__( self, DIRACImageName, endpoint):
    """
    The OCCI Image Interface (OII) provides the functionality required to use
    a standard occi cloud infrastructure.
    Authentication is provided by an occi user/password attributes
    """
    self.__DIRACImageName = DIRACImageName
    self.__bootImageName = self.__getCSImageOption( "bootImageName" ) 
    self.__hdcImageName = self.__getCSImageOption( "hdcImageName" )
    self.log = gLogger.getSubLogger( "OII %s(%s,%s): " % ( DIRACImageName, self.__bootImageName, self.__hdcImageName ) )
# __instances list not used now 
    self.__instances = []
    self.__errorStatus = ""
    #Get CloudEndpoint free slot on submission time
    self.__endpoint = endpoint
    if not self.__endpoint:
      self.__errorStatus = "Can't find endpoint for image %s" % self.__DIRACImageName
      self.log.error( self.__errorStatus )
      return
    #Get URI endpoint
    self.__occiURI = self.__getCSCloudEndpointOption( "occiURI" )
    if not self.__occiURI:
      self.__errorStatus = "Can't find the server occiURI for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    #Get OCCI server URI
    self.__occiURI = self.__getCSCloudEndpointOption( "occiURI" )
    if not self.__occiURI:
      self.__errorStatus = "Can't find the server occiURI for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    #Get OCCI user/password
    # user
    self.__occiUser = self.__getCSCloudEndpointOption( "occiUser" )
    if not self.__occiUser:
      self.__errorStatus = "Can't find the occiUser for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # password
    self.__occiPasswd = self.__getCSCloudEndpointOption( "occiPasswd" )
    if not self.__occiPasswd:
      self.__errorStatus = "Can't find the occiPasswd for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # get the driver
    self.__driver = self.__getCSCloudEndpointOption( "driver" )
    if not self.__driver:
      self.__errorStatus = "Can't find the driver for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return
    # check connection, depending on the driver
    if self.__driver == "occi-0.8":
      from VMDIRAC.WorkloadManagementSystem.Client.Occi08 import OcciClient
      self.__cliocci = OcciClient(self.__occiURI, self.__occiUser, self.__occiPasswd)
    if self.__driver == "occi-0.9":
      from VMDIRAC.WorkloadManagementSystem.Client.Occi09 import OcciClient
      self.__cliocci = OcciClient(self.__occiURI, self.__occiUser, self.__occiPasswd)
    else:
      self.__errorStatus = "Driver %s not supported" % self.__driver
      self.log.error( self.__errorStatus )
      return
    request = self.__cliocci.check_connection()
    if request.returncode != 0:
      self.__errorStatus = "Can't connect to OCCI server %s\n%s" % (self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return

    if not self.__errorStatus:
      self.log.info( "Available OCCI server  %s" % self.__occiURI )

    #Get the boot Occi Image Id (OII) from URI server
    request = self.__cliocci.get_image_id( self.__bootImageName )
    if request.returncode != 0:
      self.__errorStatus = "Can't get the boot image id for %s from server %s\n%s" % (self.__bootImageName, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return
    self.__bootOII = request.stdout

    #Get the hdc Occi Image Id (OII) from URI server
    request = self.__cliocci.get_image_id( self.__hdcImageName )
    if request.returncode != 0:
      self.__errorStatus = "Can't get the contextual image id for %s from server %s\n%s" % (self.__hdcImageName, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return
    self.__hdcOII = request.stdout

    # iface manual or auto (DHCP image)
    self.__iface = self.__getCSCloudEndpointOption( "iface" )
    if not self.__iface:
      self.__errorStatus = "Can't find the iface (manual/auto) for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return

    if self.__iface == 'static': 

      # dns1
      self.__DNS1 = self.__getCSCloudEndpointOption( "DNS1" )
      if not self.__DNS1:
        self.__errorStatus = "Can't find the DNS1 for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # dns2
      self.__DNS2 = self.__getCSCloudEndpointOption( "DNS2" )
      if not self.__DNS2:
        self.__errorStatus = "Can't find the DNS2 for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

      # domain
      self.__Domain = self.__getCSCloudEndpointOption( "Domain" )
      if not self.__Domain:
        self.__errorStatus = "Can't find the Domain for endpoint %s" % self.__endpoint
        self.log.error( self.__errorStatus )
        return

    # cvmfs http proxy:
    self.__CVMFS_HTTP_PROXY = self.__getCSCloudEndpointOption( "CVMFS_HTTP_PROXY" )
    if not self.__CVMFS_HTTP_PROXY:
      self.__errorStatus = "Can't find the CVMFS_HTTP_PROXY for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return

    # URL context files:
    self.__URLcontextfiles = self.__getCSImageOption( "URLcontextfiles" )
    if not self.__URLcontextfiles:
      self.__URLcontextfiles = "http://lhcweb.pic.es/vmendez/context/root.pub"

    # Network id
    self.__NetId = self.__getCSCloudEndpointOption( "NetId" )
    if not self.__NetId:
      self.__errorStatus = "Can't find the NetId for endpoint %s" % self.__endpoint
      self.log.error( self.__errorStatus )
      return

  def __getCSImageOption( self, option, defValue = "" ):
    """
    Following we can see that every CSImageOption are related with the booting
    image, the contextualized hdc image has no asociated options
    """
    return gConfig.getValue( "/Resources/VirtualMachines/Images/%s/%s" % ( self.__DIRACImageName, option ), defValue )

  def __getCSCloudEndpointOption( self, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( self.__endpoint, option ), defValue )

  def startNewInstance( self, instanceType = "small", imageDriver = "default" ):
    """
    Prior to use, virtual machine images are uploaded to the OCCI cloud manager
    assigned an id (OII in a URI). 
    """
    if self.__errorStatus:
      return S_ERROR( self.__errorStatus )
    self.log.info( "Starting new instance for image (boot,hdc): %s,%s; to endpoint %s, and driver %s" % ( self.__bootImageName, self.__hdcImageName, self.__endpoint, self.__driver ) )
    request = self.__cliocci.create_VMInstance( self.__bootImageName, self.__hdcImageName, instanceType, imageDriver, self.__bootOII, self.__hdcOII, self.__iface, self.__DNS1, self.__DNS2, self.__Domain, self.__CVMFS_HTTP_PROXY, self.__URLcontextfiles, self.__NetId)
    if request.returncode != 0:
      self.__errorStatus = "Can't create instance for boot image (boot,hdc): %s/%s at server %s\n%s" % (self.__bootImageName, self.__hdcImageName, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stdout )

  def stopInstance( self, VMinstanceId ):
    """
    Simple call to terminate a VM based on its id
    """

    request = self.__cliocci.terminate_VMinstance( VMinstanceId )
    if request.returncode != 0:
      self.__errorStatus = "Can't delete VM instance ide %s from server %s\n%s" % (VMinstanceId, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stdout )


#VMInstance operations is from VirtualMachineDB.Instances, instead of endpoint interfaced
# no more OcciVMInstances
#  def getAllInstances( self ):
#    """
#    Get all instances for this image
#    """
#    instances = []
#    request = self.__cliocci.get_all_VMinstances( self.__bootImageName )
#    if request.returncode != 0:
#      self.__errorStatus = "Error while get all instances of %s from server %s\n%s" % (self.__bootImage, self.__occiURI, request.stdout)
#      self.log.error( self.__errorStatus )
#      return S_ERROR( self.__errorStatus )
#
#    for instanceId in request.rlist:
#      instances.append( OcciVMInstance ( instanceId, self.__occiURI, self.__occiUser, self.__occiPasswd ) ) 
#    return instances

#  def getAllRunningInstances( self ):
#    """
#    Get all running instances for this image
#    """
#    instances = []
#    request = self.__cliocci.get_running_VMinstances( self.__bootImageName )
#    if request.returncode != 0:
#      self.__errorStatus = "Error while get the running instances of %s from server %s\n%s" % (self.__bootImage, self.__occiURI, request.stdout)
#      self.log.error( self.__errorStatus )
#      return S_ERROR( self.__errorStatus )
#
#    for instanceId in request.rlist:
#      instances.append( OcciVMInstance ( instanceId, self.__occiURI, self.__occiUser, self.__occiPasswd ) ) 
#    return instances
#

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
