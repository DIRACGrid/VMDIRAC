########################################################################
# $HeadURL$
# File :   OcciImage.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################


import time
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

from VMDIRAC.WorkloadManagementSystem.Client.OcciVMInstance import OcciVMInstance
from VMDIRAC.WorkloadManagementSystem.Client.OcciClient import OcciClient

class OcciImage:

  """
  The OCCI Image Interface (OII) provides the functionality required to use
  a standard occi cloud infrastructure.
  Authentication is provided by an occi user/password attributes
  """
  def __init__( self, bootImageName):
    self.__bootImageName = bootImageName
    self.__hdcImageName = self.__getCSImageOption( "hdcImageName" )
    self.log = gLogger.getSubLogger( "OII (boot,hdc): (%s,%s) " % ( bootImageName, self.__hdcImageName ) )
# __instances list not used now 
    self.__instances = []
    self.__errorStatus = ""
    #Get CloudSite
    self.__CloudSite = self.__getCSImageOption( "CloudSite" )
    if not self.__CloudSite:
      self.__errorStatus = "Can't find CloudSite for image %s" % self.__bootImageName
      self.log.error( self.__errorStatus )
      return
    #Get OCCI server URI
    self.__occiURI = self.__getCSCloudSiteOption( "occiURI" )
    if not self.__occiURI:
      self.__errorStatus = "Can't find the server occiURI for flavor %s" % self.__CloudSite
      self.log.error( self.__errorStatus )
      return
    #Get OCCI user/password
    # user
    self.__occiUser = self.__getCSCloudSiteOption( "occiUser" )
    if not self.__occiUser:
      self.__errorStatus = "Can't find the occiUser for flavor %s" % self.__CloudSite
      self.log.error( self.__errorStatus )
      return
    # password
    self.__occiPasswd = self.__getCSCloudSiteOption( "occiPasswd" )
    if not self.__occiPasswd:
      self.__errorStatus = "Can't find the occiPasswd for flavor %s" % self.__CloudSite
      self.log.error( self.__errorStatus )
      return
    #check connection
    self.__cliocci = OcciClient(self.__occiURI, self.__occiUser, self.__occiPasswd)
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

    # dns1
    self.__DNS1 = self.__getCSCloudSiteOption( "DNS1" )
    if not self.__DNS1:
      self.__errorStatus = "Can't find the DNS1 for flavor %s" % self.__CloudSite
      self.log.error( self.__errorStatus )
      return

    # dns2
    self.__DNS2 = self.__getCSCloudSiteOption( "DNS2" )
    if not self.__DNS2:
      self.__errorStatus = "Can't find the DNS2 for flavor %s" % self.__CloudSite
      self.log.error( self.__errorStatus )
      return

    # domain
    self.__Domain = self.__getCSCloudSiteOption( "Domain" )
    if not self.__Domain:
      self.__errorStatus = "Can't find the Domain for flavor %s" % self.__CloudSite
      self.log.error( self.__errorStatus )
      return

    # URL context files:
    self.__URLcontextfiles = self.__getCSImageOption( "URLcontextfiles" )
    if not self.__URLcontextfiles:
    	self.__URLcontextfiles = "http://lhcweb.pic.es/vmendez/context/root.pub"

    # Network id
    self.__NetId = self.__getCSCloudSiteOption( "NetId" )
    if not self.__NetId:
      self.__errorStatus = "Can't find the NetId for flavor %s" % self.__CloudSite
      self.log.error( self.__errorStatus )
      return

  """
  Following we can see that every CSImageOption are related with the booting
  image, the contextualized hdc image has no asociated options
  """
  def __getCSImageOption( self, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/Images/%s/%s" % ( self.__bootImageName, option ), defValue )

  """
  One flavour can correspond to many images, get the usr, passwd, dns, domain
  URL to root.pub file, network_id and other occi server specific values
  """
  def __getCSCloudSiteOption( self, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/CloudSites/%s/%s" % ( self.__CloudSite, option ), defValue )

  """
  Prior to use, virtual machine images are uploaded to the OCCI cloud manager
  assigned an id (OII in a URI). 
  """
  def startNewInstance( self, instanceType = "small"):
    if self.__errorStatus:
      return S_ERROR( self.__errorStatus )
    self.log.info( "Starting new instance for image (boot,hdc): %s,%s" % ( self.__bootImageName, self.__hdcImageName ) )
    request = self.__cliocci.create_VMInstance( self.__bootImageName, self.__hdcImageName, instanceType, self.__bootOII, self.__hdcOII, self.__DNS1, self.__DNS2, self.__Domain, self.__URLcontextfiles, self.__NetId)
    if request.returncode != 0:
      self.__errorStatus = "Can't create instance for boot image (boot,hdc): %s/%s at server %s\n%s" % (self.__bootImageName, self.__hdcImageName, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stdout )

  """
  Simple call to terminate a VM based on its id
  """
  def stopInstance( self, VMinstanceId ):

    request = self.__cliocci.terminate_VMinstance( VMinstanceId )
    if request.returncode != 0:
      self.__errorStatus = "Can't delete VM instance ide %s from server %s\n%s" % (VMinstanceId, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stdout )


  """
  Get all instances for this image
  """
  def getAllInstances( self ):
    instances = []
    request = self.__cliocci.get_all_VMinstances( self.__bootImageName )
    if request.returncode != 0:
      self.__errorStatus = "Error while get all instances of %s from server %s\n%s" % (self.__bootImage, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    for instanceId in request.rlist:
      instances.append( OcciVMInstance ( instanceId, self.__occiURI, self.__occiUser, self.__occiPasswd ) ) 
    return instances

  """
  Get all running instances for this image
  """
  def getAllRunningInstances( self ):
    instances = []
    request = self.__cliocci.get_running_VMinstances( self.__bootImageName )
    if request.returncode != 0:
      self.__errorStatus = "Error while get the running instances of %s from server %s\n%s" % (self.__bootImage, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    for instanceId in request.rlist:
      instances.append( OcciVMInstance ( instanceId, self.__occiURI, self.__occiUser, self.__occiPasswd ) ) 
    return instances

