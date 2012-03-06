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
    #Get Flavor
    self.__Flavor = self.__getCSImageOption( "Flavor" )
    if not self.__Flavor:
      self.__errorStatus = "Can't find Flavor for image %s" % self.__bootImageName
      self.log.error( self.__errorStatus )
      return
    #Get OCCI server URI
    self.__occiURI = self.__getCSImageOption( "occiURI" )
    if not self.__occiURI:
      self.__errorStatus = "Can't find the server occiURI for image %s" % self.__bootImageName
      self.log.error( self.__errorStatus )
      return
    #Get OCCI user/password
    # user
    self.__occiUser = self.__getCSImageOption( "occiUser" )
    if not self.__occiUser:
      self.__errorStatus = "Can't find User for occi server %s" % self.__occiURI
      self.log.error( self.__errorStatus )
      return
    # password
    self.__occiPasswd = self.__getCSImageOption( "occiPasswd" )
    if not self.__occiPasswd:
      self.__errorStatus = "Can't find Passwd for occi server %s" % self.__occiURI
      self.log.error( self.__errorStatus )
      return
    #check connection
    self.__cliocci = OcciClient(self.__occiURI, self.__occiUser, self.__occiPasswd)
    request = self.__cliocci.check_connection()
    if request.returncode != 0:
      self.__errorStatus = "Can't connect to OCCI server %s/n%s" % (self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return

    if not self.__errorStatus:
      self.log.info( "Available OCCI server  %s" % self.__occiURI )

    #Get the boot Occi Image Id (OII) from URI server
    self.__bootOII = self.__cliocci.get_image_id( self.__bootImageName )
    if request.returncode != 0:
      self.__errorStatus = "Can't get the boot image id for %s from server %s/n%s" % (self.__bootImageName, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return

    #Get the hdc Occi Image Id (OII) from URI server
    self.__hdcOII = self.__cliocci.get_image_id( self.__hdcImageName )
    if request.returncode != 0:
      self.__errorStatus = "Can't get the contextual image id for %s from server %s/n%s" % (self.__hdcImageName, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return

    # dns1
    self.__occiDNS1 = self.__getCSImageOption( "DNS1" )
    if not self.__DNS1:
      self.__errorStatus = "Can't find DNS1 in Flavor %s" % self.__Flavor
      self.log.error( self.__errorStatus )
      return

    # dns2
    self.__occiDNS2 = self.__getCSImageOption( "DNS2" )
    if not self.__DNS2:
      self.__errorStatus = "Can't find DNS2 in Flavor %s" % self.__Flavor
      self.log.error( self.__errorStatus )
      return

    # domain
    self.__occiDomain = self.__getCSImageOption( "Domain" )
    if not self.__Domain:
      self.__errorStatus = "Can't find Domain in Flavor %s" % self.__Flavor
      self.log.error( self.__errorStatus )
      return

    # URL context files:
    self.__occiURLcontextfiles = self.__getCSImageOption( "URLcontextfiles" )
    if not self.__URLcontextfiles:
    	self.__occiURLcontextfiles = "http://lhcweb.pic.es/vmendez/context/root.pub http://lhcweb.pic.es/vmendez/context/e2fsprogs-1.41.14.tar.gz http://lhcweb.pic.es/vmendez/context/expat-2.0.1.tar.gz http://lhcweb.pic.es/vmendez/context/gettext-0.18.1.1.tar.gz http://lhcweb.pic.es/vmendez/context/git-1.7.7.2.tar.gz"

    # Network id
    self.__occiNetId = self.__getCSImageOption( "NetId" )
    if not self.__NetId:
      self.__errorStatus = "Can't find NetId in Flavor %s" % self.__Flavor
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
  def __getCSFlavorOption( self, option, defValue = "" ):
    return gConfig.getValue( "/Resources/VirtualMachines/Flavors/%s/%s" % ( self.__Flavor, option ), defValue )

  """
  Prior to use, virtual machine images are uploaded to the OCCI cloud manager
  assigned an id (OII in a URI). 
  """
  def startNewInstance( self, instanceType = "small"):
    if self.__errorStatus:
      return S_ERROR( self.__errorStatus )
    self.log.info( "Starting new instance for image (boot,hdc): %s" % ( self.__bootImageName, self.__hdcImageName ) )
    request = self.__cliocci.create_VMInstance( self.__bootImageName, self.__hdcImageName, instanceType, self.__bootOII, self.__hdcOII, self.__occiDNS1, self.__occiDNS2, self.__Domain, self.__occiURLcontextfiles, self.__occiNetId)
    if request.returncode != 0:
      self.__errorStatus = "Can't create instance for boot image (boot,hdc): %s at server %s/n%s" % (self.__bootImageName, self.__hdcImageName, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stdout )

  """
  Simple call to terminate a VM based on its id
  """
  def stopInstance( self, VMinstanceId ):

    request = self.__clientocci.terminate_VMinstance( VMinstanceId )
    if request.returncode != 0:
      self.__errorStatus = "Can't delete VM instance ide %s from server %s/n%s" % (VMinstanceId, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    return S_OK( request.stdout )


  """
  Get all instances for this image
  """
  def getAllInstances( self ):
    instances = []
    request = self.__clientocci.get_all_VMinstances( self.__bootImageName )
    if request.returncode != 0:
      self.__errorStatus = "Error while get all instances of %s from server %s/n%s" % (self.__bootImage, self.__occiURI, request.stdout)
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
    request = self.__clientocci.get_running_VMinstances( self.__bootImageName )
    if request.returncode != 0:
      self.__errorStatus = "Error while get the running instances of %s from server %s/n%s" % (self.__bootImage, self.__occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return S_ERROR( self.__errorStatus )

    for instanceId in request.rlist:
      instances.append( OcciVMInstance ( instanceId, self.__occiURI, self.__occiUser, self.__occiPasswd ) ) 
    return instances

