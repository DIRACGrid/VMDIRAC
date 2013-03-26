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

  CONTEXT_METHODS    = { 'ssh'       : [ 'vmCertPath', 'vmKeyPath', 'vmContextualizeScriptPath',
                                         'vmCvmfsContextURL', 'vmDiracContextURL',
                                         'vmRunJobAgentURL', 'vmRunVmMonitorAgentURL',
                                         'vmRunLogJobAgentURL', 'vmRunLogVmMonitorAgentURL',
                                         'vmOsIpPool' ],
                         'adhoc'     : [ 'vmOsIpPool' ],
                         'amiconfig' : [ 'vmOsIpPool' ] }

  #FIXME: mixture of upper and lower case keys.. not good at all
  NOVA_ENDPOINT_KEYS = [ 'driver', 'siteName', 'MaxEndpointInstances',
                         'osBaseURL', 'osAuthURL', 'osUserName', 'osPasswd',
                         'osTenantName', 'osServiceRegion', 'CVMFS_HTTP_PROXY' ]

  def __init__( self, imageName, endpoint ):
    """
    The NovaImage provides the functionality required to use
    a OpenStack cloud infrastructure, with NovaAPI DIRAC driver
    Authentication is provided by user/password attributes
    """
    
    #FIXME: check that bootImageName exists !
    #self.__bootImageName  = self.imageOptions.bootImageName
     
    self.log = gLogger.getSubLogger( 'NovaImage %s: ' % imageName )
    
    self.imageName = imageName
    self.endpoint  = endpoint
    
    # Full dictionary with at least self.NOVA_ENDPOINT_KEYS, or completely empty
    self.imageDict         = self.__getImageDict( imageName )
    self.cloudEndpointDict = self.__getCloudEndpointDict( endpoint )   
       
    self.__clinova   = None
    self.__bootImage = None
    
    #...........................................................................
    #...........................................................................
    #...........................................................................    
    
    
#    # OpenStack base URL (not needed in most of openstack deployments which use Auth server, in this case value can be 'Auth')
#    self.__osBaseURL = self.__getCSCloudEndpointOption( "osBaseURL" )
#    if not self.__osBaseURL:
#      self.__errorStatus = "Can't find the osBaseURL for endpoint %s" % self.__endpoint
#      self.log.error( self.__errorStatus )
#      return
#    #Get Auth endpoint
#    self.__osAuthURL = self.__getCSCloudEndpointOption( "osAuthURL" )
#    if not self.__osAuthURL:
#      self.__errorStatus = "Can't find the server osAuthURL for endpoint %s" % self.__endpoint
#      self.log.error( self.__errorStatus )
#      return
#    #Get OpenStack user/password
#    # user
#    self.__osUserName = self.__getCSCloudEndpointOption( "osUserName" )
#    if not self.__osUserName:
#      self.__errorStatus = "Can't find the osUserName for endpoint %s" % self.__endpoint
#      self.log.error( self.__errorStatus )
#      return
#    # password
#    self.__osPasswd = self.__getCSCloudEndpointOption( "osPasswd" )
#    if not self.__osPasswd:
#      self.__errorStatus = "Can't find the osPasswd for endpoint %s" % self.__endpoint
#      self.log.error( self.__errorStatus )
#      return
#    # OpenStack tenant name
#    self.__osTenantName = self.__getCSCloudEndpointOption( "osTenantName" )
#    if not self.__osTenantName:
#      self.__errorStatus = "Can't find the osTenantName for endpoint %s" % self.__endpoint
#      self.log.error( self.__errorStatus )
#      return
#    # OpenStack service region 
#    self.__osServiceRegion = self.__getCSCloudEndpointOption( "osServiceRegion" )
#    if not self.__osServiceRegion:
#      self.__errorStatus = "Can't find the osServiceRegion for endpoint %s" % self.__endpoint
#      self.log.error( self.__errorStatus )
#      return
#    # Site name (temporaly at Endpoint, but this sould be get it from Resources LHCbDIRAC like scheme)
#    self.__siteName = self.__getCSCloudEndpointOption( "siteName" )
#    if not self.__siteName:
#      self.__errorStatus = "Can't find the siteName for endpoint %s" % self.__endpoint
#      self.log.error( self.__errorStatus )
#      return
#    # CloudDriver to be passed to VM to match cloud manager depenadant operations
#    self.__cloudDriver = self.__getCSCloudEndpointOption( "driver" )
#    if not self.__cloudDriver:
#      self.__errorStatus = "Can't find the driver for endpoint %s" % self.__endpoint
#      self.log.error( self.__errorStatus )
#      return
    
    
    
#    # creating driver for connection to the endpoint and check connection
#    self.__clinova = NovaClient(self.__osAuthURL, self.__osUserName, self.__osPasswd, self.__osTenantName, self.__osBaseURL, self.__osServiceRegion)
#    request = self.__clinova.check_connection()
#    if request.returncode != 0:
#      self.__errorStatus = "Can't connect to OpenStack nova endpoint %s\n osAuthURL: %s\n%s" % (self.__osBaseURL, self.__osAuthURL, request.stderr)
#      self.log.error( self.__errorStatus )
#      return

#    if not self.__errorStatus:
#      self.log.info( "Available OpenStack nova endpoint  %s and Auth URL: %s" % (self.__osBaseURL, self.__osAuthURL) )
#
#    #Get the boot OpenStack Image from URI server
#    request = self.__clinova.get_image( self.__bootImageName )
#    if request.returncode != 0:
#      self.__errorStatus = "Can't get the boot image for %s from server %s\n and Auth URL: %s\n%s" % (self.__bootImageName, self.__osBaseURL, self.__osAuthURL, request.stderr)
#      self.log.error( self.__errorStatus )
#      return
#    self.__bootImage = request.image

#    if self.__contextMethod == 'ssh': 
      # the virtualmachine cert/key to be copy on the VM of a specific endpoint
      #self.__vmCertPath = self.__getCSImageOption( "vmCertPath" )
#      self.__vmCertPath = self.imageOptions.contextDict.get( 'vmCertPath', '' )
#      if not self.__vmCertPath:
#        self.__errorStatus = "Can't find the vmCertPath for endpoint %s" % self.__endpoint
#        self.log.error( self.__errorStatus )
#        return

      #self.__vmKeyPath = self.__getCSImageOption( "vmKeyPath" )
#      self.__vmKeyPath = self.imageOptions.contextDict.get( 'vmKeyPath', '' )
#      if not self.__vmKeyPath:
#        self.__errorStatus = "Can't find the vmKeyPath for endpoint %s" % self.__endpoint
#        self.log.error( self.__errorStatus )
#        return

      #self.__vmContextualizeScriptPath = self.__getCSImageOption( "vmContextualizeScriptPath" )
#      self.__vmContextualizeScriptPath = self.imageOptions.contextDict.get( 'vmContextualizeScriptPath', '' )
#      if not self.__vmContextualizeScriptPath:
#        self.__errorStatus = "Can't find the vmContextualizeScriptPath for endpoint %s" % self.__endpoint
#        self.log.error( self.__errorStatus )
#        return

      # the cvmfs context URL
      #self.__vmCvmfsContextURL = self.__getCSImageOption( "vmCvmfsContextURL" )
#      self.__vmCvmfsContextURL = self.imageOptions.contextDict.get( 'vmCvmfsContextURL', '' )
#      if not self.__vmCvmfsContextURL:
#        self.__errorStatus = "Can't find the vmCvmfsContextURL for endpoint %s" % self.__endpoint
#        self.log.error( self.__errorStatus )
#        return

      # the specific context URL
      #self.__vmDiracContextURL = self.__getCSImageOption( "vmDiracContextURL" )
#      self.__vmDiracContextURL = self.imageOptions.contextDict.get( 'vmDiracContextURL', '' )
#      if not self.__vmDiracContextURL:
#        self.__errorStatus = "Can't find the vmDiracContextURL for endpoint %s" % self.__endpoint
#        self.log.error( self.__errorStatus )
#        return

      # the runsvdir run file forjobAgent URL
      #self.__vmRunJobAgentURL = self.__getCSImageOption( "vmRunJobAgentURL" )
#      self.__vmRunJobAgentURL = self.imageOptions.contextDict.get( 'vmRunJobAgentURL', '' )
#      if not self.__vmRunJobAgentURL:
#        self.__errorStatus = "Can't find the vmRunJobAgentURL for endpoint %s" % self.__endpoint
#        self.log.error( self.__errorStatus )
#        return

      # the runsvdir run file vmMonitorAgentURL 
      #self.__vmRunVmMonitorAgentURL = self.__getCSImageOption( "vmRunVmMonitorAgentURL" )
#      self.__vmRunVmMonitorAgentURL = self.imageOptions.contextDict.get( 'vmRunVmMonitorAgentURL', '' )
#      if not self.__vmRunVmMonitorAgentURL:
#        self.__errorStatus = "Can't find the vmRunVmMonitorAgentURL for endpoint %s" % self.__endpoint
#        self.log.error( self.__errorStatus )
#        return

      # the runsvdir run.log file forjobAgent URL 
      #self.__vmRunLogJobAgentURL = self.__getCSImageOption( "vmRunLogJobAgentURL" )
#      self.__vmRunLogJobAgentURL = self.imageOptions.contextDict.get( 'vmRunLogJobAgentURL', '' )
#      if not self.__vmRunLogJobAgentURL:
#        self.__errorStatus = "Can't find the vmRunLogJobAgentURL for endpoint %s" % self.__endpoint
#        self.log.error( self.__errorStatus )
#        return

      # the runsvdir run.log file vmMonitorAgentURL       
      #self.__vmRunLogVmMonitorAgentURL = self.__getCSImageOption( "vmRunLogVmMonitorAgentURL" )
#      self.__vmRunLogVmMonitorAgentURL = self.imageOptions.contextDict.get( 'vmRunLogVmMonitorAgentURL', '' )
#      if not self.__vmRunLogVmMonitorAgentURL:
#        self.__errorStatus = "Can't find the vmRunLogVmMonitorAgentURL for endpoint %s" % self.__endpoint
#        self.log.error( self.__errorStatus )
#        return

      #FIXME: isn't this on the CloudEndpoint ! ??
      # cvmfs http proxy:
      #self.__cvmfs_http_proxy = self.__getCSImageOption( "CVMFS_HTTP_PROXY" )
#      self.__cvmfs_http_proxy = self.__getCSCloudEndpointOption( "CVMFS_HTTP_PROXY" )
#      if not self.__cvmfs_http_proxy:
#        self.__errorStatus = "Can't find the CVMFS_HTTP_PROXY for endpoint %s" % self.__endpoint
#        self.log.error( self.__errorStatus )
#        return

    ## Additional Network pool
    #self.__osIpPool = self.__getCSImageOption( "vmOsIpPool" )

# FIXME: WE DO NOT WANT DEFAULTS !    
    
#    self.__osIpPool = self.imageOptions.contextDict.get( 'vmOsIpPool', '' )
#    if not self.__osIpPool:
#      self.__osIpPool = 'NO'

#  def __getCSImageOption( self, option, defValue = "" ):
#    """
#    Following we can see that every CSImageOption are related with the booting image
#    """
#    return gConfig.getValue( "/Resources/VirtualMachines/Images/%s/%s" % ( self.__DIRACImageName, option ), defValue )

  #.............................................................................
  #.............................................................................
  #.............................................................................
  #.............................................................................
  # NEW STUFF

  def connectNova( self ):

    # self.imageDict
    if not self.imageDict:
      return S_ERROR( "Empty imageDict" )
    # self.cloudEndpointDict
    if not self.cloudEndpointDict:
      return S_ERROR( "Empty cloudEndpointDict" )

    self.__clinova = NovaClient( osAuthURL       = self.cloudEndpointDict[ 'osAuthURL' ], 
                                 osUserName      = self.cloudEndpointDict[ 'osUserName' ], 
                                 osPasswd        = self.cloudEndpointDict[ 'osPasswd' ], 
                                 osTenantName    = self.cloudEndpointDict[ 'osTenantName' ], 
                                 osBaseURL       = self.cloudEndpointDict[ 'osBaseURL' ], 
                                 osServiceRegion = self.cloudEndpointDict[ 'osServiceRegion' ] )

    request = self.__clinova.check_connection()
    if request.returncode != 0:
      self.log.error( "NovaClient returned code %s checking connection" % request.returncode )
      return S_ERROR( "NovaClient returned code %s checking connection" % request.returncode )

    _msg = "Available OpenStack nova endpoint  %s and Auth URL: %s"
    self.log.info( _msg % ( self.cloudEndpointDict[ 'osBaseURL' ], self.cloudEndpointDict[ 'osAuthURL' ] ) )
     
    request = self.__clinova.get_image( self.imageDict[ 'bootImageName' ] )
    if request.returncode != 0:
      _msg      = "Can't get the boot image for %s from server %s\n and Auth URL: %s\n%s"
      _msgtuple = ( self.imageDict[ 'bootImageName' ], self.cloudEndpointDict[ 'osBaseURL' ], 
                    self.cloudEndpointDict[ 'osAuthURL' ], request.stderr )
      self.log.error( _msg % _msgtuple )
      return S_ERROR( _msg % _msgtuple )
      
    self.__bootImage = request.image
    return S_OK()    

  def startNewInstance( self, instanceType ):
    """
    Wrapping the image creation
    """
    
    _msg = "Starting new instance for image: %s; to endpoint %s DIRAC driver of nova endpoint"
    self.log.info( _msg % ( self.imageDict[ 'bootImageName' ], self.endpoint ) )
    
    request = self.__clinova.create_VMInstance( self.imageDict[ 'bootImageName' ], 
                                                self.imageDict[ 'contextMethod' ], 
                                                instanceType, 
                                                self.__bootImage, 
                                                self.imageDict[ 'osIpPool' ] )
    if request.returncode != 0:
      _errMsg = "Can't create instance for boot image: %s at server %s and Auth URL: %s \n%s"
      _errMsg = _errMsg % ( self.imageDict[ 'bootImageName' ], self.cloudEndpointDict[ 'osBaseURL' ], 
                            self.cloudEndpointDict[ 'osAuthURL' ], request.stderr )
      self.log.error( _errMsg )
      return S_ERROR( _errMsg )

    return S_OK( request )

  def contextualizeInstance( self, uniqueId, public_ip ):
    """
    Wrapping the contextualization
    With ssh method, contextualization is asyncronous operation
    """
    if self.imageDict[ 'contextMethod' ] =='ssh':
      
      self.log.verbose( 'Contextualising %s with ssh' % uniqueId )
      request = self.__clinova.contextualize_VMInstance( uniqueId, public_ip, self.imageDict[ 'contextMethod' ], 
                                                         self.imageDict[ 'vmCertPath' ], 
                                                         self.imageDict[ 'vmKeyPath' ], 
                                                         self.imageDict[ 'vmContextualizeScriptPath' ], 
                                                         self.imageDict[ 'vmRunJobAgentURL' ], 
                                                         self.imageDict[ 'vmRunVmMonitorAgentURL' ], 
                                                         self.imageDict[ 'vmRunLogJobAgentURL' ], 
                                                         self.imageDict[ 'vmRunLogVmMonitorAgentURL' ],
                                                         self.imageDict[ 'vmCvmfsContextURL' ], 
                                                         self.imageDict[ 'vmDiracContextURL' ] , 
                                                         self.imageDict[ 'CVMFS_HTTP_PROXY' ], 
                                                         self.cloudEndpointDict[ 'siteName' ], 
                                                         self.cloudEndpointDict[ 'cloudDriver' ] )
      if request.returncode != 0:
        __errorStatus = "Can't contextualize VM id %s at endpoint %s: %s" % ( uniqueId, self.endpoint, request.stderr )
        self.log.error( __errorStatus )
        return S_ERROR( __errorStatus )

    return S_OK( uniqueId )

  def getInstanceStatus( self, uniqueId ):
    """
    Wrapping the get status of the uniqueId VM from the endpoint
    """
    request = self.__clinova.getStatus_VMInstance( uniqueId )
    if request.returncode != 0:
      __errorStatus = "Can't get status %s at endpoint %s: %s" % (uniqueId, self.endpoint, request.stderr)
      self.log.error( __errorStatus )
      return S_ERROR( __errorStatus )

    return S_OK( request.status )

  def stopInstance( self, uniqueId, public_ip ):
    """
    Simple call to terminate a VM based on its id
    """

    request = self.__clinova.terminate_VMinstance( uniqueId, self.cloudEndpointDict[ 'osIpPool' ], public_ip )
    if request.returncode != 0:
      __errorStatus = "Can't delete VM instance %s, IP %s, IpPool %s, from endpoint %s: %s"
      __errorStatus = __errorStatus % ( uniqueId, public_ip, self.cloudEndpointDict[ 'osIpPool' ], 
                                        self.endpoint, request.stderr )
      self.log.error( __errorStatus )
      return S_ERROR( __errorStatus )

    return S_OK( request.stderr )


  #.............................................................................
  # Private methods

  def __getImageDict( self, imageName ):
    
    # Defaults to be returned
    imageDict = {}
    
    # Dictionary with options from the CS
    imageOptions = gConfig.getOptionsDict( '/Resources/VirtualMachines/Images/%s' % imageName )
    if not imageOptions[ 'OK' ]:
      self.log.error( imageOptions[ 'Message' ] )
    else:
      imageOptions = imageOptions[ 'Value' ]
      if not 'bootImageName' in imageOptions:
        self.log.error( "Missing bootImageName in imageOptions" )
      elif not 'contextMethod' in imageOptions:
        self.log.error( "Missing contextMethod in imageOptions" )    
      elif not imageOptions[ 'contextMethod' ] in self.CONTEXT_METHODS: 
        self.log.error( "contextMethod '%s' is not known" % imageOptions[ 'contextMethod' ])
      elif set( self.CONTEXT_METHODS[ imageOptions[ 'contextMethod' ] ] ).difference( set( imageOptions ) ):
        _keys = set( self.CONTEXT_METHODS[ imageOptions[ 'contextMethod' ] ] ).difference( set( imageOptions ) )
        self.log.error( 'Missing %s keys' % _keys )
      else:
        imageDict = imageOptions
                  
    return imageDict

  def __getCloudEndpointDict( self, endpoint ):
    
    # Dictionary to be returned
    cloudEndpoint        = {}

    # Dictionary with options from the CS
    cloudEndpointOptions = gConfig.getOptionsDict( '/Resources/VirtualMachines/CloudEndpoints/%s' % endpoint )
    
    if not cloudEndpointOptions[ 'OK' ]:
      self.log.error( cloudEndpointOptions[ 'Message' ] )
      
    else:
      cloudEndpointOptions = cloudEndpointOptions[ 'Value' ]
      for key in self.NOVA_ENDPOINT_KEYS:
        try:
          if not cloudEndpointOptions[ key ]:
            self.log.error( '%s key on %s endpoint is empty' % ( key, endpoint ) )
            cloudEndpoint = {}
            break
          # If it is not empty, we copy it to the dict returning the result
          cloudEndpoint[ key ] = cloudEndpointOptions[ key ]
          
        except KeyError:
          self.log.error( 'Missing %s key on %s endpoint configuration' % ( key, endpoint ) )
          cloudEndpoint = {} 
          break
    
    return cloudEndpoint

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF