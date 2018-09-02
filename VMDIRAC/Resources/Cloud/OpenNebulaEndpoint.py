"""
   OpenNebulaEndpoint is Endpoint base class implementation for OpenNebula XML-RPC protocol.
"""

__RCSID__ = '$Id$'

import os
import requests
from requests.auth import HTTPBasicAuth
import base64
from pprint import pprint as pp
import xmlrpclib
import ssl

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from VMDIRAC.Resources.Cloud.Endpoint import Endpoint
from VMDIRAC.Resources.Cloud.KeystoneClient import KeystoneClient
from DIRAC.Core.Utilities.File import makeGuid

DEBUG = False

class OpenNebulaEndpoint( Endpoint ):
  """ OpenNebula implementation of the Cloud Endpoint interface
  """

  def __init__( self, parameters = {} ):
    """
    """
    super( OpenNebulaEndpoint, self ).__init__(parameters = parameters, bootstrapParameters=parameters)

    # TODO: HostCert and HostKey are not set via parameters
    self.parameters['HostCert'] = '/opt/dirac/etc/grid-security/hostcert.pem'
    self.bootstrapParameters['HostCert'] = self.parameters['HostCert']
    self.parameters['HostKey'] = '/opt/dirac/etc/grid-security/hostkey.pem'
    self.bootstrapParameters['HostKey'] = self.parameters['HostKey']

    # logger
    self.log = gLogger.getSubLogger( 'OpenNebulaEndpoint' )

    #self.log.info(parameters['CAPath']['CallStack'])
    #self.log.info(parameters['CAPath']['Message'])
    self.valid = False
    self.vmType = self.parameters.get( 'VMType' )
    self.site = self.parameters.get( 'Site' )

    # Prepare the authentication request parameters
    self.session = None
    self.authArgs = {}
    self.user = self.parameters.get( "User" )
    self.password = self.parameters.get( "Password" )
    self.loginMode = False

    if self.user and self.password:
      # we have the login/password case
      self.authArgs['auth'] = HTTPBasicAuth(self.user, self.password)
      self.authArgs['verify'] = False
      self.loginMode = True
      self.oneauth = self.user + ':' + self.password
    else:
      # TODO: untested branch
      # we have the user proxy case
      self.userProxy = os.environ.get( 'X509_USER_PROXY' )
      self.userProxy = self.parameters.get( "Proxy", self.userProxy )
      if self.userProxy is None:
        self.log.error("User proxy is not defined")
        self.valid = False
        return
      self.authArgs['cert'] = self.userProxy
      self.caPath = self.parameters.get( 'CAPath', '/opt/dirac/etc/grid-security/certificates/RDIG.pem' )
      self.authArgs['verify'] = self.caPath
      if self.parameters.get("Auth") == "voms":
        self.authArgs['data'] = '{"auth":{"voms": true}}'

    self.serviceUrl = self.parameters.get( 'EndpointUrl' )

    result = self.initialize()
    if result['OK']:
      self.log.debug( 'OpenNebulaEndpoint is created and validated' )
      self.valid = True

  def initialize( self ):
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    self.rpcproxy = xmlrpclib.ServerProxy( self.serviceUrl, context=ssl_ctx )

    # TODO: check that self.serviceUrl is acceceble

    return  S_OK()

  def createInstances( self, vmsToSubmit ):
    outputDict = {}
    message = ''
    for nvm in xrange( vmsToSubmit ):
      instanceID = makeGuid()[:8]
      createPublicIP = 'ipPool' in self.parameters

      result = self.createInstance( instanceID, createPublicIP )
      if result['OK']:
        nodeID, publicIP = result['Value']
        self.log.debug( 'Created VM instance %s/%s with publicIP %s' % ( nodeID, instanceID, publicIP ) )
        nodeDict = {}
        nodeDict['PublicIP'] = publicIP
        nodeDict['InstanceID'] = instanceID
        nodeDict['NumberOfCPUs'] = 2
        #nodeDict['RAM'] = self.flavor.ram
        #nodeDict['DiskSize'] = self.flavor.disk
        #nodeDict['Price'] = self.flavor.price
        outputDict[nodeID] = nodeDict
      else:
        message = result['Message']
        break

    # We failed submission utterly
    if not outputDict:
      return S_ERROR('No VM submitted: %s' % message)

    return S_OK( outputDict )

  def createInstance( self, instanceID = '', createPublicIP = True  ):
    """
    Creates VM in OpenNebula cloud.

    one.template.instantiate XML-RPC method is called with the following parameters

    * template ID is obtained from Endpoint's `TemplateID` constructor parameter
    * VM name equals to `instanceID` method argument
    * onhold flag is obtained from Endpoint's `Onhold` constructor parameter
    * template string contains userdata scripts encoded in base64 and DNS 
    servers adresses

    :Parameters:
      **instanceID** - `string`
        name of a new VM
      **createPublicIP** - `bool`
        ignored

    :return: S_OK( ( nodeID, publicIP ) ) | S_ERROR
    """

    # TODO: which templateId to use? 
    templateId = self.parameters.get( 'TemplateID', 1362 )
    onhold = self.parameters.get( 'Onhold', False )

    # this stuff is required by _createUserDataScript
    self.parameters['VMUUID'] = instanceID
    self.parameters['VMType'] = self.parameters.get( 'CEType', 'Occi' )
    result = self._createUserDataScript()
    if not result['OK']:
      return result
    userData = str( result['Value'] )

    # TODO: hardcoding DNS is also not a good idea
    template='''CONTEXT = [
NETWORK = "YES",
USER_DATA = "{}",
DNS = "8.8.8.8 8.8.4.4",
USERDATA_ENCODING = "base64"
]'''.format(base64.b64encode(userData))

    ret = self.rpcproxy.one.template.instantiate(
      self.oneauth, templateId, instanceID, onhold, template
    )

    # XXX: Sometimes it returns 4 values although documemntation says otherwise
    ok = ret[0]
    payload = ret[1]
    error = ret[2]

    if ok:
      return S_OK((payload,None))

    return S_ERROR('one.template.instantiate failed with code: {} message: {} ', error, payload)


  def getVMIDs( self ):
    """ Get all the VM IDs on the endpoint

    :return: list of VM ids
    """
    return S_ERROR('getVMIDs is not implemented')

  def getVMStatus( self, nodeID ):
    """
    Get the status for a given node ID. libcloud translates the status into a digit
    from 0 to 4 using a many-to-one relation ( ACTIVE and RUNNING -> 0 ), which
    means we cannot undo that translation. It uses an intermediate states mapping
    dictionary, SITEMAP, which we use here inverted to return the status as a
    meaningful string. The five possible states are ( ordered from 0 to 4 ):
    RUNNING, REBOOTING, TERMINATED, PENDING & UNKNOWN.

    :Parameters:
      **uniqueId** - `string`
        openstack node id ( not uuid ! )

    :return: S_OK( status ) | S_ERROR
    """
    return S_ERROR('getVMStatus is not implemented')

  def getVMNetworks( self, networkNames = [] ):
    """ Get a network object corresponding to the networkName

    :param str networkName: network name
    :return: S_OK|S_ERROR network object in case of S_OK
    """
    return S_ERROR('getVMNetworks is not implemented')

  def getVMNetworkInterface( self, network ):
    """ Get a network object corresponding to the networkName

    :param str networkName: network name
    :return: S_OK|S_ERROR network object in case of S_OK
    """
    return S_ERROR('getVMNetworkInterface is not implemented')

  def stopVM( self, nodeID, publicIP = '' ):
    """
    Terminates VM specified by nodeID by sending one.vm.action(..., 'delete', ..)
    request.

    :Parameters:
      **nodeID** - `string`
        OpenNebula VM id specifier
      **publicIP** - `string`
        ignored

    :return: S_OK | S_ERROR
    """
    self.log.info('DELETE ID: ' + str(nodeID))

    ret = self.rpcproxy.one.vm.action(self.oneauth, 'delete', int(nodeID))

    # XXX: Sometimes it returns 4 values although documemntation says otherwise
    ok = ret[0]
    payload = ret[1]
    error = ret[2]

    if ok:
      return S_OK(payload)

    return S_ERROR('one.vm.action failed with code: {} message: {} ', error, payload)

  def assignFloatingIP( self, nodeID ):
    """
    Given a node, assign a floating IP from the ipPool defined on the imageConfiguration
    on the CS.

    :Parameters:
      **node** - `libcloud.compute.base.Node`
        node object with the vm details

    :return: S_OK( public_ip ) | S_ERROR
    """
    return S_ERROR('assignFloatingIP is not implemented')

  def getVMFloatingIP( self, publicIP ):
    return S_ERROR( 'Not implemented' )

  def deleteFloatingIP( self, nodeID ):
    """
    Deletes a floating IP <public_ip> from the server.

    :param str publicIP: public IP to be deleted
    :param object node: node to which IP is attached
    :return: S_OK | S_ERROR
    """
    return S_ERROR('deleteFloatingIP is not implemnted')
