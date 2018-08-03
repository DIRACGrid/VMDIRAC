###########################################################
# $HeadURL$
# File: OpenStackEndpoint.py
# Author: A.T.
###########################################################

"""
   OpenStackEndpoint is Endpoint base class implementation for the OpenStack cloud service.
"""

__RCSID__ = '$Id$'

import requests
import json

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from VMDIRAC.Resources.Cloud.Endpoint import Endpoint
from VMDIRAC.Resources.Cloud.KeystoneClient import KeystoneClient
from DIRAC.Core.Utilities.File import makeGuid

DEBUG = False

class OpenStackEndpoint( Endpoint ):
  """ OpenStack implementation of the Cloud Endpoint interface
  """

  def __init__( self, parameters = {}, bootstrapParameters = {} ):
    """
    """
    super( OpenStackEndpoint, self ).__init__(parameters = parameters,
                                              bootstrapParameters = bootstrapParameters)
    # logger
    self.log = gLogger.getSubLogger( 'OpenStackEndpoint' )
    self.ks = None
    self.flavors = {}
    self.computeURL = None
    self.imageURL = None
    self.networkURL = None
    self.project = None
    self.vmInfo = {}
    self.initialize()


  def initialize( self ):

    self.project = self.parameters.get("Project")
    keyStoneURL = self.parameters.get("AuthURL")
    self.ks = KeystoneClient(keyStoneURL, self.parameters)
    result = self.ks.getToken()
    if not result['OK']:
      return result
    self.token = result['Value']
    self.computeURL = self.ks.computeURL
    self.imageURL = self.ks.imageURL
    self.networkURL = self.ks.networkURL

    self.log.verbose("Service interfaces:\ncompute %s,\nimage %s,\nnetwork %s" %
                     (self.computeURL, self.imageURL, self.networkURL))

    result = self.getFlavors()
    return result

  def getFlavors(self):

    result = requests.get("%s/flavors/detail" % self.computeURL,
                           headers = {"X-Auth-Token": self.token})

    output = json.loads(result.text)
    for flavor in output['flavors']:
      print flavor["name"]
      self.flavors[flavor["name"]] = {"FlavorID": flavor['id'],
                                      "RAM": flavor['ram'],
                                      "NumberOfProcessors":flavor['vcpus']}

  def createInstances( self, vmsToSubmit ):
    outputDict = {}
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
        break

    # We failed submission utterly
    if not outputDict:
      return S_ERROR('No VM submitted')

    return S_OK( outputDict )

  def createInstance(self, instanceID = ''):
    """
    This creates a VM instance for the given boot image
    and creates a context script, taken the given parameters.
    Successful creation returns instance VM

    Boots a new node on the OpenStack server defined by self.endpointConfig. The
    'personality' of the node is done by self.imageConfig. Both variables are
    defined on initialization phase.

    The node name has the following format:
    <bootImageName><contextMethod><time>

    It boots the node. If IPpool is defined on the imageConfiguration, a floating
    IP is created and assigned to the node.

    :return: S_OK( ( nodeID, publicIP ) ) | S_ERROR
    """

    imageID = self.parameters.get( 'ImageID' )
    flavor = self.parameters.get( 'FlavorName' )
    flavorID = self.flavors[flavor]["FlavorID"]
    self.parameters['VMUUID'] = instanceID
    self.parameters['VMType'] = self.parameters.get( 'CEType', 'Occi' )

    result = self._createUserDataScript()
    if not result['OK']:
      return result
    userData = str( result['Value'] )

    headers = {"X-Auth-Token": self.token}

    requestDict = {"server": {"user_data": userData,
                              "name": instanceID,
                              "imageRef": imageID,
                              "flavorRef": flavorID }
                   }

    requestJson = json.dumps(requestDict)

    result = requests.post("%s/servers" % self.computeURL,
                           data = requestJson,
                           headers = headers)

    print "AT >>> createInstance", result, result.headers
    print "AT >>> result.text", result.text

    output = json.loads(result.text)

    nodeID = output["server"]["id"]

    return S_OK((nodeID,None))

  def getVMIDs( self ):
    """ Get all the VM IDs on the endpoint

    :return: list of VM ids
    """

    try:
      response = requests.get("%s/servers" % self.computeURL,
                              headers = {"X-Auth-Token": self.token})
    except Exception as e:
      return S_ERROR( 'Cannot connect to ' + self.computeUrl + ' (' + str(e) + ')' )

    print "AT >>> getVMIDs", response, response.headers
    print "AT >>> result.text", response.text

    output = json.loads(response.text)
    idList = []
    for server in output["servers"]:
      idList.append(server['id'])
    return S_OK( idList )

  def getVMStatus(self, nodeID):
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

    result = self.__getVMInfo(nodeID)
    if not result['OK']:
      return result

    output = result['Value']
    status = output["server"]["status"]

    return S_OK( status )

  def getVMNetworks( self,  projectIDs = [] ):
    """ Get a network object corresponding to the networkName

    :param str networkName: network name
    :return: S_OK|S_ERROR network object in case of S_OK
    """
    try:
      result = requests.get("%s/v2.0/networks" % self.networkURL,
                            headers = {"X-Auth-Token": self.token})
      output = json.loads(result.text)
      import pprint
      pprint.pprint(output)

    except  Exception as exc:
      return S_ERROR( 'Cannot get networks: %s' % str(exc) )

    networks = []
    for network in output['networks']:
      if network['project_id'] in projectIDs:
        networks.append(network)
    return S_OK( networks )

  def stopVM( self, nodeID, publicIP = '' ):
    """
    Given the node ID it gets the node details, which are used to destroy the
    node making use of the libcloud.openstack driver. If three is any public IP
    ( floating IP ) assigned, frees it as well.

    :Parameters:
      **uniqueId** - `string`
        openstack node id ( not uuid ! )
      **public_ip** - `string`
        public IP assigned to the node if any

    :return: S_OK | S_ERROR
    """

    print "%s/servers/%s" % (self.computeURL, nodeID)

    try:
      response = requests.delete("%s/servers/%s" % (self.computeURL, nodeID),
                                 headers = {"X-Auth-Token": self.token})
    except Exception as e:
      return S_ERROR( 'Cannot get node details for %s (' % nodeID + str(e) + ')' )

    if response.status_code == 204:
      return S_OK( response.text )
    else:
      return S_ERROR( response.text )

  def __getVMPortID(self, nodeID):
    """ Get th port ID associated with the given VM

    :param str nodeID: VM ID
    :return: port ID
    """
    if nodeID in self.vmInfo and 'portID' in self.vmInfo[nodeID]:
      return S_OK(self.vmInfo[nodeID]['portID'])

    # Get the port of my VM
    try:
      result = requests.get("%s/v2.0/ports" % self.networkURL,
                            headers = {"X-Auth-Token": self.token})
      output = json.loads(result.text)
      import pprint
      pprint.pprint(output)

      portID = None
      for port in output['ports']:
        if port['device_id'] == nodeID:
          portID = port['id']
          self.vmInfo.setdefault(nodeID,{})
          self.vmInfo[nodeID]['portID'] = portID
    except Exception as exc:
      return S_ERROR( 'Cannot get ports: %s' % str(exc) )

    return S_OK(portID)

  def assignFloatingIP( self, nodeID ):
    """
    Given a node, assign a floating IP from the ipPool defined on the imageConfiguration
    on the CS.

    :Parameters:
      **node** - `libcloud.compute.base.Node`
        node object with the vm details

    :return: S_OK( public_ip ) | S_ERROR
    """

    result = self.getVMFloatingIP(nodeID)
    if result['OK']:
      ip = result['Value']
      if ip:
        return S_OK(ip)

    # Get the port of my VM
    result = self.__getVMPortID(nodeID)
    if not result['OK']:
      return result
    portID = result['Value']

    # Get an available floating IP
    try:
      result = requests.get("%s/v2.0/floatingips" % self.networkURL,
                            headers = {"X-Auth-Token": self.token})
      output = json.loads(result.text)
      import pprint
      pprint.pprint(output)

    except Exception as e:
      return S_ERROR( 'Cannot get floatingips' )


    fipID = None
    for fip in output['floatingips']:
      if fip['fixed_ip_address'] is None:
        fipID = fip['id']
        break

    print "AT >>> nodeID, portID, fipID",   nodeID, portID, fipID

    if fipID is None:
      return S_ERROR( 'No floating IP available:q' )

    data = {"floatingip": {"port_id": portID}}
    dataJson = json.dumps(data)

    try:
      result = requests.put("%s/v2.0/floatingips/%s" % (self.networkURL, fipID),
                             data = dataJson,
                             headers = {"X-Auth-Token": self.token})
    except Exception as e:
      return S_ERROR('Cannot assign floating IP')

    print "AT >>> floatingip", result, result.headers, result.text
    output = json.loads(result.text)

    self.vmInfo.setdefault(nodeID, {})
    self.vmInfo['floatingID'] = output['floatingip']['id']

    output = json.loads(result.text)
    import pprint
    pprint.pprint(output)
    self.vmInfo.setdefault(nodeID, {})
    self.vmInfo['floatingID'] = output['floatingip']['id']

    ip = output['floatingip']['floating_ip_address']
    return S_OK(ip)

  def __getVMInfo(self, nodeID):

    try:
      response = requests.get("%s/servers/%s" % (self.computeURL, nodeID),
                              headers = {"X-Auth-Token": self.token})
    except Exception as e:
      return S_ERROR( 'Cannot get node details for %s (' % nodeID + str(e) + ')' )

    if response.status_code == 404:
      return S_ERROR()

    print "AT >>> get", response, response.text

    output = json.loads(response.text)
    import pprint
    pprint.pprint(output)

    if response.status_code == 404:
      return S_ERROR("Cannot get VM info: %s" % output['itemNotFound']['message'])

    # Cache some info
    if response.status_code == 200:
      self.vmInfo.setdefault(nodeID,{})
      self.vmInfo[nodeID]['imageID'] = output['server']['image']['id']
      self.vmInfo[nodeID]['flavorID'] = output['server']['flavor']['id']

    return S_OK(output)

  def getVMFloatingIP(self, nodeID):

    result = self.__getVMInfo(nodeID)
    if not result['OK']:
      return result

    floatingIP = None
    output = result['Value']
    for network, addressList in output['server']['addresses'].items():
      for address in addressList:
        if address['OS-EXT-IPS:type'] == "floating":
          floatingIP = address['addr']

    return S_OK( floatingIP )

  def deleteFloatingIP( self, nodeID, floatingIP = None ):
    """
    Deletes a floating IP <public_ip> from the server.

    :param str publicIP: public IP to be deleted
    :param object node: node to which IP is attached
    :return: S_OK | S_ERROR
    """

    if nodeID in self.vmInfo and "floatingID" in self.vmInfo[nodeID]:
      fipID = self.vmInfo[nodeID]["floatingID"]
    else:
      result = self.getVMFloatingIP(nodeID)
      if not result['OK']:
        return result
      ip = result['Value']
      if ip is None:
        return S_OK()

      result = self.__getVMPortID(nodeID)
      if not result['OK']:
        return result

      portID = result['Value']
      # Get an available floating IP
      try:
        result = requests.get("%s/v2.0/floatingips" % self.networkURL,
                              headers = {"X-Auth-Token": self.token})
        output = json.loads(result.text)
        import pprint
        pprint.pprint(output)

      except Exception as e:
        return S_ERROR( 'Cannot get floatingips' )


      fipID = None
      for fip in output['floatingips']:
        if fip['port_id'] == portID:
          fipID = fip['id']
          break

    if not fipID:
      return S_ERROR('Can not get the floating IP ID')

    data = {"floatingip": {"port_id": None}}
    dataJson = json.dumps(data)

    try:
      result = requests.put("%s/v2.0/floatingips/%s" % (self.networkURL, fipID),
                             data = dataJson,
                             headers = {"X-Auth-Token": self.token})
    except Exception as e:
      return S_ERROR('Cannot disassociate floating IP')

    if result.status_code == 200:
      return S_OK(fipID)
    else:
      return S_ERROR("Cannot disassociate floating IP: %s" % result.text)








