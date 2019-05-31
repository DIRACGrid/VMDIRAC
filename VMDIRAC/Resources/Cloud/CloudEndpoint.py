###########################################################
# $HeadURL$
# File: CloudEndpoint.py
# Original author: Victor Mendez
# Modified: A.T.
###########################################################

"""
   CloudEndpoint is a base class for the clients used to connect to different
   cloud providers
"""

__RCSID__ = '$Id$'

import os
import ssl
import time

from libcloud import security
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File import makeGuid

from VMDIRAC.Resources.Cloud.Endpoint import Endpoint

DEBUG = False


class CloudEndpoint(Endpoint):
  """ CloudEndpoint base class
  """

  def __init__(self, parameters={}):
    """
    """
    super(CloudEndpoint, self).__init__(parameters=parameters)
    # logger
    self.log = gLogger.getSubLogger('CloudEndpoint')
    self.valid = False
    result = self.initialize()
    if result['OK']:
      self.log.debug('CloudEndpoint created and validated')
      self.valid = True

  def initialize(self):

    # Relax security
    security.SSL_VERSION = ssl.PROTOCOL_SSLv23
    security.VERIFY_SSL_CERT = False

    # Variables needed to contact the service
    connDict = {}
    for var in ['ex_domain_name', 'ex_force_auth_url', 'ex_force_service_region', 'ex_force_auth_version',
                'ex_tenant_name', 'ex_keyname', 'ex_voms_proxy']:
      if var in self.parameters:
        connDict[var] = self.parameters[var]

    username = self.parameters.get('User')
    password = self.parameters.get('Password')

    # log info:
    if DEBUG:
      os.system("export LIBCLOUD_DEBUG=/tmp/libcloud.log")
    for key in connDict:
      self.log.info("%s: %s" % (key, connDict[key]))

    # get cloud driver
    providerName = self.parameters.get('Provider', 'OPENSTACK').upper()
    providerCode = getattr(Provider, providerName)
    self.driverClass = get_driver(providerCode)

    self.__driver = self.driverClass(username, password, **connDict)

    result = self.__checkConnection()
    return result

  def __checkConnection(self):
    """
    Checks connection status by trying to list the images.

    :return: S_OK | S_ERROR
    """
    try:
      _result = self.__driver.list_images()
      # the libcloud library, throws Exception. Nothing to do.
    except Exception as errmsg:
      return S_ERROR(errmsg)

    return S_OK()

  def __getImageByName(self, imageName):
    """
    Given the imageName, returns the current image object from the server.

    :Parameters:
      **imageName** - `string`
        imageName as stored on the OpenStack image repository ( glance )

    :return: S_OK( image ) | S_ERROR
    """
    try:
      images = self.__driver.list_images()
      # the libcloud library, throws Exception. Nothing to do.
    except Exception as errmsg:
      return S_ERROR(errmsg)

    image = None
    for im in images:
      if im.name == imageName:
        image = im
        break

    if image is None:
      return S_ERROR("Image %s not found" % imageName)

    return S_OK(image)

  def __getFlavorByName(self, flavorName):
    """
    Given the flavorName, returns the current flavor object from the server.

    :Parameters:
      **flavorName** - `string`
        flavorName as stored on the OpenStack service

    :return: S_OK( flavor ) | S_ERROR
    """
    try:
      flavors = self.__driver.list_sizes()
      # the libcloud library, throws Exception. Nothing to do.
    except Exception as errmsg:
      return S_ERROR(errmsg)

    flavor = None
    for fl in flavors:
      if fl.name == flavorName:
        flavor = fl

    if flavor is None:
      return S_ERROR("Flavor %s not found" % flavorName)

    return S_OK(flavor)

  def __getSecurityGroups(self, securityGroupNames=[]):
    """
    Given the securityGroupName, returns the current security group object from the server.

    :Parameters:
      **securityGroupName** - `string`
        securityGroupName as stored on the OpenStack service

    :return: S_OK( securityGroup ) | S_ERROR
    """

    if not securityGroupNames:
      securityGroupNames = []
    elif not isinstance(securityGroupNames, list):
      securityGroupNames = [securityGroupNames]

    if 'default' not in securityGroupNames:
      securityGroupNames.append('default')

    try:
      secGroups = self.__driver.ex_list_security_groups()
      # the libcloud library, throws Exception. Nothing to do.
    except Exception as errmsg:
      return S_ERROR(errmsg)

    return S_OK([secGroup for secGroup in secGroups if secGroup.name in securityGroupNames])

  def createInstances(self, vmsToSubmit):
    outputDict = {}

    for nvm in xrange(vmsToSubmit):
      instanceID = makeGuid()[:8]
      createPublicIP = 'ipPool' in self.parameters
      result = self.createInstance(instanceID, createPublicIP)
      if result['OK']:
        node, publicIP = result['Value']
        self.log.debug('Created VM instance %s/%s with publicIP %s' % (node.id, instanceID, publicIP))
        nodeDict = {}
        nodeDict['PublicIP'] = publicIP
        nodeDict['InstanceID'] = instanceID
        nodeDict['NumberOfCPUs'] = self.flavor.vcpus
        nodeDict['RAM'] = self.flavor.ram
        nodeDict['DiskSize'] = self.flavor.disk
        nodeDict['Price'] = self.flavor.price
        outputDict[node.id] = nodeDict
      else:
        break

    # We failed submission utterly
    if not outputDict:
      return result

    return S_OK(outputDict)

  def createInstance(self, instanceID='', createPublicIP=True):
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

    if not instanceID:
      instanceID = makeGuid()[:8]

    self.parameters['VMUUID'] = instanceID
    self.parameters['VMType'] = self.parameters.get('CEType', 'OpenStack')

    createNodeDict = {}

    # Get the image object
    if "ImageID" not in self.parameters and 'ImageName' in self.parameters:
      result = self.__getImageByName(self.parameters['ImageName'])
      if not result['OK']:
        return result
      image = result['Value']
    elif "ImageID" in self.parameters:
      try:
        image = self.__driver.get_image(self.parameters['ImageID'])
      except Exception as e:
        if "Image not found" in str(e):
          return S_ERROR("Image with ID %s not found" % self.parameters['ImageID'])
        else:
          return S_ERROR("Failed to get image for ID %s" % self.parameters['ImageID'])
    else:
      return S_ERROR('No image specified')
    createNodeDict['image'] = image

    # Get the flavor object
    if "FlavorID" not in self.parameters and 'FlavorName' in self.parameters:
      result = self.__getFlavorByName(self.parameters['FlavorName'])
      if not result['OK']:
        return result
      flavor = result['Value']
    elif 'FlavorID' in self.parameters:
      flavor = self.__driver.ex_get_size(self.parameters['FlavorID'])
    else:
      return S_ERROR('No flavor specified')
    self.flavor = flavor

    createNodeDict['size'] = flavor

    # Get security groups
    # if 'ex_security_groups' in self.parameters:
    #  result = self.__getSecurityGroups( self.parameters['ex_security_groups'] )
    #  if not result[ 'OK' ]:
    #    self.log.error( result[ 'Message' ] )
    #    return result
    #  self.parameters['ex_security_groups'] = result[ 'Value' ]

    result = self._createUserDataScript()
    if not result['OK']:
      return result

    createNodeDict['ex_userdata'] = result['Value']

    # Optional node contextualization parameters
    for param in ['ex_metadata', 'ex_pubkey_path', 'ex_keyname', 'ex_config_drive']:
      if param in self.parameters:
        createNodeDict[param] = self.parameters[param]

    createNodeDict['name'] = 'DIRAC_%s' % instanceID

    #createNodeDict['ex_config_drive'] = True

    self.log.verbose("Creating node:")
    for key, value in createNodeDict.items():
      self.log.verbose("%s: %s" % (key, value))

    if 'networks' in self.parameters:
      result = self.getVMNetwork()
      if not result['OK']:
        return result
      createNodeDict['networks'] = result['Value']
    if 'keyname' in self.parameters:
      createNodeDict['ex_keyname'] = self.parameters['keyname']

    if 'availability_zone' in self.parameters:
      createNodeDict['ex_availability_zone'] = self.parameters['availability_zone']

    # Create the VM instance now
    try:
      vmNode = self.__driver.create_node(**createNodeDict)

    except Exception as errmsg:
      self.log.error("Exception in driver.create_node", errmsg)
      return S_ERROR(errmsg)

    publicIP = None
    if createPublicIP:

      # Wait until the node is running, otherwise getting public IP fails
      try:
        self.__driver.wait_until_running([vmNode], timeout=600)
        result = self.assignFloatingIP(vmNode)
        if result['OK']:
          publicIP = result['Value']
        else:
          vmNode.destroy()
          return result
      except Exception as exc:
        self.log.debug('Failed to wait node running %s' % str(exc))
        vmNode.destroy()
        return S_ERROR('Failed to wait until the node is Running')

    return S_OK((vmNode, publicIP))

  def getVMNodes(self):
    """ Get all the nodes on the endpoint

    :return:
    """

    try:
      nodes = self.__driver.list_nodes()
    except Exception as errmsg:
      return S_ERROR(errmsg)

    return S_OK(nodes)

  def getVMNode(self, nodeID):
    """
    Given a Node ID, returns all its configuration details on a
    libcloud.compute.base.Node object.

    :Parameters:
      **nodeID** - `string`
        openstack node id ( not uuid ! )

    :return: S_OK( Node ) | S_ERROR
    """

    try:
      node = self.__driver.ex_get_node_details(nodeID)
    except Exception as errmsg:
      # Let's if the node is in the list of available nodes
      result = self.getVMNodes()
      if not result['OK']:
        return S_ERROR('Failed to get nodes')
      nodeList = result['Value']
      for nd in nodeList:
        if nd.id == nodeID:
          # Let's try again
          try:
            node = self.__driver.ex_get_node_details(nodeID)
            break
          except Exception as exc:
            return S_ERROR('Failed to get node details %s' % str(exc))
      node = None

    return S_OK(node)

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

    result = self.getVMNode(nodeID)
    if not result['OK']:
      return result

    state = result['Value'].state

    # reversed from libcloud
    stateMapDict = {0: 'RUNNING',
                    1: 'REBOOTING',
                    2: 'TERMINATED',
                    3: 'PENDING',
                    4: 'UNKNOWN'}

    if state not in stateMapDict:
      return S_ERROR('State %s not in STATEMAP' % state)

    return S_OK(stateMapDict[state])

  def getVMNetwork(self, networkNames=[]):
    """ Get a network object corresponding to the networkName

    :param str networkName: network name
    :return: S_OK|S_ERROR network object in case of S_OK
    """
    resultList = []
    nameList = list(networkNames)
    if not nameList:
      nameList = self.parameters.get('networks')
      if not nameList:
        return S_ERROR('Network names are not specified')
      else:
        nameList = nameList.split(',')

    result = self.__driver.ex_list_networks()
    for oNetwork in result:
      if oNetwork.name in nameList:
        resultList.append(oNetwork)

    return S_OK(resultList)

  def stopVM(self, nodeID, publicIP=''):
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

    # Get Node object with node details
    result = self.getVMNode(nodeID)
    if not result['OK']:
      return result
    node = result['Value']
    if node is None:
      # Node does not exist
      return S_OK()

    nodeIP = node.public_ips[0] if node.public_ips else None
    if not publicIP and nodeIP is not None:
      publicIP = nodeIP

    # Delete floating IP if any
    if publicIP:
      result = self.deleteFloatingIP(publicIP, node)
      if not result['OK']:
        self.log.error('Failed in deleteFloatingIP:', result['Message'])

    # Destroy the VM instance
    if node is not None:
      try:
        result = self.__driver.destroy_node(node)
        if not result:
          return S_ERROR("Failed to destroy node: %s" % node.id)
      except Exception as errmsg:
        return S_ERROR(errmsg)

    return S_OK()

  def getVMPool(self, poolName):

    try:
      poolList = self.__driver.ex_list_floating_ip_pools()
      for pool in poolList:
        if pool.name == poolName:
          return S_OK(pool)
    except Exception as errmsg:
      return S_ERROR(errmsg)

    return S_ERROR('IP Pool with the name %s not found' % poolName)

  # .............................................................................
  # Private methods

  def assignFloatingIP(self, node):
    """
    Given a node, assign a floating IP from the ipPool defined on the imageConfiguration
    on the CS.

    :Parameters:
      **node** - `libcloud.compute.base.Node`
        node object with the vm details

    :return: S_OK( public_ip ) | S_ERROR
    """

    ipPool = self.parameters.get('ipPool')

    if ipPool:
      result = self.getVMPool(ipPool)
      if not result['OK']:
        return result

      pool = result['Value']
      try:
        floatingIP = pool.create_floating_ip()
        # Add sleep between creation and assignment
        time.sleep(60)
        self.__driver.ex_attach_floating_ip_to_node(node, floatingIP)
        publicIP = floatingIP.ip_address
        return S_OK(publicIP)

      except Exception as errmsg:
        return S_ERROR(errmsg)
    else:
      return S_ERROR('No IP pool specified')

  def getVMFloatingIP(self, publicIP):

    # We are still with IPv4
    publicIP = publicIP.replace('::ffff:', '')

    ipPool = self.parameters.get('ipPool')

    if ipPool:
      try:
        floatingIP = None
        poolList = self.__driver.ex_list_floating_ip_pools()
        for pool in poolList:
          if pool.name == ipPool:
            ipList = pool.list_floating_ips()
            for ip in ipList:
              if ip.ip_address == publicIP:
                floatingIP = ip
                break
            break
        return S_OK(floatingIP)
      except Exception as errmsg:
        return S_ERROR(errmsg)
    else:
      return S_ERROR('No IP pool specified')

  def deleteFloatingIP(self, publicIP, node):
    """
    Deletes a floating IP <public_ip> from the server.

    :param str publicIP: public IP to be deleted
    :param object node: node to which IP is attached
    :return: S_OK | S_ERROR
    """

    # We are still with IPv4
    publicIP = publicIP.replace('::ffff:', '')

    result = self.getVMFloatingIP(publicIP)
    if not result['OK']:
      return result

    floatingIP = result['Value']
    if floatingIP is None:
      return S_OK()

    try:
      if node is not None:
        self.__driver.ex_detach_floating_ip_from_node(node, floatingIP)
      floatingIP.delete()
      return S_OK()
    except Exception as errmsg:
      return S_ERROR(errmsg)
