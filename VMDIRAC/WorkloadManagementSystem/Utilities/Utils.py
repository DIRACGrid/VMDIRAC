""" Utilities for VMDIRAC.WorkloadManagementSystem
"""

# DIRAC
from DIRAC import S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

# VMDIRAC
from VMDIRAC.Resources.Cloud.ConfigHelper import findGenericCloudCredentials


__RCSID__ = "$Id$"


def getProxyFileForCE(ce):
  """ Get a file with the proxy to be used to connect to the
      given cloud endpoint

  :param ce: cloud endpoint object
  :return: S_OK/S_ERROR, value is the path to the proxy file
  """

  vo = ce.parameters.get('VO')
  cloudDN = None
  cloudGroup = None
  if vo:
    result = findGenericCloudCredentials(vo=vo)
    if not result['OK']:
      return result
    cloudDN, cloudGroup = result['Value']

  cloudUser = ce.parameters.get('GenericCloudUser')
  if cloudUser:
    result = Registry.getDNForUsername(cloudUser)
    if not result['OK']:
      return result
    cloudDN = result['Value'][0]
  cloudGroup = ce.parameters.get('GenericCloudGroup', cloudGroup)

  if cloudDN and cloudGroup:
    result = gProxyManager.getPilotProxyFromDIRACGroup(cloudDN, cloudGroup, 3600)
    if not result['OK']:
      return result
    proxy = result['Value']
    result = gProxyManager.dumpProxyToFile(proxy)
    return result
  else:
    return S_ERROR('Could not find generic cloud credentials')
