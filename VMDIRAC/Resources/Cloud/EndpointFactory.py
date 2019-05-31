########################################################################
# File :   EndpointFactory.py
# Author : Andrei Tsaregorodtsev
########################################################################

"""  The Cloud Endpoint Factory has one method that instantiates a given Cloud Endpoint
"""
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import ObjectLoader
from VMDIRAC.Resources.Cloud.ConfigHelper import getVMImageConfig

__RCSID__ = "$Id$"


class EndpointFactory(object):

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger('EndpointFactory')

  def getCE(self, site, endpoint, image=''):

    result = getVMImageConfig(site, endpoint, image)
    if not result['OK']:
      return result

    ceParams = result['Value']
    result = self.getCEObject(parameters=ceParams)
    return result

  #############################################################################
  def getCEObject(self, parameters={}):
    """This method returns the CloudEndpoint instance corresponding to the supplied
       CEUniqueID.  If no corresponding CE is available, this is indicated.
    """
    ceType = parameters.get('CEType', 'Cloud')
    self.log.verbose('Creating Endpoint of %s type' % ceType)
    subClassName = "%sEndpoint" % (ceType)

    objectLoader = ObjectLoader.ObjectLoader()
    result = objectLoader.loadObject('Resources.Cloud.%s' % subClassName, subClassName)
    if not result['OK']:
      gLogger.error('Failed to load object', '%s: %s' % (subClassName, result['Message']))
      return result

    ceClass = result['Value']
    try:
      endpoint = ceClass(parameters)
    except Exception as x:
      msg = 'EndpointFactory could not instantiate %s object: %s' % (subClassName, str(x))
      self.log.exception()
      self.log.warn(msg)
      return S_ERROR(msg)

    return S_OK(endpoint)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
