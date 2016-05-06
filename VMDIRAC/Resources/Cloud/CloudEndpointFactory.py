########################################################################
# File :   CloudEndpointFactory.py
# Author : Andrei Tsaregorodtsev
########################################################################

"""  The Cloud Endpoint Factory has one method that instantiates a given Cloud Endpoint
"""
from DIRAC                             import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities              import ObjectLoader
from VMDIRAC.Resources.Cloud.Utilities import getVMImageConfig

__RCSID__ = "$Id$"

class CloudEndpointFactory( object ):

  #############################################################################
  def __init__(self, ceType=''):
    """ Standard constructor
    """
    self.ceType = 'Base'
    if ceType:
      self.ceType = ceType
    self.log = gLogger.getSubLogger( self.ceType )

  def getCE( self, site, endpoint, image = '' ):

    result = getVMImageConfig( site, endpoint, image )
    if not result[ 'OK' ]:
      return result

    ceParams = result['Value']
    result = self.getCEObject( parameters = ceParams )
    return result

  #############################################################################
  def getCEObject( self, ceType = '', parameters = {} ):
    """This method returns the CloudEndpoint instance corresponding to the supplied
       CEUniqueID.  If no corresponding CE is available, this is indicated.
    """
    ceTypeLocal = ceType
    if not ceTypeLocal:
      ceTypeLocal = self.ceType
    self.log.verbose('Creating CloudEndpoint of %s type' % ceTypeLocal )
    subClassName = "%sCloudEndpoint" % (ceTypeLocal)
    if ceTypeLocal == "Base":
      subClassName = "CloudEndpoint"

    objectLoader = ObjectLoader.ObjectLoader()
    result = objectLoader.loadObject( 'Resources.Cloud.%s' % subClassName, subClassName )
    if not result['OK']:
      gLogger.error( 'Failed to load object', '%s: %s' % ( subClassName, result['Message'] ) )
      return result

    ceClass = result['Value']
    try:
      cloudEndpoint = ceClass( parameters )
    except Exception as x:
      msg = 'CloudEndpointFactory could not instantiate %s object: %s' % ( subClassName, str( x ) )
      self.log.exception()
      self.log.warn( msg )
      return S_ERROR( msg )

    return S_OK( cloudEndpoint )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
