###########################################################
# $HeadURL$
###########################################################

"""
   CloudEndpoint is a base class for the clients used to connect to different
   cloud providers
"""

from DIRAC import S_ERROR
from VMDIRAC.Resources.Cloud.Utilities import createUserDataScript

__RCSID__ = '$Id$'

class Endpoint( object ):
  """ Endpoint base class
  """
  def __init__(self, parameters = {}, bootstrapParameters={}):
    """
    """
    # logger
    self.parameters = parameters
    self.bootstrapParameters = bootstrapParameters
    self.valid = False

  def isValid( self ):
    return self.valid

  def setParameters( self, parameters ):
    self.parameters = parameters

  def setBootstrapParameters(self, bootstrapParameters):
    self.bootstrapParameters = bootstrapParameters

  def getParameterDict( self ):
    return self.parameters

  def initialize( self ):
    pass

  def _createUserDataScript( self ):

    return createUserDataScript(self.parameters, self.bootstrapParameters)