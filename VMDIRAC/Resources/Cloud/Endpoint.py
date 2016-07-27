###########################################################
# $HeadURL$
###########################################################

"""
   CloudEndpoint is a base class for the clients used to connect to different
   cloud providers
"""

__RCSID__ = '$Id$'

class Endpoint( object ):
  """ Endpoint base class
  """
  def __init__( self, parameters = {} ):
    """
    """
    # logger
    self.parameters = parameters
    self.valid = False

  def isValid( self ):
    return self.valid

  def setParameters( self, parameters ):
    self.parameters = parameters

  def getParameterDict( self ):
    return self.parameters

  def initialize( self ):
    pass
