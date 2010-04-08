import logging

from dirac.lib.base import *
from dirac.lib.diset import getRPCClient, getTransferClient

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import Time, List
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from dirac.lib.webBase import defaultRedirect

log = logging.getLogger( __name__ )

class VirtualmachinesController( BaseController ):

  def index( self ):
    # Return a rendered template
    #   return render('/some/template.mako')
    # or, Return a response
    return defaultRedirect()

  def browse( self ):
    return render( "/systems/virtualmachines/browse.mako" )

  @jsonify
  def getInstancesList( self ):
    try:
      start = int( request.params[ 'start' ] )
    except:
      start = 0
    try:
      limit = int( request.params[ 'limit' ] )
    except:
      limit = 0
    try:
      sortField = str( request.params[ 'sortField' ] ).replace( "_", "." )
      sortDir = str( request.params[ 'sortDirection' ] )
      sort = [ ( sortField, sortDir ) ]
    except:
      return S_ERROR( "Oops! Couldn't understand the request" )
    rpcClient = getRPCClient( "WorkloadManagement/VirtualMachineManager" )
    retVal = rpcClient.getInstancesContent( {}, sort, start, limit )
    if not retVal[ 'OK' ]:
      return retVal
    svcData = retVal[ 'Value' ]
    data = { 'numRecords' : svcData[ 'TotalRecords' ], 'instances' : [] }
    dnMap = {}
    for record in svcData[ 'Records' ]:
      rD = {}
      for iP in range( len( svcData[ 'ParameterNames' ] ) ):
        param = svcData[ 'ParameterNames' ][iP].replace( ".", "_" )
        if param == 'inst_LastUpdate':
          rD[ param ] = record[iP].strftime( "%Y-%m-%d %H:%M:%S" )
        else:
          rD[ param ] = record[iP]
      data[ 'instances' ].append( rD )
    return data
