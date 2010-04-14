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
    condDict = {}
    try:
      if 'statusSelector' in request.params:
        condDict[ 'inst.Status' ] = [ str( request.params[ 'statusSelector' ] ) ]
    except:
      pass
    print condDict
    rpcClient = getRPCClient( "WorkloadManagement/VirtualMachineManager" )
    result = rpcClient.getInstancesContent( condDict, sort, start, limit )
    if not result[ 'OK' ]:
      return result
    svcData = result[ 'Value' ]
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

  @jsonify
  def getInstanceHistory( self ):
    try:
      instanceID = int( request.params[ 'instanceID' ] )
    except:
      return S_ERROR( "OOps, instance ID has to be an integer" )
    rpcClient = getRPCClient( "WorkloadManagement/VirtualMachineManager" )
    result = rpcClient.getHistoryForInstance( instanceID )
    if not result[ 'OK' ]:
      return result
    svcData = result[ 'Value' ]
    data = { 'history' : [] }
    for record in svcData[ 'Records' ]:
      rD = {}
      for iP in range( len( svcData[ 'ParameterNames' ] ) ):
        param = svcData[ 'ParameterNames' ][iP].replace( ".", "_" )
        if param == 'Update':
          rD[ param ] = record[iP].strftime( "%Y-%m-%d %H:%M:%S" )
        else:
          rD[ param ] = record[iP]
      data[ 'history' ].append( rD )
    return data
