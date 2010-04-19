import logging

import types
import simplejson
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

  def dashboard( self ):
    return render( "/systems/virtualmachines/dashboard.mako" )

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
      if 'cond' in request.params:
        dec = simplejson.loads( request.params[ 'cond' ] )
        for k in dec:
          v = dec[ k ]
          if type( v ) in ( types.StringType, types.UnicodeType ):
            v = [ str( v ) ]
          else:
            v = [ str( f ) for f in v ]
          condDict[ str( k ).replace( "_", "." ) ] = v
    except:
      raise
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
  def getHistoryForInstanceID( self ):
    try:
      instanceID = int( request.params[ 'instanceID' ] )
    except:
      return S_ERROR( "OOps, instance ID has to be an integer" )
    rpcClient = getRPCClient( "WorkloadManagement/VirtualMachineManager" )
    result = rpcClient.getHistoryForInstanceID( instanceID )
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

  @jsonify
  def getInstanceStatusCounters( self ):
    rpcClient = getRPCClient( "WorkloadManagement/VirtualMachineManager" )
    result = rpcClient.getInstanceCounters()
    if not result[ 'OK' ]:
      return S_ERROR( result[ 'Message' ] )
    return result

  @jsonify
  def getGroupedInstanceHistory( self ):
    try:
      dbVars = [ str( f ) for f in simplejson.loads( request.params[ 'vars' ] ) ]
    except:
      dbVars = [ 'Load', 'Jobs', 'TransferredFiles' ]
    rpcClient = getRPCClient( "WorkloadManagement/VirtualMachineManager" )
    result = rpcClient.getAverageHistoryValues( 3600, {}, dbVars )
    if not result[ 'OK' ]:
      return S_ERROR( result[ 'Message' ] )
    svcData = result[ 'Value' ]
    print svcData
    data = []
    for record in svcData[ 'Records' ]:
      rL = []
      for iP in range( len( svcData[ 'ParameterNames' ] ) ):
        param = svcData[ 'ParameterNames' ][iP]
        if param == 'Update':
          rL.append( Time.toEpoch( record[iP] ) )
        else:
          rL.append( record[iP] )
      data.append( rL )
    return S_OK( { 'data': data, 'fields' : svcData[ 'ParameterNames' ] } )
