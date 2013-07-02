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

  def overview( self ):
    return render( "/systems/virtualmachines/overview.mako" )

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
      return S_ERROR( "OOps, instance ID has to be an integer " )
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
  def getHistoryValues( self ):
    try:
      dbVars = [ str( f ) for f in simplejson.loads( request.params[ 'vars' ] ) ]
    except:
      dbVars = [ 'Load', 'Jobs', 'TransferredFiles' ]
    try:
      timespan = int( request.params[ 'timespan' ] )
    except:
      timespan = 86400
    rpcClient = getRPCClient( "WorkloadManagement/VirtualMachineManager" )
    result = rpcClient.getHistoryValues( 3600, {}, dbVars, timespan )
    if not result[ 'OK' ]:
      return S_ERROR( result[ 'Message' ] )
    svcData = result[ 'Value' ]
    data = []
    olderThan = Time.toEpoch() - 400
    for record in svcData[ 'Records' ]:
      rL = []
      for iP in range( len( svcData[ 'ParameterNames' ] ) ):
        param = svcData[ 'ParameterNames' ][iP]
        if param == 'Update':
          rL.append( Time.toEpoch( record[iP] ) )
        else:
          rL.append( record[iP] )
      if rL[0] < olderThan:
        data.append( rL )
    return S_OK( { 'data': data, 'fields' : svcData[ 'ParameterNames' ] } )

  @jsonify
  def getRunningInstancesHistory( self ):
    try:
      bucketSize = int( request.params[ 'bucketSize' ] )
    except:
      bucketSize = 900
    try:
      timespan = int( request.params[ 'timespan' ] )
    except:
      timespan = 86400
    rpcClient = getRPCClient( "WorkloadManagement/VirtualMachineManager" )
    result = rpcClient.getRunningInstancesHistory( timespan, bucketSize )
    if not result[ 'OK' ]:
      return S_ERROR( result[ 'Message' ] )
    svcData = result[ 'Value' ]
    data = []
    olderThan = Time.toEpoch() - 400
    for record in svcData:
      eTime = Time.toEpoch( record[0] )
      if eTime < olderThan:
        rL = [ eTime, int( record[1] ) ]
      data.append( rL )
    return S_OK( data )

  @jsonify
  def getRunningInstancesBEPHistory( self ):
    try:
      bucketSize = int( request.params[ 'bucketSize' ] )
    except:
      bucketSize = 900
    try:
      timespan = int( request.params[ 'timespan' ] )
    except:
      timespan = 86400
    rpcClient = getRPCClient( "WorkloadManagement/VirtualMachineManager" )
    result = rpcClient.getRunningInstancesBEPHistory( timespan, bucketSize )
    if not result[ 'OK' ]:
      return S_ERROR( result[ 'Message' ] )
    svcData = result[ 'Value' ]
    data = []
    olderThan = Time.toEpoch() - 400
    for record in svcData:
      eTime = Time.toEpoch( record[0] )
      if eTime < olderThan:
        rL = [ eTime, record[1], int( record[2] ) ]
      data.append( rL )
    return S_OK( data )

  @jsonify
  def checkVmWebOperation( self ):
    try:
      operation = str( request.params[ 'operation' ] )
    except Exception, e:
      print e
      return S_ERROR( "Oops! Couldn't understand the request" )
    rpcClient = getRPCClient( "WorkloadManagement/VirtualMachineManager" )
    result = rpcClient.checkVmWebOperation( operation )
    if not result[ 'OK' ]:
      return S_ERROR( result[ 'Message' ] )
    data = result[ 'Value' ]
    return S_OK( data )

  @jsonify
  def declareInstancesStopping( self ):
    try:
      webIds = simplejson.loads( request.params[ 'idList' ] ) 
    except Exception, e:
      print e
      return S_ERROR( "Oops! Couldn't understand the request" )
    rpcClient = getRPCClient( "WorkloadManagement/VirtualMachineManager" )
    result = rpcClient.declareInstancesStopping( webIds )
    return result

