# $HeadURL$

import os
import time

from DIRAC                                            import gConfig, S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities                             import List
from DIRAC.Core.Utilities.Subprocess                  import pythonCall
from DIRAC.Core.Utilities.ThreadPool                  import ThreadPool
from DIRAC.Core.Utilities.ThreadSafe                  import Synchronizer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Resources.Catalog.FileCatalog              import FileCatalog
from DIRAC.Resources.Storage.StorageElement           import StorageElement

__RCSID__ = '$Id: $'

transferSync = Synchronizer()

class OutputDataExecutor:

  def __init__( self, csPath = "" ):
    self.log = gLogger.getSubLogger( "OutputDataExecutor" )
    if not csPath:
      vo = gConfig.getValue( "/DIRAC/VirtualOrganization", "" )
      self.__transfersCSPath = '/Operations/%s/OutputData' % vo
    else:
      self.__transfersCSPath = csPath
    self.log.verbose( "Reading transfer paths from %s" % self.__transfersCSPath )
    self.__requiredCSOptions = ['InputPath', 'InputFC', 'OutputPath', 'OutputFC', 'OutputSE']

    self.__threadPool = ThreadPool( gConfig.getValue( "%s/MinTransfers" % self.__transfersCSPath, 1 ),
                                    gConfig.getValue( "%s/MaxTransfers" % self.__transfersCSPath, 4 ),
                                    gConfig.getValue( "%s/MaxQueuedTransfers" % self.__transfersCSPath, 100 ) )
    self.__threadPool.daemonize()
    self.__processingFiles = set()
    self.__okTransferredFiles = 0
    self.__okTransferredBytes = 0
    self.__failedFiles = {}

  def getNumOKTransferredFiles( self ):
    return self.__okTransferredFiles

  def getNumOKTransferredBytes( self ):
    return self.__okTransferredBytes

  def transfersPending( self ):
    return self.__threadPool.isWorking()

  def getDefinedTransferPaths( self ):
    result = gConfig.getSections( self.__transfersCSPath )
    if not result['OK']:
      self.log.info( 'No Input/Output Pair defined in CS' )
      return S_OK([])

    pathList = result['Value']

    tPaths = {}
    for name in pathList:
      csPath = self.__transfersCSPath + '/%s' % name
      result = gConfig.getOptionsDict( csPath )
      if not result['OK']:
        continue
      transferDict = result['Value']
      ok = True
      for i in self.__requiredCSOptions:
        if i not in transferDict:
          self.log.error( 'Missing Option %s in %s' % ( i, csPath ) )
          ok = False
          break
      if not ok:
        continue
      tPaths[ name ] = transferDict

    return S_OK( tPaths )

  def getNumLocalOutgoingFiles( self ):
    result = self.getDefinedTransferPaths()
    if not result[ 'OK' ]:
      return 0
    localOutgoing = 0
    tPaths = result[ 'Value' ]
    for name in tPaths:
      transferDict = tPaths[ name ]
      if 'LocalDisk' != transferDict['InputFC']:
        continue
      localOutgoing += len( self.getOutgoingFiles( transferDict ) )
    return localOutgoing

  def getOutgoingFiles( self, transferDict ):
    """
    Get list of files to be processed from InputPath
    """
    inputFCName = transferDict['InputFC']
    inputPath = transferDict['InputPath']

    if inputFCName == 'LocalDisk':
      files = []
      try:
        for fileName in os.listdir( inputPath ):
          if os.path.isfile( os.path.join( inputPath, fileName ) ):
            files.append( fileName )
      except:
        pass
      return files

    inputFC = FileCatalog( [inputFCName] )
    result  = inputFC.listDirectory( inputPath, True )

    if not result['OK']:
      self.log.error( result['Message'] )
      return []
    if not inputPath in result['Value']['Successful']:
      self.log.error( result['Value']['Failed'][inputPath] )
      return []

    subDirs = result['Value']['Successful'][inputPath]['SubDirs']
    files   = result['Value']['Successful'][inputPath]['Files']
    for subDir in subDirs:
      self.log.info( 'Ignoring subdirectory:', subDir )
    return files.keys()

  def checkForTransfers( self ):
    """
    Check for transfers to do and start them
    """
    result = self.getDefinedTransferPaths()
    if not result[ 'OK' ]:
      return result
    tPaths = result[ 'Value' ]
    for name in tPaths:
      transferPath = tPaths[ name ]
      self.log.verbose( "Checking %s transfer path" % name )
      filesToTransfer = self.getOutgoingFiles( tPaths[ name ] )
      self.log.info( "Transfer path %s has %d files" % ( name, len( filesToTransfer ) ) )
      ret = self.__addFilesToThreadPool( filesToTransfer, transferPath )
      if not ret['OK']:
        # The thread pool got full 
        break

  def processAllPendingTransfers( self ):
    self.__threadPool.processAllResults()

  @transferSync
  def __addFilesToThreadPool( self, files, transferDict ):
    for fileName in files:
      fileName = os.path.basename( fileName )
      if fileName in self.__processingFiles:
        continue
      self.__processingFiles.add( fileName )
      time.sleep( 1 )
      ret = self.__threadPool.generateJobAndQueueIt( self.__transferIfNotRegistered,
                                            args = ( fileName, transferDict ),
                                            oCallback = self.transferCallback,
                                            blocking = False )
      if not ret['OK']:
        # The thread pool got full 
        return ret
    return S_OK()

  def __transferIfNotRegistered( self, file, transferDict ):
    result = self.isRegisteredInOutputCatalog( file, transferDict )
    if not result[ 'OK' ]:
      self.log.error( result[ 'Message' ] )
      return result
    #Already registered. Need to delete
    if result[ 'Value' ]:
      self.log.info( "Transfer file %s is already registered in the output catalog" % file )
      #Delete
      filePath = os.path.join( transferDict[ 'InputPath' ], file )
      if transferDict[ 'InputFC' ] == 'LocalDisk':
        os.unlink( filePath )
      #FIXME: what is inFile supposed to be ??
      else:
        inputFC = FileCatalog( [ transferDict['InputFC'] ] )
        replicaDict = inputFC.getReplicas( filePath )
        if not replicaDict['OK']:
          self.log.error( "Error deleting file", replicaDict['Message'] )
        elif not inFile in replicaDict['Value']['Successful']:
          self.log.error( "Error deleting file", replicaDict['Value']['Failed'][inFile] )
        else:
          seList = replicaDict['Value']['Successful'][inFile].keys()
          for se in seList:
            se = StorageElement( se )
            self.log.info( 'Removing from %s:' % se.name, inFile )
            se.removeFile( inFile )
          inputFC.removeFile( file )
      self.log.info( "File %s deleted from %s" % ( file, transferDict[ 'InputFC' ] ) )
      self.__processingFiles.discard( file )
      return S_OK( file )
    #Do the transfer
    return self.__retrieveAndUploadFile( file, transferDict )

  def isRegisteredInOutputCatalog( self, file, transferDict ):
    fc = FileCatalog( [ transferDict[ 'OutputFC' ] ] )
    lfn = os.path.join( transferDict['OutputPath'], os.path.basename( file ) )
    result = fc.getReplicas( lfn )
    if not result[ 'OK' ]:
      return result
    if lfn not in result[ 'Value' ][ 'Successful' ]:
      return S_OK( False )
    replicas = result[ 'Value' ][ 'Successful' ][ lfn ]
    for seName in List.fromChar( transferDict[ 'OutputSE' ], "," ):
      if seName in replicas:
        self.log.verbose( "Transfer file %s is already registered in %s SE" % ( file, seName ) )
        return S_OK( True )
    return S_OK( False )

  def __retrieveAndUploadFile( self, file, outputDict ):
    """
    Retrieve, Upload, and remove
    """
    fileName = file
    inputPath = outputDict['InputPath']
    inputFCName = outputDict['InputFC']
    inBytes = 0
    if inputFCName == 'LocalDisk':
      inFile = file
      file = os.path.join( inputPath, file )
    else:
      inputFC = FileCatalog( [inputFCName] )

      inFile = os.path.join( inputPath, file )
      replicaDict = inputFC.getReplicas( inFile )
      if not replicaDict['OK']:
        self.log.error( replicaDict['Message'] )
        return S_ERROR( fileName )
      if not inFile in replicaDict['Value']['Successful']:
        self.log.error( replicaDict['Value']['Failed'][inFile] )
        return S_ERROR( fileName )
      seList = replicaDict['Value']['Successful'][inFile].keys()

      inputSE = StorageElement( seList[0] )
      self.log.info( 'Retrieving from %s:' % inputSE.name, inFile )
      # ret = inputSE.getFile( inFile )
      # lcg_util binding prevent multithreading, use subprocess instead
      res = pythonCall( 2 * 3600, inputSE.getFile, inFile )
      if not res['OK']:
        self.log.error( res['Message'] )
        return S_ERROR( fileName )
      ret = res['Value']
      if not ret['OK']:
        self.log.error( ret['Message'] )
        return S_ERROR( fileName )
      if not inFile in ret['Value']['Successful']:
        self.log.error( ret['Value']['Failed'][inFile] )
        return S_ERROR( fileName )

    if os.path.isfile( file ):
      inBytes = os.stat( file )[6]

    outputPath = outputDict['OutputPath']
    outputFCName = outputDict['OutputFC']
    replicaManager = ReplicaManager()
    outFile = os.path.join( outputPath, os.path.basename( file ) )
    transferOK = False
    for outputSEName in List.fromChar( outputDict['OutputSE'], "," ):
      outputSE = StorageElement( outputSEName )
      self.log.info( 'Trying to upload to %s:' % outputSE.name, outFile )
      # ret = replicaManager.putAndRegister( outFile, os.path.realpath( file ), outputSE.name, catalog=outputFCName )
      # lcg_util binding prevent multithreading, use subprocess instead
      result = pythonCall( 2 * 3600, replicaManager.putAndRegister, outFile, os.path.realpath( file ), outputSE.name, catalog = outputFCName )
      if result['OK'] and result['Value']['OK']:
        if outFile in result['Value']['Value']['Successful']:
          transferOK = True
          break
        else:
          self.log.error( result['Value']['Value']['Failed'][outFile] )
      else:
        if result['OK']:
          self.log.error( result['Value']['Message'] )
        else:
          self.log.error( result['Message'] )

    if not transferOK:
      return S_ERROR( fileName )

    if result['OK'] or not inputFCName == 'LocalDisk':
      os.unlink( file )

    if not result['OK']:
      self.log.error( ret['Message'] )
      return S_ERROR( fileName )

    self.log.info( "Finished transferring %s [%s bytes]" % ( inFile, inBytes ) )
    self.__okTransferredFiles += 1
    self.__okTransferredBytes += inBytes

    if inputFCName == 'LocalDisk':
      return S_OK( fileName )

    # Now the file is on final SE/FC, remove from input SE/FC
    for se in seList:
      se = StorageElement( se )
      self.log.info( 'Removing from %s:' % se.name, inFile )
      se.removeFile( inFile )

    inputFC.removeFile( inFile )

    return S_OK( fileName )

  @transferSync
  def transferCallback( self, threadedJob, submitResult ):
    if not submitResult['OK']:
      fileName = submitResult['Message']
      if fileName not in self.__failedFiles:
        self.__failedFiles[fileName] = 0
      self.__failedFiles[fileName] += 1
    else:
      fileName = submitResult['Value']
      if fileName in self.__failedFiles:
        del self.__failedFiles[fileName]
    #Take out from processing files
    if fileName in self.__processingFiles:
      self.__processingFiles.discard( fileName )
