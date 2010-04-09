########################################################################
# $HeadURL$
# File :   OutputDataAgent.py
# Author : Ricardo Graciani
########################################################################
"""
  Agent in charge of retrieving outputs from Cloud Output SE and upload it to final 
  destination including registration in LFC and removal of entry in Cloud FC.

  Handled Paths are looked for under /Operations/[vo]/OutputData/Name
  Each Name is a section that should define:
    InputPath
    InputFC
    OutputPath
    OutputFC
    OutputSE

"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.Core.Utilities.ThreadPool                            import ThreadPool
from DIRAC.DataManagementSystem.Client.ReplicaManager           import ReplicaManager
from DIRAC.Resources.Catalog.FileCatalog                        import FileCatalog
from DIRAC.Resources.Storage.StorageElement                     import StorageElement
from DIRAC.Core.Utilities.Shifter                               import setupShifterProxyInEnv


from DIRAC import S_OK, S_ERROR, gConfig

import os, threading, shutil

class OutputDataAgent( AgentModule ):

  pool = None

  def initialize( self ):
    """
    Start a ThreadPool object to process transfers
    """
    self.am_setOption( "MinThreadsInPool", 1 )
    self.am_setOption( "MaxThreadsInPool", 20 )
    self.am_setOption( "TotalThreadsInPool", 100 )

    #Define the shifter proxy needed
    self.am_setModuleParam( "shifterProxy", "DataManager" )

    self.pool = ThreadPool( self.am_getOption( 'MinThreadsInPool' ),
                            self.am_getOption( 'MaxThreadsInPool' ),
                            self.am_getOption( 'TotalThreadsInPool' ) )
    # Daemonize
    self.pool.daemonize()

    self.vo = gConfig.getValue( "/DIRAC/VirtualOrganization", "" )
    if not self.vo:
      return S_ERROR( 'Virtual Organization not defined' )

    self.csPath = '/Operations/%s/OutputData' % self.vo

    self.csOptions = ['InputPath', 'InputFC', 'OutputPath', 'OutputFC', 'OutputSE']

    self.callBackLock = threading.Lock()

    self.failedFiles = {}
    self.processingFiles = {}

    return S_OK()

  def execute( self ):
    """
    Loop over InputPath and OutputPath pairs
    """
    result = gConfig.getSections( self.csPath )
    if not result['OK']:
      self.log.info( 'No Input/Output Pair defined in CS' )
      return S_OK()

    pathList = result['Value']

    for path in pathList:
      csPath = self.csPath + '/%s' % path
      result = gConfig.getOptionsDict( csPath )
      if not result['OK']:
        continue
      outputDict = result['Value']
      ok = True
      for i in self.csOptions:
        if i not in outputDict:
          self.log.error( 'Missing Option %s in %s' % ( i, csPath ) )
          ok = False
          break
      if not ok:
        continue

      files = self.__getFiles( outputDict )

      ret = S_OK()
      for file in files:
        file = os.path.basename( file )
        self.callBackLock.acquire()
        if file in self.processingFiles:
          continue
        self.processingFiles[file] = 1
        self.callBackLock.release()
        ret = self.pool.generateJobAndQueueIt( self.__retrieveAndUploadFile,
                                              args = ( file, outputDict ),
                                              oCallback = self.callBack,
                                              blocking = False )
        if not ret['OK']:
          # The thread pool got full 
          break
      if not ret['OK']:
        # The thread pool got full 
        break

    maxCycles = self.am_getMaxCycles()
    if maxCycles > 0 and self.am_getCyclesDone() == maxCycles - 1:
      #We are in the last cycle. Need to purge the thread pool
      self.pool.processAllResults()

    return S_OK()

  def __getFiles( self, outputDict ):
    """
    Get list of files to be processed from InputPath
    """
    inputFCName = outputDict['InputFC']
    inputPath = outputDict['InputPath']

    if inputFCName == 'LocalDisk':
      files = []
      try:
        for file in os.listdir( inputPath ):
          if os.path.isfile( os.path.join( inputPath, file ) ):
            files.append( file )
      except:
        pass
      return files

    inputFC = FileCatalog( [inputFCName] )
    result = inputFC.listDirectory( inputPath, True )

    if not result['OK']:
      self.log.error( result['Message'] )
      return []
    if not inputPath in result['Value']['Successful']:
      self.log.error( result['Value']['Failed'][inputPath] )
      return []

    subDirs = result['Value']['Successful'][inputPath]['SubDirs']
    files = result['Value']['Successful'][inputPath]['Files']
    for dir in subDirs:
      self.log.info( 'Ignoring subdirectory:', dir )
    return files.keys()



  def __retrieveAndUploadFile( self, file, outputDict ):
    """
    Retrieve, Upload, and remove
    """
    inputPath = outputDict['InputPath']
    inputFCName = outputDict['InputFC']
    if inputFCName == 'LocalDisk':
      inFile = file
      file = os.path.join( inputPath, file )
    else:
      inputFC = FileCatalog( [inputFCName] )

      inFile = os.path.join( inputPath, file )
      replicaDict = inputFC.getReplicas( inFile )
      if not replicaDict['OK']:
        self.log.error( replicaDict['Message'] )
        return S_ERROR( inFile )
      if not inFile in replicaDict['Value']['Successful']:
        self.log.error( replicaDict['Value']['Failed'][inFile] )
        return S_ERROR( inFile )
      seList = replicaDict['Value']['Successful'][inFile].keys()

      inputSE = StorageElement( seList[0] )
      self.log.info( 'Retrieving from %s:' % inputSE.name, inFile )
      ret = inputSE.getFile( inFile )
      if not ret['OK']:
        self.log.error( ret['Message'] )
        return S_ERROR( inFile )
      if not inFile in ret['Value']['Successful']:
        self.log.error( ret['Value']['Failed'][inFile] )
        return S_ERROR( inFile )

    outputPath = outputDict['OutputPath']
    outputSE = StorageElement( outputDict['OutputSE'] )
    outputFCName = outputDict['OutputFC']
    replicaManager = ReplicaManager()

    outFile = os.path.join( outputPath, os.path.basename( file ) )
    self.log.info( 'Uploading to %s:' % outputSE.name, outFile )
    ret = replicaManager.putAndRegister( outFile, os.path.realpath( file ), outputSE.name, catalog = outputFCName )
    if ret['OK'] or not inputFCName == 'LocalDisk':
      os.unlink( file )

    if not ret['OK']:
      self.log.error( ret['Message'] )
      return S_ERROR( inFile )

    if inputFCName == 'LocalDisk':
      return S_OK( inFile )

    # Now the file is on final SE/FC, remove from input SE/FC
    self.log.info( 'Removing from %s:' % inputSE.name, inFile )
    inputSE.removeFile( inFile )
    for se in seList[1:]:
      se = StorageElement( se )
      self.log.info( 'Removing from %s:' % se.name, inFile )
      se.removeFile( inFile )

    inputFC.removeFile( inFile )

    return S_OK( inFile )

  def callBack( self, threadedJob, submitResult ):
    if not submitResult['OK']:
      self.callBackLock.acquire()
      file = submitResult['Message']
      if file not in self.failedFiles:
        self.failedFiles[file] = 0
      self.failedFiles[file] += 1
      try:
        del self.processingFiles[file]
      except:
        pass
      self.callBackLock.release()
    else:
      self.callBackLock.acquire()
      file = submitResult['Value']
      if file in self.failedFiles:
        del self.failedFiles[file]
      try:
        del self.processingFiles[file]
      except:
        pass
      self.callBackLock.release()

