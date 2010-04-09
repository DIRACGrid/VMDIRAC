
import os
from DIRAC import gConfig, S_OK, S_ERROR

class OutputDataHelper:

  def __init__( self ):
    self.vo = gConfig.getValue( "/DIRAC/VirtualOrganization", "" )
    self.csPath = '/Operations/%s/OutputData' % self.vo
    self.csOptions = ['InputPath', 'InputFC', 'OutputPath', 'OutputFC', 'OutputSE']

  def getDefinedTransferPaths( self ):
    self.vo = gConfig.getValue( "/DIRAC/VirtualOrganization", "" )
    if not self.vo:
      return S_ERROR( 'Virtual Organization not defined' )

    result = gConfig.getSections( self.csPath )
    if not result['OK']:
      self.log.info( 'No Input/Output Pair defined in CS' )
      return S_OK()

    pathList = result['Value']

    tPaths = {}
    for path in pathList:
      csPath = self.csPath + '/%s' % path
      result = gConfig.getOptionsDict( csPath )
      if not result['OK']:
        continue
      transferDict = result['Value']
      ok = True
      for i in self.csOptions:
        if i not in transferDict:
          self.log.error( 'Missing Option %s in %s' % ( i, csPath ) )
          ok = False
          break
      if not ok:
        continue
      tPaths[ path ] = transferDict

    return S_OK( tPaths )

  def getNumLocalOutgoingFiles( self ):
    result = self.getDefinedTransferPaths()
    if not result[ 'OK' ]:
      return 0
    localOutgoing = 0
    tPaths = result[ 'Value' ]
    for path in tPaths:
      transferDict = tPaths[ path ]
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
