from DIRAC import gConfig, S_OK
from DIRAC.Core.Utilities.List import fromChar

STATE_MAP = { 0: 'RUNNING',
              1: 'REBOOTING',
              2: 'TERMINATED',
              3: 'PENDING',
              4: 'UNKNOWN',
              5: 'STOPPED',
              6: 'SUSPENDED',
              7: 'ERROR',
              8: 'PAUSED' }

def getVMImageConfig( site, ce, image = '' ):
  """ Get parameters of the specified queue
  """
  Tags = []
  grid = site.split( '.' )[0]
  result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/Cloud/%s' % ( grid, site, ce ) )
  if not result['OK']:
    return result
  resultDict = result['Value']
  ceTags = resultDict.get( 'Tag' )
  if ceTags:
    Tags = fromChar( ceTags )

  if image:
    result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/Cloud/%s/Images/%s' % ( grid, site, ce, image ) )
    if not result['OK']:
      return result
    resultDict.update( result['Value'] )
    queueTags = resultDict.get( 'Tag' )
    if queueTags:
      queueTags = fromChar( queueTags )
      Tags = list( set( Tags + queueTags ) )

  if Tags:
    resultDict['Tag'] = Tags
  resultDict['Image'] = image
  resultDict['Site'] = site
  return S_OK( resultDict )