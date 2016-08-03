####################################################################
#
# Configuration related utilities
#
####################################################################

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers import Registry, Operations
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Utilities.List import fromChar

__RCSID__ = "$Id$"

def findGenericCloudCredentials( vo = False, group = False ):
  if not group and not vo:
    return S_ERROR( "Need a group or a VO to determine the Generic cloud credentials" )
  if not vo:
    vo = Registry.getVOForGroup( group )
    if not vo:
      return S_ERROR( "Group %s does not have a VO associated" % group )
  opsHelper = Operations.Operations( vo = vo )
  cloudGroup = opsHelper.getValue( "Cloud/GenericCloudGroup", "" )
  cloudDN = opsHelper.getValue( "Cloud/GenericCloudDN", "" )
  if not cloudDN:
    cloudUser = opsHelper.getValue( "Cloud/GenericCloudUser", "" )
    if cloudUser:
      result = Registry.getDNForUsername( cloudUser )
      if result['OK']:
        cloudDN = result['Value'][0]
  if cloudDN and cloudGroup:
    gLogger.verbose( "Cloud credentials from CS: %s@%s" % ( cloudDN, cloudGroup ) )
    result = gProxyManager.userHasProxy( cloudDN, cloudGroup, 86400 )
    if not result[ 'OK' ]:
      return S_ERROR( "%s@%s has no proxy in ProxyManager" )
    return S_OK( ( cloudDN, cloudGroup ) )
  return S_ERROR( "Cloud credentials not found" )

def getImages( siteList = None, ceList = None, imageList = None, vo = None ):
  """ Get CE/image options according to the specified selection
  """

  result = gConfig.getSections( '/Resources/Sites' )
  if not result['OK']:
    return result

  resultDict = {}

  grids = result['Value']
  for grid in grids:
    result = gConfig.getSections( '/Resources/Sites/%s' % grid )
    if not result['OK']:
      continue
    sites = result['Value']
    for site in sites:
      if siteList is not None and not site in siteList:
        continue
      if vo:
        voList = gConfig.getValue( '/Resources/Sites/%s/%s/VO' % ( grid, site ), [] )
        if voList and not vo in voList:
          continue
      result = gConfig.getSections( '/Resources/Sites/%s/%s/Cloud' % ( grid, site ) )
      if not result['OK']:
        continue
      ces = result['Value']
      for ce in ces:
        if ceList is not None and not ce in ceList:
          continue
        if vo:
          voList = gConfig.getValue( '/Resources/Sites/%s/%s/Cloud/%s/VO' % ( grid, site, ce ), [] )
          if voList and not vo in voList:
            continue
        result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/Cloud/%s' % ( grid, site, ce ) )
        if not result['OK']:
          continue
        ceOptionsDict = result['Value']
        result = gConfig.getSections( '/Resources/Sites/%s/%s/Cloud/%s/Images' % ( grid, site, ce ) )
        if not result['OK']:
          continue
        images = result['Value']
        for image in images:
          if imageList is not None and not image in imageList:
            continue
          if vo:
            voList = gConfig.getValue( '/Resources/Sites/%s/%s/Cloud/%s/Images/%s/VO' % ( grid, site, ce, image ), [] )
            if voList and not vo in voList:
              continue
          resultDict.setdefault( site, {} )
          resultDict[site].setdefault( ce, ceOptionsDict )
          resultDict[site][ce].setdefault( 'Images', {} )
          result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/Cloud/%s/Images/%s' % ( grid, site, ce, image ) )
          if not result['OK']:
            continue
          imageOptionsDict = result['Value']
          resultDict[site][ce]['Images'][image] = imageOptionsDict

  return S_OK( resultDict )

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

def getPilotBootstrapParameters( vo = '', runningPod = '' ):

  op = Operations.Operations( vo = vo )
  result = op.getOptionsDict( 'Cloud' )
  opParameters = {}
  if result['OK']:
    opParameters = result['Value']
  opParameters['Project'] = op.getValue( 'Cloud/Project', 'DIRAC' )
  opParameters['Version'] = op.getValue( 'Cloud/Version' )
  opParameters['Setup'] = gConfig.getValue( '/DIRAC/Setup', 'unknown' )
  result = op.getOptionsDict( 'Cloud/%s' % runningPod )
  if result['OK']:
    opParameters.update( result['Value'] )
  return S_OK( opParameters )
