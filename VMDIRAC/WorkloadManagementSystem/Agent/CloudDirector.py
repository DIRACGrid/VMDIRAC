########################################################################
# File :    CloudDirector.py
# Author :  A.Tsaregorodtsev
########################################################################

"""  The Cloud Director is a simple agent performing VM instantiations
"""
import os
import random
import socket
import hashlib
from collections import defaultdict

# DIRAC
import DIRAC
from DIRAC                                                 import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule                           import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers              import CSGlobals, Registry, Operations, Resources
from DIRAC.WorkloadManagementSystem.Client.ServerUtils     import jobDB
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.List                             import fromChar

# VMDIRAC
from VMDIRAC.Resources.Cloud.EndpointFactory               import EndpointFactory
from VMDIRAC.Resources.Cloud.ConfigHelper                  import findGenericCloudCredentials, \
                                                                  getImages, \
                                                                  getPilotBootstrapParameters
from VMDIRAC.WorkloadManagementSystem.Client.ServerUtils   import virtualMachineDB
from DIRAC.WorkloadManagementSystem.Client.ServerUtils     import pilotAgentsDB

__RCSID__ = "$Id$"

class CloudDirector( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    AgentModule.__init__( self, *args, **kwargs )
    self.imageDict = {}
    self.imageCECache = {}
    self.imageSlots = {}
    self.failedImages = defaultdict( int )
    self.firstPass = True

    self.vo = ''
    self.group = ''
    # self.voGroups contain all the eligible user groups for clouds submitted by this SiteDirector
    self.voGroups = []
    self.cloudDN = ''
    self.cloudGroup = ''
    self.platforms = []
    self.sites = []

    self.proxy = None

    self.updateStatus = True
    self.getOutput = False
    self.sendAccounting = True

  def initialize( self ):
    """ Standard constructor
    """
    return S_OK()

  def beginExecution( self ):

    # The Director is for a particular user community
    self.vo = self.am_getOption( "VO", '' )
    if not self.vo:
      self.vo = CSGlobals.getVO()
    # The SiteDirector is for a particular user group
    self.group = self.am_getOption( "Group", '' )

    # Choose the group for which clouds will be submitted. This is a hack until
    # we will be able to match clouds to VOs.
    if not self.group:
      if self.vo:
        result = Registry.getGroupsForVO( self.vo )
        if not result['OK']:
          return result
        self.voGroups = []
        for group in result['Value']:
          if 'NormalUser' in Registry.getPropertiesForGroup( group ):
            self.voGroups.append( group )
    else:
      self.voGroups = [ self.group ]

    result = findGenericCloudCredentials( vo = self.vo )
    if not result[ 'OK' ]:
      return result
    self.cloudDN, self.cloudGroup = result[ 'Value' ]
    self.maxVMsToSubmit = self.am_getOption( 'MaxVMsToSubmit', 1 )
    self.runningPod = self.am_getOption( 'RunningPod', self.vo)

    # Get the site description dictionary
    siteNames = None
    if not self.am_getOption( 'Site', 'Any' ).lower() == "any":
      siteNames = self.am_getOption( 'Site', [] )
      if not siteNames:
        siteNames = None
    ces = None
    if not self.am_getOption( 'CEs', 'Any' ).lower() == "any":
      ces = self.am_getOption( 'CEs', [] )
      if not ces:
        ces = None

    result = getImages( vo = self.vo,
                        siteList = siteNames )
    if not result['OK']:
      return result
    resourceDict = result['Value']
    result = self.getImages( resourceDict )
    if not result['OK']:
      return result

    #if not siteNames:
    #  siteName = gConfig.getValue( '/DIRAC/Site', 'Unknown' )
    #  if siteName == 'Unknown':
    #    return S_OK( 'No site specified for the SiteDirector' )
    #  else:
    #    siteNames = [siteName]
    #self.siteNames = siteNames

    self.log.always( 'Sites:', siteNames )
    self.log.always( 'CEs:', ces )
    self.log.always( 'CloudDN:', self.cloudDN )
    self.log.always( 'CloudGroup:', self.cloudGroup )

    self.localhost = socket.getfqdn()
    self.proxy = ''

    if self.firstPass:
      if self.imageDict:
        self.log.always( "Agent will serve images:" )
        for queue in self.imageDict:
          self.log.always( "Site: %s, CE: %s, Image: %s" % ( self.imageDict[queue]['Site'],
                                                             self.imageDict[queue]['CEName'],
                                                             queue ) )
    self.firstPass = False
    return S_OK()

  def __generateImageHash( self, imageDict ):
    """ Generate a hash of the queue description
    """
    myMD5 = hashlib.md5()
    myMD5.update( str( imageDict ) )
    hexstring = myMD5.hexdigest()
    return hexstring

  def getImages( self, resourceDict ):
    """ Get the list of relevant CEs and their descriptions
    """

    self.imageDict = {}
    ceFactory = EndpointFactory()

    result = getPilotBootstrapParameters( vo = self.vo, runningPod = self.runningPod )
    if not result['OK']:
      return result
    opParameters = result['Value']

    for site in resourceDict:
      for ce in resourceDict[site]:
        ceDict = resourceDict[site][ce]
        ceTags = ceDict.get( 'Tag', [] )
        if isinstance( ceTags, basestring ):
          ceTags = fromChar( ceTags )
        ceMaxRAM = ceDict.get( 'MaxRAM', None )
        qDict = ceDict.pop( 'Images' )
        for image in qDict:
          imageName = '%s_%s' % ( ce, image )
          self.imageDict[imageName] = {}
          self.imageDict[imageName]['ParametersDict'] = qDict[image]
          self.imageDict[imageName]['ParametersDict']['Image'] = image
          self.imageDict[imageName]['ParametersDict']['Site'] = site
          self.imageDict[imageName]['ParametersDict']['Setup'] = gConfig.getValue( '/DIRAC/Setup', 'unknown' )
          self.imageDict[imageName]['ParametersDict']['CPUTime'] = 99999999
          imageTags = self.imageDict[imageName]['ParametersDict'].get( 'Tag' )
          if imageTags and isinstance( imageTags, basestring ):
            imageTags = fromChar( imageTags )
            self.imageDict[imageName]['ParametersDict']['Tag'] = imageTags
          if ceTags:
            if imageTags:
              allTags = list( set( ceTags + imageTags ) )
              self.imageDict[imageName]['ParametersDict']['Tag'] = allTags
            else:
              self.imageDict[imageName]['ParametersDict']['Tag'] = ceTags

          maxRAM = self.imageDict[imageName]['ParametersDict'].get( 'MaxRAM' )
          maxRAM = ceMaxRAM if not maxRAM else maxRAM
          if maxRAM:
            self.imageDict[imageName]['ParametersDict']['MaxRAM'] = maxRAM

          platform = ''
          if "Platform" in self.imageDict[imageName]['ParametersDict']:
            platform = self.imageDict[imageName]['ParametersDict']['Platform']
          elif "Platform" in ceDict:
            platform = ceDict['Platform']
          if platform and not platform in self.platforms:
            self.platforms.append( platform )

          if not "Platform" in self.imageDict[imageName]['ParametersDict'] and platform:
            result = Resources.getDIRACPlatform( platform )
            if result['OK']:
              self.imageDict[imageName]['ParametersDict']['Platform'] = result['Value'][0]

          ceImageDict = dict( ceDict )
          ceImageDict['CEName'] = ce
          ceImageDict['VO'] = self.vo
          ceImageDict['Image'] = image
          ceImageDict['RunningPod'] = self.runningPod
          ceImageDict['CSServers'] = gConfig.getValue( "/DIRAC/Configuration/Servers", [] )
          ceImageDict.update( self.imageDict[imageName]['ParametersDict'] )
          ceImageDict.update( opParameters )

          # Generate the CE object for the image or pick the already existing one
          # if the image definition did not change
          imageHash = self.__generateImageHash( ceImageDict )
          if imageName in self.imageCECache and self.imageCECache[imageName]['Hash'] == imageHash:
            imageCE = self.imageCECache[imageName]['CE']
          else:
            result = ceFactory.getCEObject( parameters = ceImageDict )
            if not result['OK']:
              return result
            self.imageCECache.setdefault( imageName, {} )
            self.imageCECache[imageName]['Hash'] = imageHash
            self.imageCECache[imageName]['CE'] = result['Value']
            imageCE = self.imageCECache[imageName]['CE']

          self.imageDict[imageName]['CE'] = imageCE
          self.imageDict[imageName]['CEName'] = ce
          self.imageDict[imageName]['CEType'] = ceDict['CEType']
          self.imageDict[imageName]['Site'] = site
          self.imageDict[imageName]['ImageName'] = image
          self.imageDict[imageName]['Platform'] = platform
          self.imageDict[imageName]['MaxInstances'] = ceDict['MaxInstances']
          if not self.imageDict[imageName]['CE'].isValid():
            self.log.error( 'Failed to instantiate CloudEndpoint for %s' % imageName )
            continue

          if site not in self.sites:
            self.sites.append( site )

    return S_OK()

  def execute( self ):
    """ Main execution method
    """

    if not self.imageDict:
      self.log.warn( 'No site defined, exiting the cycle' )
      return S_OK()

    result = self.createVMs()
    if not result['OK']:
      self.log.error( 'Errors in the job submission: ', result['Message'] )

    #cyclesDone = self.am_getModuleParam( 'cyclesDone' )
    #if self.updateStatus and cyclesDone % self.cloudStatusUpdateCycleFactor == 0:
    #  result = self.updatePilotStatus()
    #  if not result['OK']:
    #    self.log.error( 'Errors in updating cloud status: ', result['Message'] )

    return S_OK()

  def createVMs( self ):
    """ Go through defined computing elements and submit jobs if necessary
    """

    # Check that there is some work at all
    setup = CSGlobals.getSetup()
    tqDict = { 'Setup':setup,
               'CPUTime': 9999999 }
    if self.vo:
      tqDict['Community'] = self.vo
    if self.voGroups:
      tqDict['OwnerGroup'] = self.voGroups

    result = Resources.getCompatiblePlatforms( self.platforms )
    if not result['OK']:
      return result
    tqDict['Platform'] = result['Value']
    tqDict['Site'] = self.sites
    tqDict['Tag'] = []
    self.log.verbose( 'Checking overall TQ availability with requirements' )
    self.log.verbose( tqDict )

    rpcMatcher = RPCClient( "WorkloadManagement/Matcher" )
    result = rpcMatcher.getMatchingTaskQueues( tqDict )
    if not result[ 'OK' ]:
      return result
    if not result['Value']:
      self.log.verbose( 'No Waiting jobs suitable for the director' )
      return S_OK()

    jobSites = set()
    anySite = False
    testSites = set()
    totalWaitingJobs = 0
    for tqID in result['Value']:
      if "Sites" in result['Value'][tqID]:
        for site in result['Value'][tqID]['Sites']:
          if site.lower() != 'any':
            jobSites.add( site )
          else:
            anySite = True
      else:
        anySite = True
      if "JobTypes" in result['Value'][tqID]:
        if "Sites" in result['Value'][tqID]:
          for site in result['Value'][tqID]['Sites']:
            if site.lower() != 'any':
              testSites.add( site )
      totalWaitingJobs += result['Value'][tqID]['Jobs']

    tqIDList = result['Value'].keys()

    result = virtualMachineDB.getInstanceCounters( 'Status', {} )
    totalVMs = 0
    if result['OK']:
      for status in result['Value']:
        if status in [ 'New', 'Submitted', 'Running' ]:
          totalVMs += result['Value'][status]
    self.log.info( 'Total %d jobs in %d task queues with %d VMs' % (totalWaitingJobs, len( tqIDList ), totalVMs ) )

    # Check if the site is allowed in the mask
    result = jobDB.getSiteMask()
    if not result['OK']:
      return S_ERROR( 'Can not get the site mask' )
    siteMaskList = result['Value']

    images = self.imageDict.keys()
    random.shuffle( images )
    totalSubmittedPilots = 0
    matchedQueues = 0
    for image in images:

      # Check if the image failed previously
      #failedCount = self.failedImages[ image ] % self.failedImageCycleFactor
      #if failedCount != 0:
      #  self.log.warn( "%s queue failed recently, skipping %d cycles" % ( image, 10-failedCount ) )
      #  self.failedImages[image] += 1
      #  continue

      print "AT >>> image parameters:", image
      for key,value in self.imageDict[image].items():
        print key,value

      ce = self.imageDict[image]['CE']
      ceName = self.imageDict[image]['CEName']
      imageName = self.imageDict[image]['ImageName']
      siteName = self.imageDict[image]['Site']
      platform = self.imageDict[image]['Platform']
      siteMask = siteName in siteMaskList
      endpoint = "%s::%s" % ( siteName, ceName )
      maxInstances = int( self.imageDict[image]['MaxInstances'] )

      if not anySite and siteName not in jobSites:
        self.log.verbose( "Skipping queue %s at %s: no workload expected" % (imageName, siteName) )
        continue
      if not siteMask and siteName not in testSites:
        self.log.verbose( "Skipping queue %s: site %s not in the mask" % (imageName, siteName) )
        continue

      if 'CPUTime' in self.imageDict[image]['ParametersDict'] :
        imageCPUTime = int( self.imageDict[image]['ParametersDict']['CPUTime'] )
      else:
        self.log.warn( 'CPU time limit is not specified for queue %s, skipping...' % image )
        continue

      # Prepare the queue description to look for eligible jobs
      ceDict = ce.getParameterDict()

      if not siteMask:
        ceDict['JobType'] = "Test"
      if self.vo:
        ceDict['VO'] = self.vo
      if self.voGroups:
        ceDict['OwnerGroup'] = self.voGroups


      result = Resources.getCompatiblePlatforms( platform )
      if not result['OK']:
        continue
      ceDict['Platform'] = result['Value']

      # Get the number of eligible jobs for the target site/queue

      print "AT >>> getMatchingTaskQueues ceDict", ceDict

      result = rpcMatcher.getMatchingTaskQueues( ceDict )

      print result

      if not result['OK']:
        self.log.error( 'Could not retrieve TaskQueues from TaskQueueDB', result['Message'] )
        return result
      taskQueueDict = result['Value']
      if not taskQueueDict:
        self.log.verbose( 'No matching TQs found for %s' % image )
        continue

      matchedQueues += 1
      totalTQJobs = 0
      tqIDList = taskQueueDict.keys()
      for tq in taskQueueDict:
        totalTQJobs += taskQueueDict[tq]['Jobs']

      self.log.verbose( '%d job(s) from %d task queue(s) are eligible for %s queue' % (totalTQJobs, len( tqIDList ), image) )

      # Get the number of already instantiated VMs for these task queues
      totalWaitingVMs = 0
      result = virtualMachineDB.getInstanceCounters( 'Status', { 'Endpoint': endpoint } )
      if result['OK']:
        for status in result['Value']:
          if status in [ 'New', 'Submitted' ]:
            totalWaitingVMs += result['Value'][status]
      if totalWaitingVMs >= totalTQJobs:
        self.log.verbose( "%d VMs already for all the available jobs" % totalWaitingVMs )

      self.log.verbose( "%d VMs for the total of %d eligible jobs for %s" % (totalWaitingVMs, totalTQJobs, image) )

      # Get the working proxy
      #cpuTime = imageCPUTime + 86400
      #self.log.verbose( "Getting cloud proxy for %s/%s %d long" % ( self.cloudDN, self.cloudGroup, cpuTime ) )
      #result = gProxyManager.getPilotProxyFromDIRACGroup( self.cloudDN, self.cloudGroup, cpuTime )
      #if not result['OK']:
      #  return result
      #self.proxy = result['Value']
      #ce.setProxy( self.proxy, cpuTime - 60 )

      # Get the number of available slots on the target site/endpoint
      totalSlots = self.getVMInstances( endpoint, maxInstances )
      if totalSlots == 0:
        self.log.debug( '%s: No slots available' % image )
        continue

      vmsToSubmit = max( 0, min( totalSlots, totalTQJobs - totalWaitingVMs ) )
      self.log.info( '%s: Slots=%d, TQ jobs=%d, VMs: %d, to submit=%d' % \
                              ( image, totalSlots, totalTQJobs, totalWaitingVMs, vmsToSubmit ) )

      # Limit the number of clouds to submit to MAX_PILOTS_TO_SUBMIT
      vmsToSubmit = min( self.maxVMsToSubmit, vmsToSubmit )

      self.log.info( 'Going to submit %d VMs to %s queue' % ( vmsToSubmit, image ) )
      result = ce.createInstances( vmsToSubmit )

      print "AT >>> createInstances", result, image

      if not result['OK']:
        self.log.error( 'Failed submission to queue %s:\n' % image, result['Message'] )
        self.failedImages.setdefault( image, 0 )
        self.failedImages[image] += 1
        continue

      # Add VMs to the VirtualMachineDB
      vmDict = result['Value']
      totalSubmittedPilots += len( vmDict )
      self.log.info( 'Submitted %d VMs to %s@%s' % ( len( vmDict ), imageName, ceName ) )

      pilotList = []
      for uuID in vmDict:
        diracUUID = vmDict[uuID]['InstanceID']
        endpoint = '%s::%s' % ( self.imageDict[image]['Site'], ceName )
        result = virtualMachineDB.insertInstance( uuID, imageName, diracUUID, endpoint, self.vo )
        if not result['OK']:
          continue
        for ncpu in range( vmDict[uuID]['NumberOfCPUs'] ):
          pRef = 'vm://' + ceName + '/' + diracUUID + ':' + str( ncpu ).zfill( 2 )
          pilotList.append( pRef )

      stampDict = {}
      tqPriorityList = []
      sumPriority = 0.
      for tq in taskQueueDict:
        sumPriority += taskQueueDict[tq]['Priority']
        tqPriorityList.append( ( tq, sumPriority ) )
      tqDict = {}
      for pilotID in pilotList:
        rndm = random.random() * sumPriority
        for tq, prio in tqPriorityList:
          if rndm < prio:
            tqID = tq
            break
        if not tqDict.has_key( tqID ):
          tqDict[tqID] = []
        tqDict[tqID].append( pilotID )

      for tqID, pilotList in tqDict.items():
        result = pilotAgentsDB.addPilotTQReference( pilotList,
                                                    tqID,
                                                    '',
                                                    '',
                                                    self.localhost,
                                                    'Cloud',
                                                    '',
                                                    stampDict )
        if not result['OK']:
          self.log.error( 'Failed to insert pilots into the PilotAgentsDB' )

    self.log.info( "%d VMs submitted in total in this cycle, %d matched queues" % ( totalSubmittedPilots, matchedQueues ) )
    return S_OK()

  def getVMInstances( self, endpoint, maxInstances ):

    result = virtualMachineDB.getInstanceCounters( 'Status', { 'Endpoint': endpoint } )

    print "AT >>> getVMInstances", result

    if not result['OK']:
      return result

    count = 0
    for status in result['Value']:
      if status in [ 'New', 'Submitted', 'Running']:
        count += int( result['Value'][status] )

    return max( 0, maxInstances - count )
