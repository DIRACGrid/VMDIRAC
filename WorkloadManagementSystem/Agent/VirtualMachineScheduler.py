########################################################################
# $HeadURL$
# File :   VirtualMachineScheduler.py
# Author : Ricardo Graciani, for single site suport
# Author : Victor Mendez, for multiple endpoints, HEPIX, running pod, occi, etc
# Author : Victor Fernandez, for cloudstack support
########################################################################

"""  The Virtual Machine Scheduler controls the submission of VM via the
     appropriated Directors. These are Backend-specific VMDirector derived classes.
     This is a simple wrapper that performs the instantiation and monitoring
     of the VMDirector instances and add workload to them via ThreadPool
     mechanism.

     From the base Agent class it uses the following configuration Parameters
       - WorkDir:
       - PollingTime:
       - ControlDirectory:
       - MaxCycles:

     The following parameters are searched for in WorkloadManagement/VirtualMachineDirector:
       - ThreadStartDelay:
       - SubmitPools: All the Submit pools that are to be initialized
       - DefaultSubmitPools: If no specific pool is requested, use these
       - VMsPerIteration: Number of VM to instantiate per iteration
       - MaxRunningVMs: Maximum number of VMs to run

     For each backend there should be a section with at least the following:
       - Images: List of available Images
       - [ImageName]: Section with requirements associated to that Image
         (Site, JobType, PilotType, LHCbPlatform, ...), This should be in agreement with what
         is defined in the local configuration of the VM JobAgent: /AgentJobRequirements

     It will use those Directors to submit VMs for each of the Supported SubmitPools
       - SubmitPools (see above)

     SubmitPools may refer to:
       - a full Cloud provider (AmazonEC2, Occi, CloudStack)
       - a locally accessible Xen or KVM server

     For every Cloud SubmitPool there must be a corresponding Section with the necessary parameters:

       - Pool: if a dedicated Threadpool is desired for this SubmitPool
       - RunningPods: a list with the pods between DIRAC images and cloud endpoints for a run

     The DIRAC VM is configured based in three statements with their options

       - RunningPods: relates a single DIRAC VM with their VM scheduler options and the cloud ends for a run
       - Images: bootstrap image, context image and URL context files, the HEPIX of a DIRAC VM
       - CloudEndpoints: With the driver and the on-the-fly context image

      The VM submission logic is as follows:

        For each configured backend and VM image:

        - Determine the pending workload that could be executed by the VM instance.

        - Check number of already running instances.

        - Require Instantiation of a new VM for each "required" type.

        - Report the sum of the Target number of VMs to be instantiated.

        - Wait until the ThreadPool is empty.

        - Report the actual number of VMs instantiated.

        In summary:

        All VMs are considered on every iteration, so the same build up factor applies to each Cloud

        All TaskQueues are considered on every iteration, pilots are submitted
        statistically proportional to the priority and the Number of waiting tasks
        of the TaskQueue, boosted for the TaskQueues with lower CPU requirements and
        limited by the difference between the number of waiting jobs and the number of
        already waiting pilots.


      This module is prepared to work:
       - locally to the WMS DIRAC server and connect directly to the necessary DBs.
       - remotely to the WMS DIRAC server and connect via appropriated DISET methods.


"""

import random, time
import DIRAC

from numpy.random import poisson
from random       import shuffle

# DIRAC
from DIRAC                                             import gConfig
from DIRAC.Core.Base.AgentModule                       import AgentModule
from DIRAC.Core.Utilities.ThreadPool                   import ThreadPool
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB     import maxCPUSegments
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import taskQueueDB

from VMDIRAC.Resources.Cloud.CloudDirector               import CloudDirector
from VMDIRAC.WorkloadManagementSystem.Client.ServerUtils import virtualMachineDB
#from VMDIRAC.WorkloadManagementSystem.private.KVMDirector   import KVMDirector

__RCSID__ = "$Id: $"

#FIXME: why do we need the random seed ?
random.seed()

class VirtualMachineScheduler( AgentModule ):

  def initialize( self ):
    """ Standard constructor
    """
    import threading

    self.am_setOption( "PollingTime", 60.0 )

    self.am_setOption( "VMsPerIteration", 1 )
    self.am_setOption( "MaxRunningVMs", 1 )

    self.am_setOption( "ThreadStartDelay", 1 )
    self.am_setOption( "SubmitPools", [] )
    self.am_setOption( "DefaultSubmitPools", [] )


    self.am_setOption( "minThreadsInPool", 0 )
    self.am_setOption( "maxThreadsInPool", 2 )
    self.am_setOption( "totalThreadsInPool", 40 )

    self.directors = {}
    self.pools     = {}

    self.directorDict = {}

    self.callBackLock = threading.Lock()

    return DIRAC.S_OK()

  def execute( self ):
    """Main Agent code:
      1.- Query TaskQueueDB for existing TQs
      2.- Count Pending Jobs
      3.- Submit VMs
    """

    self.__checkSubmitPools()

    imagesToSubmit = {}

    for directorName, directorDict in self.directors.items():
      self.log.verbose( 'Checking Director:', directorName )
      for runningPodName in directorDict['director'].runningPods:
        runningPodDict = directorDict['director'].runningPods[runningPodName]
        imageName = runningPodDict['Image']
        instances = 0
        result = virtualMachineDB.getInstancesByStatus( 'Running' )
        if result['OK'] and imageName in result['Value']:
          instances += len( result['Value'][imageName] )
        result = virtualMachineDB.getInstancesByStatus( 'Submitted' )
        if result['OK'] and imageName in result['Value']:
          instances += len( result['Value'][imageName] )
        result = virtualMachineDB.getInstancesByStatus( 'Wait_ssh_context' )
        if result['OK'] and imageName in result['Value']:
          instances += len( result['Value'][imageName] )
        result = virtualMachineDB.getInstancesByStatus( 'Contextualizing' )
        if result['OK'] and imageName in result['Value']:
          instances += len( result['Value'][imageName] )
        self.log.verbose( 'Checking Image %s:' % imageName, instances )
        maxInstances = runningPodDict['MaxInstances']
        if instances >= maxInstances:
          self.log.info( '%s >= %s Running instances reach MaxInstances for runningPod: %s, skipping' % ( instances, maxInstances, runningPodName ) )
          continue

        endpointFound = False
        cloudEndpointsStr = runningPodDict['CloudEndpoints']
	      # random
        cloudEndpoints = [element for element in cloudEndpointsStr.split( ',' )]
        shuffle( cloudEndpoints )
        self.log.info( 'cloudEndpoints random failover: %s' % cloudEndpoints )
        numVMsToSubmit = {}
        for endpoint in cloudEndpoints:
          self.log.info( 'Checking to submit to: %s' % endpoint )
          strMaxEndpointInstances = gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( endpoint, 'maxEndpointInstances' ), "" )
          if not strMaxEndpointInstances:
            self.log.info( 'CS CloudEndpoint %s has no define maxEndpointInstances option' % endpoint )
            continue
          self.log.info( 'CS CloudEndpoint %s maxEndpointInstance: %s' % (endpoint,strMaxEndpointInstances) )

          vmPolicy = gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( endpoint, 'vmPolicy' ), "" )
          if not vmPolicy:
            self.log.info( 'CS CloudEndpoint %s has no define vmPolicy option' % endpoint )
            continue
          self.log.info( 'CS CloudEndpoint %s vmPolicy: %s' % (endpoint,vmPolicy) )

          endpointInstances = 0
          result = virtualMachineDB.getInstancesByStatusAndEndpoint( 'Running', endpoint )
          if result['OK'] and imageName in result['Value']:
            endpointInstances += len( result['Value'][imageName] )
          result = virtualMachineDB.getInstancesByStatusAndEndpoint( 'Submitted', endpoint )
          if result['OK'] and imageName in result['Value']:
            endpointInstances += len( result['Value'][imageName] )
          result = virtualMachineDB.getInstancesByStatusAndEndpoint( 'Wait_ssh_context', endpoint )
          if result['OK'] and imageName in result['Value']:
            endpointInstances += len( result['Value'][imageName] )
          result = virtualMachineDB.getInstancesByStatusAndEndpoint( 'Contextualizing', endpoint )
          if result['OK'] and imageName in result['Value']:
            endpointInstances += len( result['Value'][imageName] )
          self.log.info( 'CS CloudEndpoint %s instances: %s, maxEndpointInstances: %s' % (endpoint,endpointInstances,strMaxEndpointInstances) )
          maxEndpointInstances = int(strMaxEndpointInstances)
          if endpointInstances < maxEndpointInstances:
            if vmPolicy == 'elastic':
              numVMs = 1
            if vmPolicy == 'static':
              numVMs = maxEndpointInstances - endpointInstances
            numVMsToSubmit.update({str(endpoint): int(numVMs) })
            endpointFound = True
            break

        if not endpointFound:
          self.log.info( 'Skipping, from list %s; there is no endpoint with free slots found for image %s' % ( runningPodDict['CloudEndpoints'], imageName ) )
          continue

        imageRequirementsDict = runningPodDict['RequirementsDict']
        #self.log.info( 'Image Requirements Dict: ', imageRequirementsDict )
        result = taskQueueDB.getMatchingTaskQueues( imageRequirementsDict )
        if not result['OK']:
          self.log.error( 'Could not retrieve TaskQueues from TaskQueueDB', result['Message'] )
          return result
        taskQueueDict = result['Value']
        #self.log.info( 'Task Queues Dict: ', taskQueueDict )
        jobs = 0
        priority = 0
        cpu = 0
        for tq in taskQueueDict:
          jobs += taskQueueDict[tq]['Jobs']
          priority += taskQueueDict[tq]['Priority']
          cpu += taskQueueDict[tq]['Jobs'] * taskQueueDict[tq]['CPUTime']

        if not jobs:
          self.log.info( 'No matching jobs for %s found, skipping' % imageName )
          continue

        if instances and ( cpu / instances ) < runningPodDict['CPUPerInstance']:
          self.log.info( 'Waiting CPU per Running instance %s < %s, skipping' % ( cpu / instances, runningPodDict['CPUPerInstance'] ) )
          continue

        if directorName not in imagesToSubmit:
          imagesToSubmit[directorName] = {}
        if imageName not in imagesToSubmit[directorName]:
          imagesToSubmit[directorName][imageName] = {}
        numVMs = numVMsToSubmit.get( endpoint )
        imagesToSubmit[directorName][imageName] = { 'Jobs': jobs,
                                                    'TQPriority': priority,
                                                    'CPUTime': cpu,
                                                    'CloudEndpoint': endpoint,
                                                    'NumVMsToSubmit': numVMs,
                                                    'VMPolicy': vmPolicy,
                                                    'RunningPodName': runningPodName,
                                                    'VMPriority': runningPodDict['Priority'] }

    for directorName, imageOfJobsToSubmitDict in imagesToSubmit.items():
      for imageName, jobsToSubmitDict in imageOfJobsToSubmitDict.items():
        if self.directors[directorName]['isEnabled']:
          self.log.info( 'Requesting submission of %s to %s' % ( imageName, directorName ) )

          director = self.directors[directorName]['director']
          pool = self.pools[self.directors[directorName]['pool']]

          endpoint = jobsToSubmitDict['CloudEndpoint']
          runningPodName = jobsToSubmitDict['RunningPodName']
          numVMs = jobsToSubmitDict['NumVMsToSubmit']

          ret = pool.generateJobAndQueueIt( director.submitInstance,
                                            args = ( imageName, endpoint, numVMs, runningPodName ),
                                            oCallback = self.callBack,
                                            oExceptionCallback = director.exceptionCallBack,
                                            blocking = False )

          if not ret['OK']:
            # Disable submission until next iteration
            self.directors[directorName]['isEnabled'] = False
          else:
            time.sleep( self.am_getOption( 'ThreadStartDelay' ) )

    if 'Default' in self.pools:
      # only for those in "Default' thread Pool
      # for pool in self.pools:
      self.pools['Default'].processAllResults()

    return DIRAC.S_OK()

  def submitPilotsForTaskQueue( self, taskQueueDict, waitingPilots ):    

    taskQueueID = taskQueueDict['TaskQueueID']
    maxCPU = maxCPUSegments[-1]
    extraPilotFraction = self.am_getOption( 'extraPilotFraction' )
    extraPilots = self.am_getOption( 'extraPilots' )

    taskQueuePriority = taskQueueDict['Priority']
    self.log.verbose( 'Priority for TaskQueue %s:' % taskQueueID, taskQueuePriority )
    taskQueueCPU = max( taskQueueDict['CPUTime'], self.am_getOption( 'lowestCPUBoost' ) )
    self.log.verbose( 'CPUTime  for TaskQueue %s:' % taskQueueID, taskQueueCPU )
    taskQueueJobs = taskQueueDict['Jobs']
    self.log.verbose( 'Jobs in TaskQueue %s:' % taskQueueID, taskQueueJobs )

    # Determine number of pilots to submit, boosting TaskQueues with low CPU requirements
    #pilotsToSubmit = poisson( ( self.pilotsPerPriority * taskQueuePriority +
    #                            self.pilotsPerJob * taskQueueJobs ) * maxCPU / taskQueueCPU )
    pilotsToSubmit = poisson( ( taskQueuePriority +
                                taskQueueJobs ) * maxCPU / taskQueueCPU )
    # limit the number of pilots according to the number of waiting job in the TaskQueue
    # and the number of already submitted pilots for that TaskQueue
    pilotsToSubmit = min( pilotsToSubmit, int( ( 1 + extraPilotFraction ) * taskQueueJobs ) + extraPilots - waitingPilots )

    if pilotsToSubmit <= 0:
      return DIRAC.S_OK( 0 )
    self.log.verbose( 'Submitting %s pilots for TaskQueue %s' % ( pilotsToSubmit, taskQueueID ) )

    return self.__submitPilots( taskQueueDict, pilotsToSubmit )

  def __submitPilots( self, taskQueueDict, pilotsToSubmit ):
    """
      Try to insert the submission in the corresponding Thread Pool, disable the Thread Pool
      until next iteration once it becomes full
    """
    # Check if an specific MiddleWare is required
    if 'SubmitPools' in taskQueueDict:
      submitPools = taskQueueDict[ 'SubmitPools' ]
    else:
      submitPools = self.am_getOption( 'DefaultSubmitPools' )
    submitPools = DIRAC.List.randomize( submitPools )

    for submitPool in submitPools:
      self.log.verbose( 'Trying SubmitPool:', submitPool )

      if not submitPool in self.directors or not self.directors[submitPool]['isEnabled']:
        self.log.verbose( 'Not Enabled' )
        continue

      pool = self.pools[self.directors[submitPool]['pool']]
      director = self.directors[submitPool]['director']
      ret = pool.generateJobAndQueueIt( director.submitPilots,
                                        args = ( taskQueueDict, pilotsToSubmit, self.workDir ),
                                        oCallback = self.callBack,
                                        oExceptionCallback = director.exceptionCallBack,
                                        blocking = False )
      if not ret['OK']:
        # Disable submission until next iteration
        self.directors[submitPool]['isEnabled'] = False
      else:
        time.sleep( self.am_getOption( 'ThreadStartDelay' ) )
        break

    return DIRAC.S_OK( pilotsToSubmit )

  def __checkSubmitPools( self ):
    # this method is called at initialization and at the beginning of each execution cycle
    # in this way running parameters can be dynamically changed via the remote
    # configuration.

    # First update common Configuration for all Directors
    self.__configureDirector()

    # Now we need to initialize one thread for each Director in the List,
    # and check its configuration:
    for submitPool in self.am_getOption( 'SubmitPools' ):
      # check if the Director is initialized, then reconfigure
      if submitPool not in self.directors:
        # instantiate a new Director
        self.__createDirector( submitPool )

      self.__configureDirector( submitPool )

      # Now enable the director for this iteration, if some RB/WMS/CE is defined
      if submitPool in self.directors:
        if 'resourceBrokers' in dir( self.directors[submitPool]['director'] ) and self.directors[submitPool]['director'].resourceBrokers:
          self.directors[submitPool]['isEnabled'] = True
        if 'computingElements' in dir( self.directors[submitPool]['director'] ) and self.directors[submitPool]['director'].computingElements:
          self.directors[submitPool]['isEnabled'] = True

    # Now remove directors that are not Enable (they have been used but are no
    # longer required in the CS).
    pools = []
    for submitPool in self.directors.keys():
      if not self.directors[submitPool]['isEnabled']:
        self.log.info( 'Deleting Director for SubmitPool:', submitPool )
        director = self.directors[submitPool]['director']
        del self.directors[submitPool]
        del director
      else:
        pools.append( self.directors[submitPool]['pool'] )

    # Finally delete ThreadPools that are no longer in use
    for pool in self.pools:
      if pool != 'Default' and not pool in pools:
        pool = self.pools.pop( pool )
        # del self.pools[pool]
        del pool

  def __createDirector( self, submitPool ):
    """
     Instantiate a new VMDirector for the given SubmitPool
    """

    self.log.info( 'Creating Director for SubmitPool:', submitPool )
    # 1. get the CloudDirector

    director = CloudDirector( submitPool )
    directorName = '%sDirector' % submitPool

    self.log.info( 'Director Object instantiated:', directorName )

    # 2. check the requested ThreadPool (if not defined use the default one)
    directorPool = self.am_getOption( submitPool + '/Pool', 'Default' )
    if not directorPool in self.pools:
      self.log.info( 'Adding Thread Pool:', directorPool )
      poolName = self.__addPool( directorPool )
      if not poolName:
        self.log.error( 'Can not create Thread Pool:', directorPool )
        return

    # 3. add New director
    self.directors[ submitPool ] = { 'director': director,
                                     'pool': directorPool,
                                     'isEnabled': False,
                                   }

    self.log.verbose( 'Created Director for SubmitPool', submitPool )

    return

  def __configureDirector( self, submitPool = None ):
    # Update Configuration from CS
    # if submitPool == None then,
    #     disable all Directors
    # else
    #    Update Configuration for the VMDirector of that SubmitPool
    if submitPool == None:
      self.workDir = self.am_getOption( 'WorkDirectory' )
      # By default disable all directors
      for director in self.directors:
        self.directors[director]['isEnabled'] = False

    else:
      if submitPool not in self.directors:
        DIRAC.abort( -1, "Submit Pool not available", submitPool )
      director = self.directors[submitPool]['director']

      # Pass reference to our CS section so that defaults can be taken from there
      director.configure( self.am_getModuleParam( 'section' ), submitPool )

      # Enable director for pilot submission
      self.directors[submitPool]['isEnabled'] = True

  def __addPool( self, poolName ):
    # create a new thread Pool, by default it has 2 executing threads and 40 requests
    # in the Queue

    if not poolName:
      return None
    if poolName in self.pools:
      return None
    pool = ThreadPool( self.am_getOption( 'minThreadsInPool' ),
                       self.am_getOption( 'maxThreadsInPool' ),
                       self.am_getOption( 'totalThreadsInPool' ) )
    # Daemonize except "Default" pool
    if poolName != 'Default':
      pool.daemonize()
    self.pools[poolName] = pool
    return poolName

  def callBack( self, threadedJob, submitResult ):
    if not submitResult['OK']:
      self.log.error( 'submitInstance Failed: ', submitResult['Message'] )
      if 'Value' in submitResult:
        self.callBackLock.acquire()
        self.callBackLock.release()
    else:
      self.log.info( 'New Instance Submitted' )
      self.callBackLock.acquire()
      self.callBackLock.release()


