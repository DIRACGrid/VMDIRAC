########################################################################
# $HeadURL$
# File :   Rocci11.py
########################################################################
# rOCCI wraper for occi 1.1 on opennebula

import os
import time
import simplejson

from subprocess import Popen, PIPE, STDOUT

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.SshContextualize   import SshContextualize
from VMDIRAC.WorkloadManagementSystem.Client.BuildCloudinitScript   import BuildCloudinitScript

__RCSID__ = '$Id: $'

# Classes
###################

class Request:
  """ 
  This class is to perform syncronous and asyncronous request 
  """
  def __init__(self):
    self.stdout = None
    self.stderr = None
    self.returncode = None
    self.pid = None
    self.rlist = []

  def exec_and_wait(self, cmd, timelife = 10):
    """
    exec_and_wait is syncronous with a timelife given by
    parameter whether is reached the command request is returning and error.
    retruncode is an Operating System err code, != 0 are error codes
    """

    proc = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    t_nought = time.time()
    seconds_passed = 0
    self.pid = proc.pid
    self.stderr = proc.stderr
    self.stdout = proc.stdout.read().rstrip('\n')
    self.returncode = proc.poll()
    while(self.returncode != 0 and seconds_passed < timelife):
      seconds_passed = time.time() - t_nought
      self.returncode = proc.poll()

    if seconds_passed >= timelife:
      self.returncode = 1
      self.stdout = "Timelife expired, connection aborted"
      return

    self.returncode = 0
    return

  def exec_no_wait(self, cmd, timelife = 10):
    """
    exec_no_wait is asyncronous request, actually from the point of view of
    the openNebula occi client all occi- like commands are syncronous responding
    to exec_no_wait but most operations in the OpenNebula server are asyncronous
    to know if a command wasr successfull we have a look to stdout depending
    on the exec_no_wait caller.
    using the same Request than exec_and_wait, and maintaing the same returncode
    scheme, controlled by the exec_no_wait caller because no wait has no Operating
    System error code dependencies.
    stderrr and stdout will be selected by the OcciImage caller depending on returncode
    which is controler in the exec_no_wait caller at the level of Rocci11 functions
    """

    proc = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    self.pid = proc.pid
    self.stderr = proc.stderr
    self.stdout = proc.stdout.read().rstrip('\n')
    return


class OcciClient:
  
  def __init__(  self, userCredPath, user, secret, endpointConfig, imageConfig):
    """
    Constructor: uses user / secret authentication for the time being. 
    copy the endpointConfig and ImageConfig dictionaries to the OcciClient

    :Parameters:
      **userCredPath** - `string`
        path to a valid x509 proxy
      **endpointConfig** - `dict`
        dictionary with the endpoint configuration ( WMS.Utilities.Configuration.OcciConfiguration )
      **imageConfig** - `dict`
        dictionary with the image configuration ( WMS.Utilities.Configuration.ImageConfiguration )

    """

    # logger
    self.log = gLogger.getSubLogger( self.__class__.__name__ )

    self.endpointConfig   = endpointConfig
    self.imageConfig      = imageConfig
    self.__userCredPath   = userCredPath
    self.__user           = user
    self.__password       = secret

    if userCredPath is not None:
      self.__authArg = ' --auth x509 --user-cred ' + self.__userCredPath + ' --voms '
    else:
      self.__authArg = ' --auth digest --username %s --password %s ' % (self.__user, self.__password)

  def check_connection(self, timelife = 10):
    """
    This is a way to check the availability of a give URL as occi server
    returning a Request class
    """

    request = Request()
    command = 'occi --endpoint ' + self.endpointConfig['occiURI'] + ' --action list --resource compute ' + self.__authArg
    request.exec_and_wait(command, timelife)
    return request
   
  def create_VMInstance(self, cpuTime = None, submitPool = None, runningPodRequirements = None):
    """
    This creates a VM instance for the given boot image 
    if context method is adhoc then boot image is create to be in Submitted status
    if context method is ssh then boot image is created to be in Wait_ssh_context (for contextualization agent)
    if context method is occi_opennebula context is in hdc image, and also de OCCI context on-the-fly image, taken the given parameters
    Successful creation returns instance id  and the IP
    BTM: submitPool is not used, since rOCCI only has ssh contextualization, then used at this point. It is here by compatibility with OCCI 0.8 call
    """

    # TODO: cpuTime is here to implement HEPiX when ready with rOCCI
    #Comming from running pod specific:
    #self.__strCpuTime = str(cpuTime)

    #DIRAC image context:
    # bootImageName is in current rOCCI driver the OS template
    osTemplateName  = self.imageConfig[ 'bootImageName' ]
    flavorName  = self.imageConfig[ 'flavorName' ]
    contextMethod  = self.imageConfig[ 'contextMethod' ]
    if not ( contextMethod == 'ssh' or contextMethod == 'cloudinit'):
      self.__errorStatus = "Current rOcci DIRAC driver suports cotextMethod: ssh, cloudinit "
      self.log.error( self.__errorStatus )
      return

    occiURI  = self.endpointConfig[ 'occiURI' ]

    vmName = osTemplateName + '_' + contextMethod + '_' + str( time.time() )[0:10]

    request = Request()

    if contextMethod == 'cloudinit':
      cloudinitScript = BuildCloudinitScript();
      result = cloudinitScript.buildCloudinitScript(self.imageConfig, self.endpointConfig, 
        						runningPodRequirements = runningPodRequirements)
      if not result[ 'OK' ]:
        return result
      composedUserdataPath = result[ 'Value' ] 
      self.log.info( "cloudinitScript : %s" % composedUserdataPath )
      with open( composedUserdataPath, 'r' ) as userDataFile: 
        userdata = ''.join( userDataFile.readlines() )

#      print "rocci userdata: "
#      print userdata

      command = 'occi --endpoint ' + occiURI + '  --action create --resource compute --mixin os_tpl#' + osTemplateName + ' --mixin resource_tpl#' + flavorName + ' --attribute occi.core.title="' + vmName + '" --output-format json ' + self.__authArg + ' --context user_data="file://%s"' % composedUserdataPath
#      command = 'occi --endpoint ' + occiURI + '  --action create --resource compute --mixin os_tpl#' + osTemplateName + ' --mixin resource_tpl#' + flavorName + ' --attribute occi.core.title="' + vmName + '" --output-format json ' + self.__authArg + ' --context user_data="%s"' % userdata

    else:
      command = 'occi --endpoint ' + occiURI + '  --action create --resource compute --mixin os_tpl#' + osTemplateName + ' --mixin resource_tpl#' + flavorName + ' --attribute occi.core.title="' + vmName + '" --output-format json ' + self.__authArg

    request.exec_no_wait(command)

    print "command "
    print command

    if request.stdout == "nil":
        request.returncode = 1
        return request

    # FIXME use simplejson, filtering non-json output lines

    searchstr = occiURI + '/compute/'
    first = request.stdout.find(searchstr) 
    if first < 0:
      request.returncode = 1
      return request
    first += len(searchstr)
    last = len(request.stdout)
    iD = request.stdout[first:last]
 
    # giving time sleep to REST API caching the instance to be available:
    time.sleep( 5 )

    # TODO: getting IP should go in a standar way
    # rocci 4.2.5 client is reading stdin for describe, when actually expecting nothing
    # Popen could give ioctl for None or if PIPE then the stdin redirection (-) 
    # for this reason rocci 4.2.5 could not sucessful reply from Popen, giving stdin argument error
    # As workaround I have prepare the following HACK, which is compatible with previous rocci releases:
    # occi command will fail, but I put debug option to occi, and capture stderr to stdout then redirect to a /tmp/[iD]
    command = 'occi -d --endpoint ' + occiURI + '  --action describe --resource /compute/' + iD + ' ' + self.__authArg + ' 2>&1 | grep occi.networkinterface.address >/tmp/' + iD

    request.exec_and_wait(command)

    # continue HACK: I open the occi debug file containing the ip line (actually a couple of lines, only one valid)
    filepath='/tmp/' + iD
    hackstr=''
    with open(filepath) as fp:
      for line in fp:
        hackstr=hackstr + line
    os.remove(filepath)

    # searchstr = '\"networkinterface\":{\"address\":\"'
    searchstr = 'occi.networkinterface.address='
    first = hackstr.find(searchstr) 
    if first < 0:
      request.returncode = 1
      return request
    first += len(searchstr) + 2
    request.returncode = 0
    last = hackstr.find("\"", first) - 1
    publicIP = hackstr[first:last]
    request.stdout = iD + ', ' + publicIP 
    return request
  
  def terminate_VMinstance( self, instanceId ):
    """
    Terminate a VM instance corresponding to the instanceId parameter
    """
    occiURI  = self.endpointConfig[ 'occiURI' ]
    request = Request()
    command = 'occi --endpoint ' + occiURI + '  --action delete --resource /compute/' + instanceId + ' --output-format json ' + self.__authArg

    request.exec_no_wait(command)

    if request.stdout == "nil":
      request.returncode = 1
    else:
      request.returncode = 0

    return request

  def getStatus_VMInstance( self, instanceId ):
    """
    Get the status VM instance for a given VMinstanceId 
    """
    occiURI  = self.endpointConfig[ 'occiURI' ]
    request = Request()
    command = 'occi --endpoint ' + occiURI + '  --action describe --resource /compute/' + instanceId + ' --output-format json ' + self.__authArg

    request.exec_no_wait(command)

    if request.stdout == "nil":
      request.returncode = 1
      return request

    # FIXME use simplejson, filtering non-json output lines

    searchstr = '\"state\":\"'
    first = request.stdout.find(searchstr) 
    if first < 0:
      request.returncode = 1
      return request
    request.returncode = 0
    first += len(searchstr)
    last = request.stdout.find("\"", first) 
    request.stdout = request.stdout[first:last]
    return request

  def contextualize_VMInstance( self, uniqueId, publicIp, cpuTime, submitPool ):
    """
    This method is only used ( at the moment ) by the ssh contextualization method.
    It is called once the vm has been booted.
    </>

    :Parameters:
      **uniqueId** - `string`
        openstack node id ( not uuid ! )
      **publicIp** - `string`
        public IP assigned to the node if any

    :return: S_OK | S_ERROR
    """

    sshContext = SshContextualize()
    return sshContext.contextualise(  self.imageConfig, self.endpointConfig,
                                      uniqueId = uniqueId,
                                      publicIp = publicIp,
                                      cpuTime = cpuTime,
                                      submitPool = submitPool )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
