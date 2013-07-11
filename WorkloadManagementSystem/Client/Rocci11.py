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
  
  def __init__(  self, userCredPath, proxyCaPath, endpointConfig, imageConfig):
    """
    Constructor: uses user / secret authentication for the time being. 
    copy the endpointConfig and ImageConfig dictionaries to the OcciClient

    :Parameters:
      **user** - `string`
        username that will be used on the authentication
      **secret** - `string`
        password used on the authentication
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
    self.__proxyCaPath    = proxyCaPath

  def check_connection(self, timelife = 10):
    """
    This is a way to check the availability of a give URL as occi server
    returning a Request class
    """

    request = Request()
    command = 'occi --endpoint ' + self.endpointConfig['occiURI'] + ' --action list --resource compute --auth x509 --user-cred ' + self.__userCredPath + ' --proxy-ca ' + self.__proxyCaPath
    request.exec_and_wait(command, timelife)
    return request
   
  def create_VMInstance(self, cpuTime):
    """
    This creates a VM instance for the given boot image 
    if context method is adhoc then boot image is create to be in Submitted status
    if context method is ssh then boot image is created to be in Wait_ssh_context (for contextualization agent)
    if context method is occi_opennebula context is in hdc image, and also de OCCI context on-the-fly image, taken the given parameters
    Successful creation returns instance id  and the IP
    """

    # TODO: cpuTime is here to implement HEPiX when ready with rOCCI
    #Comming from running pod specific:
    #self.__strCpuTime = str(cpuTime)

    #DIRAC image context:
    # bootImageName is in current rOCCI driver the OS template
    osTemplateName  = self.imageConfig[ 'bootImageName' ]
    flavorName  = self.imageConfig[ 'flavorName' ]
    contextMethod  = self.imageConfig[ 'contextMethod' ]
    if contextMethod != 'ssh':
      self.__errorStatus = "Current rOcci DIRAC driver suports only ssh contextMethod "
      self.log.error( self.__errorStatus )
      return

    occiURI  = self.endpointConfig[ 'occiURI' ]

    vmName = osTemplateName + '_' + contextMethod + '_' + str( time.time() )[0:10]

    request = Request()

    command = 'occi --endpoint ' + occiURI + '  --action create --resource compute --mixin os_tpl#' + osTemplateName + ' --mixin resource_tpl#' + flavorName + ' --attributes title="' + vmName + '" --output-format json --auth x509 --user-cred ' + self.__userCredPath + ' --proxy-ca ' + self.__proxyCaPath 

    request.exec_no_wait(command)

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


    command = 'occi --endpoint ' + occiURI + '  --action describe --resource /compute/' + iD + ' --output-format json --auth x509 --user-cred ' + self.__userCredPath + ' --proxy-ca ' + self.__proxyCaPath 

    request.exec_no_wait(command)

    searchstr = '\"networkinterface\":{\"address\":\"'
    first = request.stdout.find(searchstr) 
    if first < 0:
      request.returncode = 1
      return request
    first += len(searchstr)
    request.returncode = 0
    last = request.stdout.find("\"", first) 
    publicIP = request.stdout[first:last]
    request.stdout = iD + ', ' + publicIP 
  
  def terminate_VMinstance( self, instanceId ):
    """
    Terminate a VM instance corresponding to the instanceId parameter
    """
    request = Request()
    command = 'occi --endpoint ' + occiURI + '  --action delete --resource /compute/' + instanceId + ' --output-format json --auth x509 --user-cred ' + self.__userCredPath + ' --proxy-ca ' + self.__proxyCaPath 

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
    command = 'occi --endpoint ' + occiURI + '  --action describe --resource /compute/' + instanceId + ' --output-format json --auth x509 --user-cred ' + self.__userCredPath + ' --proxy-ca ' + self.__proxyCaPath 

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

  def contextualize_VMInstance( self, uniqueId, publicIp, cpuTime ):
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
                                      cpuTime = cpuTime )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
