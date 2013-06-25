########################################################################
# $HeadURL$
# File :   Occi09.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################
# subset occi API based in the OpenNebula client command line implemention
# PIC rt 2764 

import os
import time

from subprocess import Popen, PIPE, STDOUT

# DIRAC
from DIRAC import gLogger, S_OK, S_ERROR

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
    """

    proc = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    self.pid = proc.pid
    self.stderr = proc.stderr
    self.stdout = proc.stdout.read().rstrip('\n')
    return


class OcciClient:
  
  def __init__(  self, user, secret, endpointConfig, imageConfig):
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

    self.endpointConfig = endpointConfig
    self.imageConfig    = imageConfig
    self.__user           = user
    self.__password       = secret

  def check_connection(self, timelife = 5):
    """
    This is a way to check the availability of a give URL as occi server
    returning a Request class
    """

    request = Request()
    command = 'occi-storage' + ' -U ' + self.__user + ' -P ' + self.__password + ' -R ' + self.endpointConfig['occiURI'] + ' list'
    request.exec_and_wait(command, timelife)
    return request
   
  def get_image_id(self, imageName):
    """
    The get_image_id function return the corresponding Occi Image Id (OII) for 
    a given imageName on the current occi client self.URI of the occi server.
    """

    request = Request()
    command = 'occi-storage' + ' -U ' + self.__user + ' -P ' + self.__password + ' -R ' + self.endpointConfig['occiURI'] + ' list ' 
    request.exec_no_wait(command)
    first = request.stdout.find("name='"+imageName+"'") 
    if first < 0:
      request.returncode = 1
      return request
    request.returncode = 0
    first = request.stdout.rfind("<STORAGE ", 0, first) 
    first = request.stdout.find("/storage/", first) 
    first = first + 9
    last = request.stdout.find("'", first) 
    request.stdout = request.stdout[first:last]
    return request

  def get_image_ids_of_instance(self, instanceId):
    """
    Return the pair (bootImageId, hdcImageId) of a given instanceId
    """ 

    request = Request()
    command = 'occi-compute' + ' -U ' + self.__user + ' -P ' + self.__password + ' -R ' + self.endpointConfig['occiURI'] + ' show ' + instanceId 
    request.exec_no_wait(command)
    first = request.stdout.find("/storage/") 
    if first < 0:
      request.returncode = 1
      return request
    request.returncode = 0
    first = first + 9
    last = request.stdout.find("'", first) 
    bootImageId = request.stdout[first:last]
    first = request.stdout.find("/storage/", last) 
    if first < 0:
      request.returncode = 1
      return request
    request.returncode = 0
    first = first + 9
    last = request.stdout.find("'", first) 
    hdcImageId = request.stdout[first:last]
    request.stdout = (bootImageId, hdcImageId)

    return request

  def create_VMInstance(self, cpuTime):
    """
    This creates a VM instance for the given boot image 
    if context method is adhoc then boot image is create to be in Submitted status
    if context method is ssh then boot image is created to be in Wait_ssh_context (for contextualization agent)
    if context method is occi_opennebula context is in hdc image, and also de OCCI context on-the-fly image, taken the given parameters
    Successful creation returns instance id  and the IP
    """

    #Comming from running pod specific:
    strCpuTime = str(cpuTime)

    #DIRAC image context:
    bootImageName  = self.imageConfig[ 'bootImageName' ]
    flavorName  = self.imageConfig[ 'flavorName' ]
    contextMethod  = self.imageConfig[ 'contextMethod' ]
    hdcImageName  = self.imageConfig[ 'contextConfig' ].get( 'hdcImageName' , None )
    context_files_url  = self.imageConfig[ 'contextConfig' ].get( 'context_files_url' , None )

    # endpoint context:
    siteName  = self.endpointConfig[ 'siteName' ]
    cloudDriver  = self.endpointConfig[ 'cloudDriver' ]
    occiURI  = self.endpointConfig[ 'occiURI' ]
    imageDriver  = self.endpointConfig[ 'imageDriver' ]
    vmStopPolicy  = self.endpointConfig[ 'vmStopPolicy' ]
    netId  = self.endpointConfig[ 'netId' ]
    cvmfs_http_proxy  = self.endpointConfig[ 'cvmfs_http_proxy' ]
    iface  = self.endpointConfig[ 'iface' ]
    if iface == 'static':
      dns1  = self.endpointConfig[ 'dns1' ]
      dns2  = self.endpointConfig[ 'dns2' ]
      domain  = self.endpointConfig[ 'domain' ]

    #Get the boot Occi Image Id (OII) from URI server
    request = self.get_image_id( bootImageName )
    if request.returncode != 0:
      self.__errorStatus = "Can't get the boot image id for %s from server %s\n%s" % (bootImageName, occiURI, request.stdout)
      self.log.error( self.__errorStatus )
      return
    bootOII = request.stdout
    
    if contextMethod == 'occi_opennebula':
      #Get the hdc Occi Image Id (OII) from URI server
      request = self.get_image_id( hdcImageName )
      if request.returncode != 0:
        self.__errorStatus = "Can't get the contextual image id for %s from server %s\n%s" % (self.__hdcImageName, self.__occiURI, request.stdout)
        self.log.error( self.__errorStatus )
        return
      hdcOII = request.stdout

    if contextMethod == 'occi_opennebula':
      vm_name = bootImageName + '_' + hdcImageName + '_' + str( time.time() )[0:10]
    else:
      vm_name = bootImageName + '_' + contextMethod + '_' + str( time.time() )[0:10]

    tempXMLname = '/tmp/computeOCCI.%s.xml' % os.getpid()
    tempXML = open(tempXMLname, 'w') 
      
    tempXML.write('<COMPUTE>\n')
    tempXML.write('        <NAME>' + vm_name + '</NAME>\n')
    tempXML.write('        <INSTANCE_TYPE>' + flavorName + '</INSTANCE_TYPE>\n')
    tempXML.write('        <DISK id="0">\n')
    tempXML.write('                <STORAGE href="' + occiURI + '/storage/' + bootOII + '"/>\n')
    tempXML.write('                <TYPE>OS</TYPE>\n')
    tempXML.write('                <TARGET>hda</TARGET>\n')
    if not imageDriver == 'default':
      if imageDriver == 'qcow2-one-3.2.1':
        tempXML.write('                <DRIVER>qcow2</DRIVER>\n')
      elif imageDriver == 'qcow2-one-3.2.0':
        tempXML.write('                <DRIVER type="qcow2"/>\n')
      else:
        tempXML.write('                <DRIVER>' + imageDriver + '</DRIVER>\n')
    tempXML.write('        </DISK>\n')

    if contextMethod == 'occi_opennebula':
      tempXML.write('        <DISK id="1">\n')
      tempXML.write('                <STORAGE href="' + occiURI + '/storage/' + hdcOII + '"/>\n')
      tempXML.write('                <TYPE>CDROM</TYPE>\n')
      if not imageDriver == 'default':
        if imageDriver == 'qcow2-one-3.2.1':
          tempXML.write('                <DRIVER>qcow2</DRIVER>\n')
        elif imageDriver == 'qcow2-one-3.2.0':
          tempXML.write('                <DRIVER type="qcow2"/>\n')
        else:
          tempXML.write('                <DRIVER>' + imageDriver + '</DRIVER>\n')
      tempXML.write('        </DISK>\n')

    tempXML.write('        <NIC>\n')
    tempXML.write('                <NETWORK href="' + occiURI + '/network/' + netId + '"/>\n')
    tempXML.write('        </NIC>\n')
    if contextMethod == 'occi_opennebula':
      tempXML.write('        <CONTEXT>\n')
      tempXML.write('                <VMID>$VMID</VMID>\n')
      tempXML.write('                <IP>$NIC[IP]</IP>\n')
      tempXML.write('                <MAC_ETH0>$NIC[MAC]</MAC_ETH0>\n')
      tempXML.write('                <IFACE>' + iface + '</IFACE>\n')

      if iface == 'static':
        tempXML.write('                <DOMAIN>' + domain + '</DOMAIN>\n')
        tempXML.write('                <DNS1>' + dns1 + '</DNS1>\n')
        tempXML.write('                <DNS2>' + dns2 + '</DNS2>\n')

      tempXML.write('                <CVMFS_HTTP_PROXY>' + cvmfs_http_proxy + '</CVMFS_HTTP_PROXY>\n')
      tempXML.write('                <SITE_NAME>' + siteName + '</SITE_NAME>\n')
      tempXML.write('                <CLOUD_DRIVER>' + cloudDriver + '</CLOUD_DRIVER>\n')
      tempXML.write('                <CPU_TIME>' + strCpuTime + '</CPU_TIME>\n')
      tempXML.write('                <VM_STOP_POLICY>' + vmStopPolicy + '</VM_STOP_POLICY>\n')
      tempXML.write('                <FILES>' + context_files_url + '</FILES>\n')
      tempXML.write('        </CONTEXT>\n')

    tempXML.write('</COMPUTE>\n')
      
    tempXML.close()
    #os.system("cat %s"%tempXMLname)

    # debuggin library
    # tempXML = open(tempXMLname, 'r') 
    # resultado = tempXML.read()
    # print  resultado


    request = Request()
    command = 'occi-compute' + ' -U ' + self.__user + ' -P ' + self.__password + ' -R ' + occiURI + ' create ' + tempXMLname
    request.exec_no_wait(command)
    #os.remove(tempXMLname)
    first = request.stdout.find("<ID>") 
    if first < 0:
      request.returncode = 1
      return request
    first += 4
    request.returncode = 0
    last = request.stdout.find("</ID>") 
    iD = request.stdout[first:last]
    request.stdout = iD 
    return request
  
  def terminate_VMinstance( self, instanceId ):
    """
    Terminate a VM instance corresponding to the instanceId parameter
    """
    request = Request()
    command = 'occi-compute' + ' -U ' + self.__user + ' -P ' + self.__password + ' -R ' + self.endpointConfig['occiURI'] + ' delete ' + instanceId
    request.exec_no_wait(command)
    if request.stdout == "nil":
      request.returncode = 0
    else:
      request.returncode = 1

    return request

  def get_all_VMinstances( self, bootImageName ):
    """
    Get all the VM instances for a given boot image
    """

    request = Request()
    pattern = "name=\\'" + bootImageName + "+"
    command = 'occi-compute' + ' -U ' + self.__user + ' -P ' + self.__password + ' -R ' + self.endpointConfig['occiURI'] + ' list '
    request.exec_no_wait(command)

    auxstart = request.stdout.find(pattern) 
    while auxstart >= 0:
      first = request.stdout.rfind("<COMPUTE ", 0, auxstart) 
      first = request.stdout.find("/compute/", first) 
      first = first + 9
      last = request.stdout.find("'", first) 
      request.rlist.append( request.stdout[first:last])
      auxstart = auxstart + len(pattern)
      auxstart = request.stdout.find(pattern, auxstart) 

    if request.rlist == []:
      request.returncode = 1
    else:
      request.returncode = 0
    return request

  def get_running_VMinstances( self, bootImageName ):
    """
    Get the running VM instances for a given boot image
    """

    request = Request()
    auxreq = Request()
    pattern = "name=\\'" + bootImageName + "+"
    command = 'occi-compute' + ' -U ' + self.__user + ' -P ' + self.__password + ' -R ' + self.endpointConfig['occiURI'] + ' list '
    request.exec_no_wait(command)
      
    auxstart = request.stdout.find(pattern) 
    while auxstart >= 0:
      first = request.stdout.rfind("<COMPUTE ", 0, auxstart) 
      first = request.stdout.find("/compute/", first) 
      first = first + 9
      last = request.stdout.find("'", first) 
      vmInstanceId = request.stdout[first:last]
      auxreq = self.get_status_VMinstance( vmInstanceId )
      if auxreq.stdout == "ACTIVE":
        request.rlist.append( vmInstanceId )
      auxstart = auxstart + len(pattern)
      auxstart = request.stdout.find(pattern, auxstart) 

    if request.rlist == []:
      request.returncode = 1
    else:
      request.returncode = 0
    return request

  def get_status_VMinstance( self, VMinstanceId ):
    """
    Get the status VM instance for a given VMinstanceId 
    """
    request = Request()
    command = 'occi-compute' + ' -U ' + self.__user + ' -P ' + self.__password + ' -R ' + self.endpointConfig['occiURI'] + ' show ' + VMinstanceId
    request.exec_no_wait(command)
    first = request.stdout.find("<STATE>") 
    if first < 0:
      request.returncode = 1
      return request
    request.returncode = 0
    first = first + 7
    last = request.stdout.find("</STATE>", first) 
    request.stdout = request.stdout[first:last]
    return request

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
