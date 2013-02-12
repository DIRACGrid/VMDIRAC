########################################################################
# $HeadURL$
# File :   Occi09.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################
# subset occi API based in the OpenNebula client command line implemention
# PIC rt 2764 

import os
import subprocess
import sys
import time

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

    """
    exec_and_wait is syncronous with a timelife given by
    parameter whether is reached the command request is returning and error.
    """
    def exec_and_wait(self, cmd, timelife = 10):
      from subprocess import Popen, PIPE, STDOUT

      p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
      t_nought = time.time()
      seconds_passed = 0
      self.pid = p.pid
      self.stderr = p.stderr
      self.stdout = p.stdout.read().rstrip('\n')
      self.returncode = p.poll()
      while(self.returncode != 0 and seconds_passed < timelife):
        seconds_passed = time.time() - t_nought
        self.returncode = p.poll()

      if seconds_passed >= timelife:
        self.returncode = 1
        self.stdout = "Timelife expired, connection aborted"
        return
	
      self.returncode = 0
      return

    """
    exec_no_wait is asyncronous request, actually from the point of view of
    the openNebula occi client all occi- like commands are syncronous responding
    to exec_no_wait but most operations in the OpenNebula server are asyncronous
    to know if a command wasr successfull we have a look to stdout depending
    on the exec_no_wait caller.
    """
    def exec_no_wait(self, cmd, timelife = 10):
      from subprocess import Popen, PIPE, STDOUT

      p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
      self.pid = p.pid
      self.stderr = p.stderr
      self.stdout = p.stdout.read().rstrip('\n')
      return

class OcciClient:
    def __init__(self, URI = None, User = None, Passwd = None):
      self.id = None    
      self.URI = URI    
      self.user = User    
      self.passwd = Passwd    

    """
    This is a way to check the availability of a give URL as occi server
    returning a Request class
    """
    def check_connection(self, timelife = 5):
      request = Request()
      command = 'occi-storage' + ' -U ' + self.user + ' -P ' + self.passwd + ' -R ' + self.URI + ' list'
      request.exec_and_wait(command, timelife)
      return request
   
    """
    The get_image_id function return the corresponding Occi Image Id (OII) for 
    a given imageName on the current occi client self.URI of the occi server.
    """
    def get_image_id(self, imageName):
      request = Request()
      command = 'occi-storage' + ' -U ' + self.user + ' -P ' + self.passwd + ' -R ' + self.URI + ' list ' 
      request.exec_no_wait(command)
      first = request.stdout.find("name='"+imageName+"'") 
      if first < 0:
        request.returncode = 1
        return request
      request.returncode = 0
      first = request.stdout.rfind("<STORAGE ",0,first) 
      first = request.stdout.find("/storage/",first) 
      first = first + 9
      last = request.stdout.find("'",first) 
      request.stdout = request.stdout[first:last]
      return request

    """
    Return the pair (bootImageId, hdcImageId) of a given instanceId
    """ 
    def get_image_ids_of_instance(self, instanceId):
      request = Request()
      command = 'occi-compute' + ' -U ' + self.user + ' -P ' + self.passwd + ' -R ' + self.URI + ' show ' + instanceId 
      request.exec_no_wait(command)
      first = request.stdout.find("/storage/") 
      if first < 0:
        request.returncode = 1
        return request
      request.returncode = 0
      first = first + 9
      last = request.stdout.find("'",first) 
      bootImageId = request.stdout[first:last]
      first = request.stdout.find("/storage/",last) 
      if first < 0:
        request.returncode = 1
        return request
      request.returncode = 0
      first = first + 9
      last = request.stdout.find("'",first) 
      hdcImageId = request.stdout[first:last]
      request.stdout = (bootImageId, hdcImageId)

      return request

    """
    This creates a VM instance for the given boot image and hdc image, and
    also de OCCI context on-the-fly image, taken the given parameters.
    Before the VM instance creation is doing a XML composition
    Successful creation returns instance id  and the IP
    """
    def create_VMInstance( self, bootImageName, hdcImageName, instanceType, imageDriver, bootOII, hdcOII, iface, occiDNS1, occiDNS2, Domain, CVMFS_HTTP_PROXY, occiURLcontextfiles, occiNetId):
      tempXMLname = '/tmp/computeOCCI.%s.xml' % os.getpid()
      tempXML = open(tempXMLname, 'w') 
      
      tempXML.write('<COMPUTE>\n')
      tempXML.write('        <NAME>' + bootImageName + '+' + hdcImageName + '+' + str(time.time())[0:10] + '</NAME>\n')
      tempXML.write('        <INSTANCE_TYPE>' + instanceType + '</INSTANCE_TYPE>\n')
      tempXML.write('        <DISK id="0">\n')
      tempXML.write('                <STORAGE href="' + self.URI + '/storage/' + bootOII + '"/>\n')
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
      if not hdcImageName == 'NO_CONTEXT':
          tempXML.write('        <DISK id="1">\n')
          tempXML.write('                <STORAGE href="' + self.URI + '/storage/' + hdcOII + '"/>\n')
          tempXML.write('                <TYPE>CDROM</TYPE>\n')
          if not imageDriver == 'default':
              if imageDriver == 'qcow2-one-3.2.1':
                  tempXML.write('                <DRIVER>qcow2</DRIVER>\n')
    #          elif imageDriver == 'qcow2-one-3.2.0':
    #              tempXML.write('                <DRIVER type="qcow2"/>\n')
              else:
                  tempXML.write('                <DRIVER>' + imageDriver + '</DRIVER>\n')
          tempXML.write('        </DISK>\n')
      tempXML.write('        <NIC>\n')
      tempXML.write('                <NETWORK href="' + self.URI + '/network/' + occiNetId + '"/>\n')
      tempXML.write('        </NIC>\n')
      tempXML.write('        <CONTEXT>\n')
      tempXML.write('                <VMID>$VMID</VMID>\n')
      tempXML.write('                <IP>$NIC[IP]</IP>\n')
      tempXML.write('                <MAC_ETH0>$NIC[MAC]</MAC_ETH0>\n')
      tempXML.write('                <IFACE>' + iface + '</IFACE>\n')
      if iface == 'static':
          tempXML.write('                <DOMAIN>' + Domain + '</DOMAIN>\n')
          tempXML.write('                <DNS1>' + occiDNS1 + '</DNS1>\n')
          tempXML.write('                <DNS2>' + occiDNS2 + '</DNS2>\n')
      tempXML.write('                <CVMFS_HTTP_PROXY>' + CVMFS_HTTP_PROXY + '</CVMFS_HTTP_PROXY>\n')
      tempXML.write('                <FILES>' + occiURLcontextfiles + '</FILES>\n')
      tempXML.write('        </CONTEXT>\n')
      tempXML.write('</COMPUTE>\n')
      
      tempXML.close()
      #os.system("cat %s"%tempXMLname)

      # debuggin library
      # tempXML = open(tempXMLname, 'r') 
      # resultado = tempXML.read()
      # print  resultado


      request = Request()
      command = 'occi-compute' + ' -U ' + self.user + ' -P ' + self.passwd + ' -R ' + self.URI + ' create ' + tempXMLname
      request.exec_no_wait(command)
      #os.remove(tempXMLname)
      first = request.stdout.find("<ID>") 
      if first < 0:
        request.returncode = 1
        return request
      first += 4
      request.returncode = 0
      last = request.stdout.find("</ID>") 
      id = request.stdout[first:last]
      request.stdout = id 
      return request
				
    """
    Terminate a VM instance corresponding to the instanceId parameter
    """
    def terminate_VMinstance( self, instanceId ):
      request = Request()
      command = 'occi-compute' + ' -U ' + self.user + ' -P ' + self.passwd + ' -R ' + self.URI + ' delete ' + instanceId
      request.exec_no_wait(command)
      if request.stdout == "nil":
        request.returncode = 0
      else:
        request.returncode = 1

      return request

    """
    Get all the VM instances for a given boot image
    """
    def get_all_VMinstances( self, bootImageName ):
      request = Request()
      pattern = "name=\\'" + bootImageName + "+"
      command = 'occi-compute' + ' -U ' + self.user + ' -P ' + self.passwd + ' -R ' + self.URI + ' list '
      request.exec_no_wait(command)

      auxstart = request.stdout.find(pattern) 
      while auxstart >= 0:
        first = request.stdout.rfind("<COMPUTE ",0,auxstart) 
        first = request.stdout.find("/compute/",first) 
        first = first + 9
        last = request.stdout.find("'",first) 
        request.rlist.append( request.stdout[first:last])
	auxstart = auxstart + len(pattern)
        auxstart = request.stdout.find(pattern,auxstart) 

      if request.rlist == []:
        request.returncode = 1
      else:
        request.returncode = 0
      return request

    """
    Get the running VM instances for a given boot image
    """
    def get_running_VMinstances( self, bootImageName ):
      request = Request()
      auxreq = Request()
      pattern = "name=\\'" + bootImageName + "+"
      command = 'occi-compute' + ' -U ' + self.user + ' -P ' + self.passwd + ' -R ' + self.URI + ' list '
      request.exec_no_wait(command)
      
      auxstart = request.stdout.find(pattern) 
      while auxstart >= 0:
        first = request.stdout.rfind("<COMPUTE ",0,auxstart) 
        first = request.stdout.find("/compute/",first) 
        first = first + 9
        last = request.stdout.find("'",first) 
        VMinstanceId = request.stdout[first:last]
        auxreq = self.get_status_VMinstance( VMinstanceId )
        if auxreq.stdout == "ACTIVE":
          request.rlist.append( VMinstanceId )
	auxstart = auxstart + len(pattern)
        auxstart = request.stdout.find(pattern,auxstart) 

      if request.rlist == []:
        request.returncode = 1
      else:
        request.returncode = 0
      return request

    """
    Get the status VM instance for a given VMinstanceId 
    """
    def get_status_VMinstance( self, VMinstanceId ):
      request = Request()
      command = 'occi-compute' + ' -U ' + self.user + ' -P ' + self.passwd + ' -R ' + self.URI + ' show ' + VMinstanceId
      request.exec_no_wait(command)
      first = request.stdout.find("<STATE>") 
      if first < 0:
        request.returncode = 1
        return request
      request.returncode = 0
      first = first + 7
      last = request.stdout.find("</STATE>",first) 
      request.stdout = request.stdout[first:last]
      return request
