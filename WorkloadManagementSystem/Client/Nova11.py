########################################################################
# $HeadURL$
# File :   Nova11.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################
# DIRAC driver to nova v_1.1 endpoint using libcloud and pyton-novaclient
# TODO: frist release only with user/passd, to implement proxy auth with VOMS

import os
import subprocess
import sys
import time

import libcloud.security

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

from novaclient.v1_1 import client

import paramiko 

# Classes
###################

class Request():
      image = None
      VMnode = None
      public_ip = None
      stderr = None
      status = None
      returncode = 0

class NovaClient:
    def __init__(self, osAuthURL=None , osUserName = None, osPasswd = None, osTenantName = None , osBaseURL = None, osServiceRegion = None):
      self.osServiceRegion = osServiceRegion    
      cloudManagerAPI = get_driver(Provider.OPENSTACK)
      self.driver = cloudManagerAPI(osUserName, osPasswd,
                ex_force_auth_url=osAuthURL,
                ex_force_service_region=osServiceRegion,
                ex_force_auth_version='2.0_password',
                ex_tenant_name=osTenantName)
     
      # mofify to insecure=False when ca cert ready
      self.pynovaclient = client.Client(username=osUserName, api_key=osPasswd, project_id=osTenantName, auth_url=osAuthURL, insecure=True, region_name=osServiceRegion, auth_system='keystone')


    """
    This is a way to check the availability of a give URL as occi server
    returning a Request class
    """
    def check_connection(self):
      request = Request()
      try:
          images = self.driver.list_images()
      except Exception, errmsg:
          request.stderr = errmsg
          request.returncode = -1
      return request
  
 
    """
    The get_image_id function return the corresponding openstack id
    a given imageName on the current occi client self.URI of the occi server.
    """
    def get_image(self, imageName):
      request = Request()
      try:
          images = self.driver.list_images()
          request.image = [i for i in images if i.name == imageName ][0]
      except Exception, errmsg:
          request.stderr = errmsg
          request.returncode = -1

      return request

    """
    This creates a VM instance for the given boot image 
    and creates a context script, taken the given parameters.
    Successful creation returns instance VM 
    """
    def create_VMInstance( self, bootImageName, contextMethod, flavorName, bootImage, ipPool ):
      request = Request()
      vm_name = bootImageName + '+' + contextMethod + '+' + str(time.time())[0:10] 

      flavors = self.driver.list_sizes()
      flavor = [s for s in flavors if s.name == flavorName][0]

      try:
          request.VMnode = self.driver.create_node(name= vm_name, image=bootImage, size=flavor)
      except Exception, errmsg:
          request.stderr = errmsg
          request.returncode = -1
	  return request

      if not ipPool=='NO':
        # getting a floating IP and asign to the node:
        try:
            address=self.pynovaclient.floating_ips.create(pool=ipPool)
            self.pynovaclient.servers.add_floating_ip(request.VMnode.id, address.ip)
            request.public_ip = address.ip
        except Exception, errmsg:
            request.stderr = errmsg
            request.returncode = -1
	    return request
      else:
        request.public_ip = VMnode.ip

      return request

    """ 
    Conextualize an active instance
    This is necesary because the libcloud deploy_node, including key/cert copy and ssh run, based on amiconfig, are sychronous operations which can not scale
    """
    def contextualize_VMInstance( public_ip, vmCertPath, vmKeyPath, vmRunJobAgent, vmRunVmMonitorAgent, vmRunLogJobAgent, vmRunLogVmMonitorAgent, cvmfsContextPath, diracContextPath, cvmfs_http_proxy ):
      request = Request()

      if contextMethod == 'ssh': 
        # the contextualization using ssh needs the VM to be ACTIVE, so VirtualMachineContextualization check status and launch contextualize_VMInstance

        # from specif template write the cmvfs http proxy on a temporal script to submit to the VMs with the specific cvmfs config
        f = open(cvmfsContextPath,'r')   
        chain = f.read()  
        chain = chain.replace("TEMPLATE_VALUE",cvmfs_http_proxy)  
        f.close()   
 
        tempSpecificContextName = '/tmp/nova-specific-context.%s.bash' % os.getpid()
        tempContext = open(tempContextName, 'w') 
        tempContext.write(chain)
        tempContext.close()  

        # 1) copy the necesary files

        # prepare paramiko sftp client
        try:
            privatekeyfile = os.path.expanduser('~/.ssh/id_rsa')
            mykey = paramiko.RSAKey.from_private_key_file(privatekeyfile)
            username = os.getusername()
            transport.connect(username = username, pkey = mykey)
            sftp = paramiko.SFTPClient.from_transport(transport)
        except Exception, errmsg:
            request.stderr = "Can't open sftp conection to %s: %s" % (public_ip,errmsg)
            request.returncode = -1
	    return request

        # scp DIRAC run conf files to be used with runsvdir
        try:
            sftp.put('/root/run.job-agent',vmRunJobAgent)
            sftp.put('/root/run.vm-monitor-agent',vmRunVmMonitorAgent)
            sftp.put('/root/run.log.job-agent',vmRunLogJobAgent)
            sftp.put('/root/run.log.vm-monitor-agent',vmRunLogVmMonitorAgent)
        except Exception, errmsg:
            request.stderr = errmsg
            request.returncode = -1
	    return request

        # scp VM cert/key
        try:
            sftp.put('/root/vmservicecert.pem',vmCertPath)
            sftp.put('/root/vmservicekey.pem',vmKeyPath)
        except Exception, errmsg:
            request.stderr = errmsg
            request.returncode = -1
	    return request
 
        sftp.close()
        transport.close()

        #2) Run the cvmvfs contextualization script:    

        # prepare paramiko ssh client
        try:
            sshclient = paramiko.SSHClient()
            sshclient.load_system_host_keys()
            sshclient.connect(public_ip)
        except Exception, errmsg:
            request.stderr = "Can't open ssh conection to %s: %s" % (public_ip,errmsg)
            request.returncode = -1
	    return request

        try:
            f = open(cvmfsContextPath,'r')
            chain = f.read()
        except Exception, errmsg:
            request.stderr = "Can't read from file %s: %s" % (cvmfsContextPath,errmsg)
            request.returncode = -1
	    return request

        f.close()

        try:
            stdin, stdout, stderr = sshclient.exec_command(chain)
        except Exception, errmsg:
            request.stderr = "Can't run remote ssh to %s: %s" % (public_ip,errmsg)
            request.returncode = -1
	    return request
      
        #3) Run the general DIRAC contextualization script:    

        try:
            f = open(diracContextPath,'r')
            chain = f.read()
        except Exception, errmsg:
            request.stderr = "Can't read from file %s: %s" % (diracContextPath,errmsg)
            request.returncode = -1
	    return request

        f.close()

        try:
            stdin, stdout, stderr = sshclient.exec_command(chain)
        except Exception, errmsg:
            request.stderr = "Can't run remote ssh to %s: %s" % (public_ip,errmsg)
            request.returncode = -1
	    return request
      

      return request

    """
    Get the status VM instance for a given VMinstanceId 
    """
    def getStatus_VMInstance( self, uniqueId ):
      request = Request()
      try:
          infonode = self.pynovaclient.servers.list(uniqueId)
      except Exception, errmsg:
          request.stderr = "Can't get status of VMinstance uniqueId %s; %s:" % (uniqueId,errmsg)
          request.returncode = -1
          return request

      for o in infonode:
          request.status = getattr(o, 'status', '')
          return request

      self.stderr = "Can't get status of VMinstance uniqueId %s; %s:" % (uniqueId,errmsg)
      self.returncode = -1
      return request

    """
    Terminate a VM instance with uniqueId
    """
    def terminate_VMinstance( self, uniqueId, ipPool = 'NO', public_ip = '' ):
      request = Request()

      if not ipPool=='NO':
        # getting a floating IP and asign to the node:
        try:
            self.pynovaclient.floating_ips.delete(public_ip)
        except Exception, errmsg:
            request.stderr = errmsg
            request.returncode = -1

      try:
          infonode = self.pynovaclient.servers.delete(uniqueId)
      except Exception, errmsg:
          request.stderr = errmsg
          request.returncode = -1
	  return request

      return request

