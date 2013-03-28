########################################################################
# $HeadURL$
# File :   Nova11.py
# Author : Victor Mendez ( vmendez.tic@gmail.com )
########################################################################
# DIRAC driver to nova v_1.1 endpoint using libcloud and pyton-novaclient
# TODO: frist release only with user/passd, to implement proxy auth with VOMS

import getpass
#import libcloud.security
import os
import paramiko 
import time 

from libcloud.compute.types     import Provider
from libcloud.compute.providers import get_driver
from novaclient.v1_1            import client

__RCSID__ = '$Id: $'

#FIXME: where to find it ?
#libcloud.security.CA_CERTS_PATH =[ '/etc/pki/tls/certs/CERN-bundle.pem' ]
# osServiceRegion = 'cern-geneva'

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
#  def __init__( self, osAuthURL=None , osUserName = None, osPasswd = None, 
#                osTenantName = None , osBaseURL = None, osServiceRegion = None,
#                osCaCert = None ):  
  def __init__( self, user, secret, **kwargs ):
    
    cloudManagerAPI = get_driver( Provider.OPENSTACK )   
    
    ex_force_auth_url       = kwargs.get( 'ex_force_auth_url', None )
    ex_force_service_region = kwargs.get( 'ex_force_service_region', None ) 
    ex_force_auth_version   = kwargs.get( 'ex_force_auth_version', '2.0_password' )
    ex_tenant_name          = kwargs.get( 'ex_tenant_name', None )
    
    # The driver has the access secret, we do not want it to be public at all.    
    self.__driver = cloudManagerAPI( user, secret = secret,
                                     ex_force_auth_url = ex_force_auth_url,
                                     ex_force_service_region = ex_force_service_region,
                                     ex_force_auth_version = ex_force_auth_version,
                                     ex_tenant_name = ex_tenant_name,
                                    )
     
    # mofify to insecure=False when ca cert ready
    # The client has the access secret, we do not want it to be public at all.    
    self.__pynovaclient = client.Client( username = user, api_key = secret, 
                                         project_id = ex_tenant_name, 
                                         auth_url = ex_force_auth_url, 
                                         insecure = True, 
                                         region_name = ex_force_service_region, 
                                         auth_system = 'keystone' )

  def check_connection(self):
    """
    This is a way to check the availability of a give URL as occi server
    returning a Request class
    """
    request = Request()
    try:
      _ = self.__driver.list_images()
      #FIXME: what do we do with them ?
    except Exception, errmsg:
      request.stderr = errmsg
      request.returncode = -1
    return request
  
  def get_image(self, imageName):
    """
    The get_image_id function return the corresponding openstack id
    a given imageName on the current occi client self.URI of the occi server.
    """

    request = Request()
    try:
      images = self.__driver.list_images()
      request.image = [i for i in images if i.name == imageName ][0]
    except Exception, errmsg:
      request.stderr = errmsg
      request.returncode = -1

    return request

  def create_VMInstance( self, bootImageName, contextMethod, flavorName, bootImage, ipPool,
                         userdata = None, keyname = None, metadata = None ):
    """
    This creates a VM instance for the given boot image 
    and creates a context script, taken the given parameters.
    Successful creation returns instance VM 
    """
    request = Request()
    vm_name = bootImageName + '+' + contextMethod + '+' + str(time.time())[0:10] 

    flavors = self.__driver.list_sizes()
    flavor = [s for s in flavors if s.name == flavorName][0]

    try:
      request.VMnode = self.__driver.create_node( name        = vm_name, 
                                                  image       = bootImage, 
                                                  size        = flavor,
                                                  ex_keyname  = keyname,
                                                  ex_userdata = userdata,
                                                  ex_metadata = metadata )
    except Exception, errmsg:
      request.stderr = errmsg
      request.returncode = -1
      return request

    if not ipPool=='NO':
      # getting a floating IP and asign to the node:
      try:
        address=self.__pynovaclient.floating_ips.create(pool=ipPool)
        self.__pynovaclient.servers.add_floating_ip(request.VMnode.id, address.ip)
        request.public_ip = address.ip
      except Exception, errmsg:
        request.stderr = errmsg
        request.returncode = -1
      return request
    else:
      request.public_ip = VMnode.ip

    return request

  def contextualize_VMInstance( self, uniqueId, public_ip, contextMethod, vmCertPath, 
                                vmKeyPath, vmContextualizeScriptPath, vmRunJobAgentURL, 
                                vmRunVmMonitorAgentURL, vmRunLogJobAgentURL, 
                                vmRunLogVmMonitorAgentURL, cvmfsContextURL, diracContextURL, 
                                cvmfs_http_proxy, siteName, cloudDriver ):
    """ 
    Conextualize an active instance
    This is necesary because the libcloud deploy_node, including key/cert copy and ssh run, 
    based on amiconfig, are sychronous operations which can not scale
    """

    request = Request()

    if contextMethod == 'ssh': 
      # the contextualization using ssh needs the VM to be ACTIVE, so VirtualMachineContextualization check status and launch contextualize_VMInstance

      # 1) copy the necesary files

      # prepare paramiko sftp client
      try:
        privatekeyfile = os.path.expanduser('~/.ssh/id_rsa')
        mykey = paramiko.RSAKey.from_private_key_file(privatekeyfile)
        username =  getpass.getuser()
        transport = paramiko.Transport((public_ip, 22))
        transport.connect(username = username, pkey = mykey)
        sftp = paramiko.SFTPClient.from_transport(transport)
      except Exception, errmsg:
        request.stderr = "Can't open sftp conection to %s: %s" % (public_ip,errmsg)
        request.returncode = -1
        return request

      # scp VM cert/key
      putCertPath = "/root/vmservicecert.pem"
      putKeyPath = "/root/vmservicekey.pem"
      try:
        sftp.put(vmCertPath, putCertPath)
        sftp.put(vmKeyPath, putKeyPath)
        # while the ssh.exec_command is asyncronous request I need to put on the VM the contextualize-script to ensure the file existence before exec
        sftp.put(vmContextualizeScriptPath, '/root/contextualize-script.bash')
      except Exception, errmsg:
        request.stderr = errmsg
        request.returncode = -1
        return request

      sftp.close()
      transport.close()

      #2)  prepare paramiko ssh client
      try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(public_ip, username=username, port=22, pkey=mykey)
      except Exception, errmsg:
        request.stderr = "Can't open ssh conection to %s: %s" % (public_ip,errmsg)
        request.returncode = -1
        return request

      #3) Run the DIRAC contextualization orchestator script:    

      try:
        remotecmd = "/bin/bash /root/contextualize-script.bash \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\' \'%s\'" %(uniqueId, putCertPath, putKeyPath, vmRunJobAgentURL, vmRunVmMonitorAgentURL, vmRunLogJobAgentURL, vmRunLogVmMonitorAgentURL, cvmfsContextURL, diracContextURL, cvmfs_http_proxy, siteName, cloudDriver) 
        print "remotecmd"
        print remotecmd
        stdin, stdout, stderr = ssh.exec_command(remotecmd)
      except Exception, errmsg:
        request.stderr = "Can't run remote ssh to %s: %s" % (public_ip,errmsg)
        request.returncode = -1
        return request    

      return request

  def getStatus_VMInstance( self, uniqueId ):
    """
    Get the status VM instance for a given VMinstanceId 
    """
    request = Request()
    try:
      infonode = self.__pynovaclient.servers.list(uniqueId)
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

  def terminate_VMinstance( self, uniqueId, ipPool = 'NO', public_ip = '' ):
    """
    Terminate a VM instance with uniqueId
    """

    request = Request()

    request.stderr = ""

    try:
      infonode = self.__pynovaclient.servers.delete(uniqueId)
    except Exception, errmsg:
      request.stderr = "Can't stop VMinstance uniqueId %s; %s:" % (uniqueId,errmsg)
      request.returncode = -1

    if not ipPool=='NONE':
      try:
        floating_ips = self.__pynovaclient.floating_ips.list()
        for floating_ip in floating_ips:
          if floating_ip.ip == public_ip:
            self.__pynovaclient.floating_ips.delete(floating_ip.id)
      except Exception, errmsg:
        request.stderr = "%s /n Can't delete floating ip %s of VMinstance uniqueId %s; %s:" % (request.stderr, public_ip, uniqueId,errmsg)
        request.returncode = -1

    return request

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF