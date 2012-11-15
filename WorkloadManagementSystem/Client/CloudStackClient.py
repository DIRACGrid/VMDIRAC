########################################################################
# $HeadURL$
# File :   CloudStackClient.py
# Author : Victor Fernandez ( victormanuel.fernandez@usc.es )
########################################################################

import os
import subprocess
import time
import hashlib , hmac , base64 , urllib2, sys, string
import DIRAC

from DIRAC import S_OK, S_ERROR, gLogger, gConfig, DictCache, gLogger
from xml.dom import minidom, Node

SOURCE_ENCODING = "iso-8859-1"

# Classes
###################

class Request:

    """ 
    This class is to perform syncronous and asyncronous request 
    """
    def __init__( self , apikey , secretKey ):
      self.stdout = None
      self.stderr = None
      self.returncode = None
      self.pid = None
      self.rlist = []
      self.apiKey = apikey
      self.secretKey = secretKey

    def generateApiSignature( self, options ):
        request = 'apikey=' + self.apiKey + options
        secret = self.secretKey.encode( SOURCE_ENCODING )
        request = request.encode( SOURCE_ENCODING )
        digest = hmac.new( unicode( secret, 'utf-8' ), unicode( request.lower(),
            'utf-8' ), hashlib.sha1 ).digest()
        gLogger.info( "Cloud signature:", urllib2.quote( base64.encodestring( digest )[:-1], "" ) )
        return urllib2.quote( base64.encodestring( digest )[:-1], "" )

    def callHttpCloudServer( self, cloudserver, signature, options ):
        url = 'http://' + cloudserver + ':8080/client/api?apikey=' + self.apiKey + options + '&signature=' + signature
        gLogger.info( "Request:", url )
        req = urllib2.Request( url )
        xmldoc = ""
        try:
          callToServer = urllib2.urlopen( req )
          xmldoc = minidom.parse( urllib2.urlopen( url ) )
        except Exception:
            gLogger.error( "Error in call of CloudStack Api" )
        return xmldoc

    def callHttpCloudServerAllRequest( self, cloudserver, URLpart ):
        url = 'http://' + cloudserver + ':8080/client/' + URLpart
        gLogger.info( "Request:", url )
        req = urllib2.Request( url )
        try:
            callToServer = urllib2.urlopen( req )
            xmldoc = minidom.parse( urllib2.urlopen( url ) )
        except Exception:
            gLogger.error( "Error in call of CloudStack Api" )
        return xmldoc

    def getText( self, nodelist ):
        rc = []
        for node in nodelist:
           if node.nodeType == node.TEXT_NODE:
              rc.append( node.data )
        return ''.join( rc )

    def walk( self, parent, level ):
        for node in parent.childNodes:
            if node.nodeType == Node.ELEMENT_NODE:
                self.printLevel( level )
                gLogger.info( 'Element: %s\n' % node.nodeName )
                attrs = node.attributes
                for attrName in attrs.keys():
                    attrNode = attrs.get( attrName )
                    attrValue = attrNode.nodeValue
                    self.printLevel( level + 2 )
                    gLogger.info( 'Attribute -- Name: %s  Value: %s\n' % \
                            ( attrName, attrValue ) )
                content = []
                for child in node.childNodes:
                    if child.nodeType == Node.TEXT_NODE:
                        content.append( child.nodeValue )
                if content:
                    strContent = string.join( content )
                    self.printLevel( level )
                    gLogger.info( 'Content: "', strContent )
                self.walk( node, level + 1 )

    def printLevel( self, level ):
        for idx in range( level ):
          gLogger.info( '    ' )

    def printXMLResponse( self, xmldoc ):
        rootNode = xmldoc.documentElement
        level = 0
        self.walk( rootNode, level )

    def createURL( self, options ):
        options_array = ""
        for key in sorted( options.iterkeys() ):
            options_array += "&%s=%s" % ( key, options[key] )
        return options_array

class CloudStackClient:
    def __init__( self, URI = None, SecretKey = None, ApiKey = None ):
      self.id = None
      self.URI = URI
      self.secretKey = SecretKey
      self.ApiKey = ApiKey

    """
    This is a way to check the availability of a give IP CloudStack server
    returning a Request class
    """
    def check_connection( self , cloudserver ):

      request = Request( self.ApiKey, self.secretKey )
      dic_opt = {'command':'listHosts'}
      urlpart = request.createURL( dic_opt )
      call = request.callHttpCloudServer( cloudserver, request.generateApiSignature( urlpart ), urlpart )
      #request.printXMLResponse( call )
      if ( call == "" ):
        request.stdout = call
        request.returncode = 1;
        return request
      request.returncode = 0;
      return request

    """
    Return the bootImageId of a given instanceId
    """
    def get_image_ids_of_instance( self, instanceId ):
      request = Request( self.ApiKey, self.secretKey )
      dic_opt = {'command':'listVirtualMachines'}
      create_request = request.createURL( dic_opt )
      sign = request.generateApiSignature( create_request )
      callHTTP = request.callHttpCloudServer( sign, create_request )
      vms = callHTTP.getElementsByTagName( "virtualmachine" )

      count = 0
      if ( len( vms ) > 0 ):
          for vm in vms:
              vm_id = vm.getElementsByTagName( "id" )[0]
              vm_id = request.getText( vm_id.childNodes )
              vm_service_id = vm.getElementsByTagName( "serviceofferingid" )[0]
              if ( vm_service_id == instanceId ):
                request.stdout = vm_id
                count = count + 1
      if ( count > 0 ):
          request.returncode = 0
      else:
          request.returncode = 1
      return request

    """
    This creates a VM instance for some specific parameter for CloudStack, some of the parameters are:
      command=The command to execute
      zoneId=The zone where the VM will run
      hypervisor=The type of hypervisor
      templateId=The template to use
      serviceOfferingId=Type of service to offering
      diskOfferingId=Type of disk to offering
      
      templatefilter:
          possible values are "featured", "self", "self-executable", "executable", and "community".
          * featured-templates that are featured and are public* self-templates that have been 
          registered/created by the owner* selfexecutable-templates that have been registered/created 
          by the owner that can be used to deploy a new VM* executable-all templates that can be used to 
          deploy a new VM* community-templates that are public.
    """
    def create_VMInstance( self,
                           cloudserver,
                           zoneId,
                           hypervisor,
                           instanceType,
                           templateId,
                           serviceOfferingId,
                           diskOfferingId,
                           templatefilter = "featured" ):
      request = Request( self.ApiKey, self.secretKey )
      dic_opt = {'command':'listHosts'}
      self.urlpart = request.createURL( dic_opt )
      self.call = request.callHttpCloudServer( cloudserver,
                                               request.generateApiSignature( self.urlpart ),
                                               self.urlpart )

      if self.call != "":
        dic = self.getNodesUp( 'host', self.call, 0 )
        if len( dic ) > 0:

          dic_opt = {'command':'listServiceOfferings'}
          self.urlpart = request.createURL( dic_opt )
          self.callHTTP = request.callHttpCloudServer( cloudserver,
                                                       request.generateApiSignature( self.urlpart ),
                                                       self.urlpart )

          dic_opt = {'command':'listTemplates', 'templatefilter':templatefilter}
          self.urlpart = request.createURL( dic_opt )
          self.callHTTP2 = request.callHttpCloudServer( cloudserver,
                                                        request.generateApiSignature( self.urlpart ),
                                                        self.urlpart )

          service = self.existServiceOffering( 'serviceoffering', self.callHTTP, 0, serviceOfferingId )
          template = self.existTemplate( 'template', self.callHTTP2, 0, templateId )

          if( ( service == 1 )and( template == 1 ) ):
              dic_opt = {'command':'deployVirtualMachine', \
                      'serviceOfferingId':serviceOfferingId, \
                      'templateId':templateId, \
                      'diskOfferingId':diskOfferingId, \
                      'hypervisor':hypervisor, \
                      'zoneId':zoneId}
              self.urlpart = request.createURL( dic_opt )
              #Call to deployment
              self.callHTTP2 = request.callHttpCloudServer( cloudserver,
                                                            request.generateApiSignature( self.urlpart ),
                                                            self.urlpart )
              id = self.getInstanceId( "deployvirtualmachineresponse", self.callHTTP2 )
              request.stdout = id

              if ( id != 0 ):
                request.returncode = 0
              else:
                request.returncode = 1
                gLogger.error( 'Error: problem to create VM' )
          else:
              if ( service == 0 ):
                request.returncode = 1
                gLogger.error( 'ServiceId: %s not exist in CloudStack server' % serviceOfferingId )
              if ( template == 0 ):
                request.returncode = 1
                gLogger.error( 'TemplateId: %s not exist in CloudStack server' % templateId )

      return request


    def getInstanceId( self, attrname, parent ):
      request = Request( self.ApiKey, self.secretKey )
      dic = {}
      hosts = parent.getElementsByTagName( attrname )
      for host in hosts:
          host_id = host.getElementsByTagName( "id" )[0]
          host_id = request.getText( host_id.childNodes )
          return host_id
      return 0

    def existTemplate( self, attrname, parent, level, templateid ):
      request = Request( self.ApiKey, self.secretKey )
      dic = {}
      hosts = parent.getElementsByTagName( attrname )
      for host in hosts:
          host_state = host.getElementsByTagName( "id" )[0]
          host_state = request.getText( host_state.childNodes )
          if ( host_state == templateid ):
              return 1
      return 0

    def existServiceOffering( self, attrname, parent, level, serviceofferingid ):
      request = Request( self.ApiKey, self.secretKey )
      dic = {}
      hosts = parent.getElementsByTagName( attrname )
      for host in hosts:
          host_state = host.getElementsByTagName( "id" )[0]
          host_state = request.getText( host_state.childNodes )
          if ( host_state == serviceofferingid ):
              return 1
      return 0

    def getNodesUp( self, attrname, parent, level ):
      request = Request( self.ApiKey, self.secretKey )
      dic = {}
      hosts = parent.getElementsByTagName( attrname )
      for host in hosts:
          host_state = host.getElementsByTagName( "state" )[0]
          host_state = request.getText( host_state.childNodes )
          if ( host_state == "Up" ):
              host_id = host.getElementsByTagName( "id" )[0]
              host_id = request.getText( host_id.childNodes )
              host_routing = host.getElementsByTagName( "type" )[0]
              host_routing = request.getText( host_routing.childNodes )
              if ( host_routing == "Routing" ):
                host_cpu = host.getElementsByTagName( "cpunumber" )[0]
                host_cpu = request.getText( host_cpu.childNodes )
                dic[host_id] = host_cpu
      return dic

    """
    Terminate a VM instance corresponding to the instanceId parameter
    """
    def terminate_VMinstance( self, instanceId ):
      request = Request( self.ApiKey, self.secretKey )
      dic_opt = {'command':'destroyVirtualMachine', \
                        'id':instanceId }
      urlpart = request.createURL( dic_opt )
      callHTTP = request.callHttpCloudServer( request.generateApiSignature( urlpart ), urlpart )
      if callHTTP == "":
        request.returncode = 0
      else:
        request.returncode = 1
      return request

    """
    Get all the VM instances 
    """
    def get_all_VMinstances( self, serviceofferingid ):
      request = Request( self.ApiKey, self.secretKey )

      dic_opt = {'command':'listVirtualMachines'}
      create_request = request.createURL( dic_opt )
      sign = request.generateApiSignature( create_request )
      callHTTP = request.callHttpCloudServer( sign, create_request )
      vms = callHTTP.getElementsByTagName( "virtualmachine" )

      count = 0
      if ( len( vms ) > 0 ):
          for vm in vms:
              vm_id = vm.getElementsByTagName( "id" )[0]
              vm_id = request.getText( vm_id.childNodes )
              vm_service_id = vm.getElementsByTagName( "serviceofferingid" )[0]
              if ( vm_service_id == serviceofferingid ):
                request.rlist.append( vm_id )
                count = count + 1
      if ( count > 0 ):
          request.returncode = 0
      else:
          request.returncode = 1
      return request

    """
    Get the running VM instances for a given boot image
    """
    def get_running_VMinstances( self, bootImageName ):
      request = Request( self.ApiKey, self.secretKey )

      dic_opt = {'command':'listVirtualMachines'}
      create_request = request.createURL( dic_opt )
      sign = request.generateApiSignature( create_request )
      callHTTP = request.callHttpCloudServer( sign, create_request )
      vms = callHTTP.getElementsByTagName( "virtualmachine" )

      count = 0
      if ( len( vms ) > 0 ):
          for vm in vms:
              vm_id = vm.getElementsByTagName( "id" )[0]
              vm_id = request.getText( vm_id.childNodes )
              vm_template_name = vm.getElementsByTagName( "templatename" )[0]
              vm_state = vm.getElementsByTagName( "state" )[0]
              if ( vm_template_name == bootImageName ):
                if ( vm_state == "Running" ):
                  request.rlist.append( vm_id )
                  count = count + 1
      if ( count > 0 ):
          request.returncode = 0
      else:
          request.returncode = 1
      return request

    """
    Get the status VM instance for a given VMinstanceId 
    """
    def get_status_VMinstance( self, VMinstanceId ):
      request = Request( self.ApiKey, self.secretKey )

      dic_opt = {'command':'listVirtualMachines', 'id':VMinstanceId}
      create_request = request.createURL( dic_opt )
      sign = request.generateApiSignature( create_request )
      callHTTP = request.callHttpCloudServer( sign, create_request )
      vms = callHTTP.getElementsByTagName( "virtualmachine" )
      count = 0
      if ( len( vms ) > 0 ):
        for vm in vms:
          vm_state = vm.getElementsByTagName( "state" )[0]
          vm_state = request.getText( vm_state.childNodes )
          request.stdout = request.stdout[vm_state]
          request.returncode = 0
          return request
      request.returncode = 1
      return request
