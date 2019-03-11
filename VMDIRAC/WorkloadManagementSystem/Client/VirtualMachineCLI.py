#!/usr/bin/env python
""" Virtual Machine Command Line Interface. """

import pprint

from DIRAC.Core.Base.CLI import CLI
from VMDIRAC.Resources.Cloud.EndpointFactory import EndpointFactory
from VMDIRAC.Resources.Cloud.ConfigHelper import getPilotBootstrapParameters
from DIRAC.Core.Utilities.File import makeGuid

__RCSID__ = "$Id$"

class VirtualMachineCLI(CLI):
  """ Virtual Machine management console
  """

  def __init__(self, vo=None):
    CLI.__init__( self )
    self.site = None
    self.endpoint = None
    self.project = None
    self.vmType = None
    self.vo = vo

  def do_connect(self,args):
    """ Connect to the specified cloud endpoint
    """
    argss = args.split()
    if (len(argss)<2):
      print self.do_connect.__doc__
      return
    self.site = argss[0]
    del argss[0]
    self.endpoint = argss[0]
    del argss[0]
    self.project = argss[0]
    self.prompt = '%s/%s/%s> ' % (self.site, self.endpoint, self.project)

  def __getCE(self):
    result = EndpointFactory().getCE(self.site, self.endpoint, self.vmType)
    if not result['OK']:
      print result['Message']
      return
    ce = result['Value']

    # Add extra parameters if any
    extraParams = {}
    if self.project:
      extraParams['Project'] = self.project

    if extraParams:
      ce.setParameters(extraParams)
      ce.initialize()

    return ce

  def do_list(self, args):
    """ Get VM list
    """

    ce = self.__getCE()

    result = ce.getVMIDs()
    if not result['OK']:
      print "ERROR: %s" % result['Message']
    else:
      print '\n'.join(result['Value'])

  def do_info(self, args):
    """ Get VM status
    """

    argss = args.split()
    if (len(argss) == 0):
      print self.do_status.__doc__
      return
    vmID = argss[0]
    del argss[0]
    longOutput = False
    if argss and args[0] == "-l":
      longOutput = True

    ce = self.__getCE()
    result = ce.getVMInfo(vmID)

    if not result['OK']:
      print "ERROR: %s" % result['Message']
    else:
      pprint.pprint(result['Value'])

  def do_status(self, args):
    """ Get VM status
    """

    argss = args.split()
    if (len(argss) == 0):
      print self.do_status.__doc__
      return
    vmID = argss[0]
    del argss[0]
    longOutput = False
    if argss and args[0] == "-l":
      longOutput = True

    ce = self.__getCE()
    result = ce.getVMStatus(vmID)

    if not result['OK']:
      print "ERROR: %s" % result['Message']
    else:
      print result['Value']['status']


  def do_ip(self, args):
    """ Assign IP
    """

    argss = args.split()
    if (len(argss) == 0):
      print self.do_assign-ip.__doc__
      return
    vmID = argss[0]

    ce = self.__getCE()
    result = ce.assignFloatingIP(vmID)

    if not result['OK']:
      print "ERROR: %s" % result['Message']
    else:
      print result['Value']

  def do_create(self,args):
    """ Create VM
    """

    argss = args.split()
    if (len(argss) == 0):
      print self.do_create.__doc__
      return
    self.vmType = argss[0]

    result = getPilotBootstrapParameters(vo=self.vo)
    bootParameters = result['Value']

    ce = self.__getCE()
    ce.setBootstrapParameters(bootParameters)

    diracVMID = makeGuid()[:8]
    result = ce.createInstance(diracVMID)

    if not result['OK']:
      print "ERROR: %s" % result['Message']
    else:
      print result['Value']

  def do_stop(self,args):
    """ Stop VM
    """

    argss = args.split()
    if (len(argss) == 0):
      print self.do_stop.__doc__
      return
    vmID = argss[0]

    ce = self.__getCE()
    result = ce.stopVM(vmID)

    if not result['OK']:
      print "ERROR: %s" % result['Message']
    else:
      print "VM stopped"