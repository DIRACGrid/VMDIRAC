from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from DIRAC.Core.Base import Script
Script.setUsageMessage("""
Launch the File Catalog shell

Usage:
   %s [option]
""" % Script.scriptName)

fcType = 'FileCatalog'
Script.registerSwitch("f:", "file-catalog=", "   Catalog client type to use (default %s)" % fcType)
Script.parseCommandLine(ignoreErrors=False)

from VMDIRAC.WorkloadManagementSystem.Client.VirtualMachineCLI import VirtualMachineCLI

cli = VirtualMachineCLI(vo='enmr.eu')
cli.cmdloop()
