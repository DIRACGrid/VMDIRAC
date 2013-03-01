########################################################################
# $HeadURL$
# File :   ServerUtils.py
# Author : Ricardo Graciani
########################################################################
"""
  Provide uniform interface to backend for local and remote clients (ie Director Agents)
"""

#DIRAC
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import getDBOrClient

__RCSID__ = "$Id: ServerUtils.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

def getVirtualMachineDB():
  serverName = 'WorkloadManagement/VirtualMachineManager'
  VirtualMachineDB = None
  try:
    from VMDIRAC.WorkloadManagementSystem.DB.VirtualMachineDB import VirtualMachineDB
  except:
    pass
  return getDBOrClient( VirtualMachineDB, serverName )

virtualMachineDB = getVirtualMachineDB()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF