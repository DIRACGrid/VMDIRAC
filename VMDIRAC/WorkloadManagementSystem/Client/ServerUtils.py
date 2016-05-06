########################################################################
# File :   ServerUtils.py
# Author : Ricardo Graciani
########################################################################
"""
  Provide uniform interface to backend for local and remote clients (ie Director Agents)
"""

__RCSID__ = "$Id"

from DIRAC.WorkloadManagementSystem.Client.ServerUtils import getDBOrClient

def getVirtualMachineDB():
  serverName = 'WorkloadManagement/VirtualMachineManager'
  VirtualMachineDB = None
  try:
    from VMDIRAC.WorkloadManagementSystem.DB.VirtualMachineDB import VirtualMachineDB
  except:
    pass
  return getDBOrClient( VirtualMachineDB, serverName )

virtualMachineDB = getVirtualMachineDB()
