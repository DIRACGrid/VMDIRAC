########################################################################
# $HeadURL: https://dirac-grid.googlecode.com/svn/BelleDIRAC/trunk/BelleDIRAC/WorkloadManagementSystem/Client/ServerUtils.py $
# File :   ServerUtils.py
# Author : Ricardo Graciani
########################################################################
"""
  Provide uniform interface to backend for local and remote clients (ie Director Agents)
"""

__RCSID__ = "$Id: ServerUtils.py 16 2010-03-15 11:39:29Z ricardo.graciani@gmail.com $"

from DIRAC.WorkloadManagementSystem.Client.ServerUtils import getDBOrClient

def getVirtualMachineDB():
  serverName = 'WorkloadManagement/VirtualMachineMonitor'
  VirtualMachineDB = None
  try:
    from BelleDIRAC.WorkloadManagementSystem.DB.VirtualMachineDB               import VirtualMachineDB
  except:
    pass
  return getDBOrClient( VirtualMachineDB, serverName )

virtualMachineDB  = getVirtualMachineDB()
