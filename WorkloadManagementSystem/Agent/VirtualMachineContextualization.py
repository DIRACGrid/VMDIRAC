########################################################################
# $HeadURL$
# File :   VirtualMachineContextualization.py
# Author : Victor Mendez
########################################################################

"""
	The VirtualMachineContextualization agent is used for the asyncronous vm contextualization methods.
        The first available method is ssh contextualization
        Such method needs the VM to be active, before the contextualization using ssh
        For this pupose the Image layer load Status=Wait_ssh_context on VirtualMachineDB.vm_Instances table
        This agent is lookint into this table, asking to the corresponding cloud endpoint to check the status,
        then requesting the VM contextualization and
        changing the status on the corresponding entry of VirtualMachineDB.vm_SshContextualize
"""
__RCSID__ = "$Id$"

from random import shuffle
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC import gConfig


from VMDIRAC.WorkloadManagementSystem.Client.ServerUtils     import virtualMachineDB
from VMDIRAC.WorkloadManagementSystem.Client.NovaImage import NovaImage

import random, time
import DIRAC

random.seed()

class VirtualMachineContextualization( AgentModule ):

  def initialize( self ):
    """ Standard constructor
    """
    import threading

    self.am_setOption( "PollingTime", 60.0 )

    return DIRAC.S_OK()

  def execute( self ):
    """ Agent executor
    """
    result = virtualMachineDB.getInstancesInfoByStatus ( 'Wait_ssh_context' )
    if not result['OK']:
        return result

    for uniqueId, endpoint, publicIP in result['Value']:

        retDict = virtualMachineDB.getImageNameFromInstance ( uniqueId )
        if not retDict['OK']:
           return retDict

        diracImageName = retDict['Value']
        nima = NovaImage( diracImageName, endpoint )

        result = nima.getInstanceStatus( uniqueId )
        if not result[ 'OK' ]:
          return result

        if result['Value'] == 'ACTIVE':
          result = nima.contextualizeInstance( uniqueId, publicIP )
          print "result of contextualize:"
          print result
          if not result[ 'OK' ]:
            return result
          retDict = virtualMachineDB.declareInstanceContextualizing( uniqueId )
          if not retDict['OK']:
            return retDict

    return DIRAC.S_OK() 
