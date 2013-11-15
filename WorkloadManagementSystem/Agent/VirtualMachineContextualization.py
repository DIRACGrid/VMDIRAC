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

import random


# DIRAC
from DIRAC                       import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.Client.ServerUtils import virtualMachineDB
from VMDIRAC.WorkloadManagementSystem.Client.NovaImage       import NovaImage
from VMDIRAC.WorkloadManagementSystem.Client.OcciImage       import OcciImage

__RCSID__ = "$Id: $"

#FIXME: why do we need the random seed ?
random.seed()

class VirtualMachineContextualization( AgentModule ):

  def initialize( self ):
    """ Standard constructor
    """

    self.am_setOption( "PollingTime", 60.0 )

    return S_OK()

  def execute( self ):
    """ Agent executor
    """
    result = virtualMachineDB.getInstancesInfoByStatus ( 'Wait_ssh_context' )
    if not result['OK']:
      return result

    for uniqueId, endpoint, publicIP, runningPodName in result['Value']:

      retDict = virtualMachineDB.getImageNameFromInstance ( uniqueId )
      if not retDict['OK']:
        return retDict

      diracImageName = retDict['Value']

      cloudDriver = gConfig.getValue( "/Resources/VirtualMachines/CloudEndpoints/%s/%s" % ( endpoint, "cloudDriver" ) )
      if ( cloudDriver == 'nova-1.1' ):
        nima     = NovaImage( diracImageName, endpoint )
        connection = nima.connectNova()
      elif ( cloudDriver == 'rocci-1.1' ):
        oima     = OcciImage( diracImageName, endpoint )
        connection = oima.connectOcci()
      else:
        return S_ERROR( 'cloudDriver %s, has not ssh contextualization' % cloudDriver )

      if not connection[ 'OK' ]:
        return connection

      if ( cloudDriver == 'nova-1.1' ):
        result = nima.getInstanceStatus( uniqueId )
      elif ( cloudDriver == 'rocci-1.1' ):
        result = oima.getInstanceStatus( uniqueId )
      else:
        return S_ERROR( 'cloudDriver %s, has not ssh contextualization' % cloudDriver )
      if not result[ 'OK' ]:
        return result

      runningPodDict = virtualMachineDB.getRunningPodDict( runningPodName )
      if not runningPodDict['OK']:
        self.log.error('Error in RunningPodDict: %s' % runningPodDict['Message'])
        return runningPodDict
      runningPodDict = runningPodDict[ 'Value' ]

      runningRequirementsDict = runningPodDict['Requirements']
      cpuTime = runningRequirementsDict['CPUTime']
      if not cpuTime:
        return S_ERROR( 'Unknown CPUTime in Requirements of the RunningPod %s' % runningPodName )

      submitPool = runningRequirementsDict['SubmitPool']
      if not submitPool:
        return S_ERROR( 'Unknown submitPool in Requirements of the RunningPod %s' % runningPodName )
      self.log.info('VirtualMachineContextualize.py -> RunningPodDict SubmitPool: %s' % submitPool)

      if ( cloudDriver == 'nova-1.1' ):
        if result['Value'] == 'RUNNING':
          result = nima.contextualizeInstance( uniqueId, publicIP, cpuTime, submitPool )
          self.log.info( "result of contextualize:" )
          self.log.info( result )
          if not result[ 'OK' ]:
            return result
          retDict = virtualMachineDB.declareInstanceContextualizing( uniqueId )
          if not retDict['OK']:
            return retDict
      elif ( cloudDriver == 'rocci-1.1' ):
        if result['Value'] == 'active':
          result = oima.contextualizeInstance( uniqueId, publicIP, cpuTime, submitPool )
          self.log.info( "result of contextualize:" )
          self.log.info( result )
          if not result[ 'OK' ]:
            return result
          retDict = virtualMachineDB.declareInstanceContextualizing( uniqueId )
          if not retDict['OK']:
            return retDict

    return S_OK() 


#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
