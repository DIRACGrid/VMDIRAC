########################################################################
# $HeadURL$
# File :   OutputDataAgent.py
# Author : Ricardo Graciani
########################################################################
"""
  Agent in charge of retrieving outputs from Cloud Output SE and upload it to final 
  destination including registration in LFC and removal of entry in Cloud FC.

  Handled Paths are looked for under /Operations/[vo]/OutputData/Name
  Each Name is a section that should define:
    InputPath
    InputFC
    OutputPath
    OutputFC
    OutputSE

"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule                                    import AgentModule
from DIRAC.Core.Utilities.ThreadPool                                import ThreadPool
from DIRAC.Core.Utilities.Shifter                                   import setupShifterProxyInEnv
from DIRACVM.WorkloadManagementSystem.private.OutputDataExecutor import OutputDataExecutor
from DIRAC import S_OK, S_ERROR, gConfig, gLogger


class OutputDataAgent( AgentModule ):

  def initialize( self ):
    """
    Start a ThreadPool object to process transfers
    """
    #Define the shifter proxy needed
    self.am_setModuleParam( "shifterProxy", "DataManager" )

    self.__outDataExecutor = OutputDataExecutor()
    return S_OK()

  def execute( self ):
    """
    Loop over InputPath and OutputPath pairs
    """
    self.__outDataExecutor.checkForTransfers()

    maxCycles = self.am_getMaxCycles()
    if maxCycles > 0 and maxCycles - self.am_getCyclesDone() == 1:
      gLogger.info( "Waiting to all transfers to finish before ending the last cycle" )
      #We are in the last cycle. Need to purge the thread pool
      self.__outDataExecutor.processAllPendingTransfers()

    gLogger.info( "Transferred %d files" % self.__outDataExecutor.getNumOKTransferredFiles() )
    gLogger.info( "Transferred %d bytes" % self.__outDataExecutor.getNumOKTransferredBytes() )


    return S_OK()
