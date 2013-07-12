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

# DIRAC
from DIRAC                       import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule

# VMDIRAC
from VMDIRAC.WorkloadManagementSystem.private.OutputDataExecutor import OutputDataExecutor


__RCSID__ = "$Id$"

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
      self.log.info( "Waiting to all transfers to finish before ending the last cycle" )
      #We are in the last cycle. Need to purge the thread pool
      self.__outDataExecutor.processAllPendingTransfers()

    self.log.info( "Transferred %d files" % self.__outDataExecutor.getNumOKTransferredFiles() )
    self.log.info( "Transferred %d bytes" % self.__outDataExecutor.getNumOKTransferredBytes() )


    return S_OK()
