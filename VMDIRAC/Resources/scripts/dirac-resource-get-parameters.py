#!/usr/bin/env python
"""
  Get parameters assigned to the CE
"""

__RCSID__ = "$Id$"

import json
from DIRAC.Core.Base import Script
from DIRAC import gLogger, exit as DIRACExit

Script.setUsageMessage('\n'.join(['Get the Tag of a CE',
                                  'Usage:',
                                  '%s [option]... [cfgfile]' % Script.scriptName,
                                  'Arguments:',
                                  ' cfgfile: DIRAC Cfg with description of the configuration (optional)']))

ceName = ''
ceType = ''
Site = None
Queue = None


def setCEName(args):
  global ceName
  ceName = args


def setSite(args):
  global Site
  Site = args


def setQueue(args):
  global Queue
  Queue = args


Script.registerSwitch("N:", "Name=", "Computing Element Name (Mandatory)", setCEName)
Script.registerSwitch("S:", "Site=", "Site Name (Mandatory)", setSite)
Script.registerSwitch("Q:", "Queue=", "Queue Name (Mandatory)", setQueue)


Script.parseCommandLine(ignoreErrors=True)
args = Script.getExtraCLICFGFiles()

from VMDIRAC.Resources.Cloud.ConfigHelper import getVMTypes

if len(args) > 1:
  Script.showHelp()
  exit(-1)


result = getVMTypes(Site, ceName, Queue)
if not result['OK']:
  gLogger.error("Could not retrieve resource parameters", ": " + result['Message'])
  DIRACExit(1)

queueDict = result['Value'][Site][ceName].pop('VMTypes')[Queue]
ceDict = result['Value'][Site][ceName]
ceDict.update(queueDict)
gLogger.notice(json.dumps(result['Value']))
