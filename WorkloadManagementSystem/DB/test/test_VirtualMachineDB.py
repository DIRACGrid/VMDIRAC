#!/usr/bin/env python
#
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = False )

from VMDIRAC.WorkloadManagementSystem.DB.VirtualMachineDB               import VirtualMachineDB

import DIRAC

db = VirtualMachineDB()
validStates = db.validInstanceStates

print "checkImageStatus        ", db.checkImageStatus( 'Image3' )
# print db.checkImageStatus( 'name', 'flavor'*10, 'requirements' )

ret = db.insertInstance( 'Image3', 'instance' )
print "insertInstance          ", ret
ret = db.insertInstance( 'Image2', 'instance' )
print "insertInstance          ", ret

if not ret['OK']:
  DIRAC.exit()

print type( ret['Value'] )
print "declareInstanceSubmitted", db.declareInstanceSubmitted( ret['Value'] )
id1 = DIRAC.Time.toString()
print "declareInstanceRunning  ", db.declareInstanceRunning( 'Image3', id1, 'IP', 'ip' )
id2 = DIRAC.Time.toString()
print "declareInstanceRunning  ", db.declareInstanceRunning( 'Image2', id2, 'IP', 'ip' )
print "declareInstanceRunning  ", db.instanceIDHeartBeat( id2, 1.0 )

for status in validStates:
  print "get%10sInstances  " % status, db.getInstancesByStatus( status )

print "declareInstanceHalting  ", db.declareInstanceHalting( id1, 0.0 )
print "declareInstanceHalting  ", db.declareInstanceHalting( id2, 0.0 )

print "declareStalledInstances ", db.declareStalledInstances()
print "declareStalledInstances ", db.declareStalledInstances()

from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB
tq = TaskQueueDB()
print tq.retrieveTaskQueues()
