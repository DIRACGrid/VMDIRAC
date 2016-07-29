""" RocciEndpoint class is the implementation of the rocci interface to
    a cloud endpoint via rOCCI-cli
"""

import os
import json
import subprocess
import tempfile

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File import makeGuid

from VMDIRAC.Resources.Cloud.Endpoint import Endpoint
from VMDIRAC.Resources.Cloud.Utilities import createMimeData

__RCSID__ = '$Id$'

class RocciEndpoint( Endpoint ):

  def __init__( self, parameters = {} ):
    """
    """
    Endpoint.__init__( self, parameters = parameters )
    # logger
    self.log = gLogger.getSubLogger( 'RocciEndpoint' )
    self.valid = False
    result = self.initialize()
    if result['OK']:
      self.log.debug( 'RocciEndpoint created and validated' )
      self.valid = True
    else:
      self.log.error( result['Message'] )

  def initialize( self ):

    availableParams = {
      'EndpointUrl': 'endpoint',
      'Timeout':     'timeout',
      'Auth':        'auth',
      'User':        'username',
      'Password':    'password',
      'UserCred':    'user-cred',
      'VOMS':        'voms',
    }

    self.__occiBaseCmd = ['occi', '--skip-ca-check', '--output-format', 'json_extended']
    for var in availableParams:
      if var in self.parameters:
        self.__occiBaseCmd += ['--%s' % availableParams[var], '%s' % self.parameters[var]]

    result = self.__checkConnection()
    return result

  def __filterCommand( self, cmd ):
    filteredCmd = []
    mask = False
    for arg in cmd:
      if mask:
        filteredCmd.append( 'xxxxxx' )
        mask = False
      else:
        filteredCmd.append( arg )

      if arg in ['--username', '--password']:
        mask = True
    return ' '.join( filteredCmd )

  def __occiCommand( self, actionArgs ):
    try:
      finalCmd = self.__occiBaseCmd + actionArgs
      self.log.debug( 'Running command:', self.__filterCommand( finalCmd ) )
      p = subprocess.Popen( finalCmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
      stdout, stderr = p.communicate()
      if p.returncode != 0:
        return S_ERROR( 'occi command exit with error %s: %s' % (p.returncode, stderr) )
    except Exception as e:
      return S_ERROR( 'Can not run occi command' )

    return S_OK( stdout )

  def __checkConnection( self ):
    """
    Checks connection status by trying to list the images.

    :return: S_OK | S_ERROR
    """
    actionArgs = ['--action', 'list', '--resource', 'os_tpl']
    result = self.__occiCommand( actionArgs )
    if not result['OK']:
      return result

    return S_OK()

  def __getImageByName( self, imageName ):
    """
    Given the imageName, returns the current image object from the server.

    :Parameters:
      **imageName** - `string`

    :return: S_OK( image ) | S_ERROR
    """
    # the libcloud library, throws Exception. Nothing to do.
    actionArgs = ['--action', 'describe', '--resource', 'os_tpl']
    result = self.__occiCommand( actionArgs )
    if not result['OK']:
      return result

    imageIds = []
    for image in json.loads( result['Value'] ):
      if image['title'] == imageName:
        imageIds.append( image['term'] )

    if not imageIds:
      return S_ERROR( "Image %s not found" % imageName )

    if len( imageIds ) > 1:
      self.log.warn( 'More than one image found', '%s images with name "%s"' % ( len( imageIds ), imageName ) )

    return S_OK( imageIds[-1] )

  def __createUserDataScript( self ):

    userDataDict = {}

    # Arguments to the vm-bootstrap command
    bootstrapArgs = { 'dirac-site': self.parameters['Site'],
#                      'submit-pool': self.parameters['SubmitPool'],
                      'ce-name': self.parameters['CEName'],
                      'image-name': self.parameters['Image'],
                      'vm-uuid': self.parameters['VMUUID'],
                      'vmtype': self.parameters['VMType'],
                      'vo': self.parameters['VO'],
                      'running-pod': self.parameters['RunningPod'],
                      'cvmfs-proxy': self.parameters.get( 'CVMFSProxy', 'None' ),
                      'cs-servers': ','.join( self.parameters.get( 'CSServers', [] ) ),
                      'release-version': self.parameters['Version'] ,
                      'release-project': self.parameters['Project'] ,
                      'setup': self.parameters['Setup'] }

    bootstrapString = ''
    for key, value in bootstrapArgs.items():
      bootstrapString += " --%s=%s \\\n" % ( key, value )
    userDataDict['bootstrapArgs'] = bootstrapString

    userDataDict['user_data_commands_base_url'] = self.parameters.get( 'user_data_commands_base_url' )
    if not userDataDict['user_data_commands_base_url']:
      return S_ERROR( 'user_data_commands_base_url is not defined' )
    with open( self.parameters['HostCert'] ) as cfile:
      userDataDict['user_data_file_hostkey'] = cfile.read().strip()
    with open( self.parameters['HostKey'] ) as kfile:
      userDataDict['user_data_file_hostcert'] = kfile.read().strip()

    # List of commands to be downloaded
    bootstrapCommands = self.parameters.get( 'user_data_commands' )
    if isinstance( bootstrapCommands, basestring ):
      bootstrapCommands = bootstrapCommands.split( ',' )
    if not bootstrapCommands:
      return S_ERROR( 'user_data_commands list is not defined' )
    userDataDict['bootstrapCommands'] = ' '.join( bootstrapCommands )

    script = """
cat <<X5_EOF >/root/hostkey.pem
%(user_data_file_hostkey)s
%(user_data_file_hostcert)s
X5_EOF
mkdir -p /var/spool/checkout/context
cd /var/spool/checkout/context
for dfile in %(bootstrapCommands)s
do
  echo curl --insecure -s %(user_data_commands_base_url)s/$dfile -o $dfile
  i=7
  while [ $i -eq 7 ]
  do
    curl --insecure -s %(user_data_commands_base_url)s/$dfile -o $dfile
    i=$?
    if [ $i -eq 7 ]; then
      echo curl connection failure for file $dfile
      sleep 10
    fi
  done
  curl --insecure -s %(user_data_commands_base_url)s/$dfile -o $dfile || echo Download of $dfile failed with $? !
done
chmod +x vm-bootstrap
/var/spool/checkout/context/vm-bootstrap %(bootstrapArgs)s
#/sbin/shutdown -h now
    """ % userDataDict

    if "HEPIX" in self.parameters:
      script = """
cat <<EP_EOF >>/var/lib/hepix/context/epilog.sh
#!/bin/sh
%s
EP_EOF
chmod +x /var/lib/hepix/context/epilog.sh
      """ % script

    user_data = """#!/bin/bash
mkdir -p /etc/joboutputs
(
%s
) > /etc/joboutputs/user_data.log 2>&1 &
exit 0
    """ % script

    cloud_config = """#cloud-config

output: {all: '| tee -a /var/log/cloud-init-output.log'}

cloud_final_modules:
  - [scripts-user, always]
    """

    return createMimeData( ( ( user_data, 'text/x-shellscript', 'dirac_boot.sh' ),
                             ( cloud_config, 'text/cloud-config', 'cloud-config') ) )

  def createInstances( self, vmsToSubmit ):
    outputDict = {}

    for nvm in xrange( vmsToSubmit ):
      instanceID = makeGuid()[:8]
      result = self.createInstance( instanceID )
      if result['OK']:
        occiId, nodeDict = result['Value']
        self.log.debug( 'Created VM instance %s/%s' % ( occiId, instanceID ) )
        outputDict[occiId] = nodeDict
      else:
        break

    return S_OK( outputDict )

  def createInstance( self, instanceID = ''  ):
    if not instanceID:
      instanceID = makeGuid()[:8]

    self.parameters['VMUUID'] = instanceID
    self.parameters['VMType'] = self.parameters.get( 'CEType', 'EC2' )

    actionArgs = ['--action', 'create']
    actionArgs += ['--resource', 'compute']

    # Image
    if not "ImageID" in self.parameters and 'ImageName' in self.parameters:
      result = self.__getImageByName( self.parameters['ImageName'] )
      if not result['OK']:
        return result
      imageId = result['Value']
    elif "ImageID" in self.parameters:
      result = self.__occiCommand( ['--action', 'describe', '--resource', 'os_tpl#%s' % self.parameters['ImageID']] )
      if not result['OK']:
        return S_ERROR( "Failed to get image for ID %s" % self.parameters['ImageID'], result['Message'] )
      imageId = self.parameters['ImageID']
    else:
      return S_ERROR( 'No image specified' )
    actionArgs += ['--mixin', 'os_tpl#%s' % imageId]

    # Optional flavor name
    if 'FlavorName' in self.parameters:
      result = self.__occiCommand( ['--action', 'describe', '--resource', 'resource_tpl#%s' % self.parameters['FlavorName']] )
      if not result['OK']:
        return S_ERROR( "Failed to get flavor %s" % self.parameters['FlavorName'], result['Message'] )
      actionArgs += ['--mixin', 'resource_tpl#%s' % self.parameters['FlavorName']]

    # Instance name
    actionArgs += ['--attribute', 'occi.core.title=DIRAC_%s' % instanceID]

    # Other params
    for param in []:
      if param in self.parameters:
        actionArgs += ['--%s' % param, '%s' % self.parameters[param]]

    self.log.info( "Creating node:" )
    self.log.verbose( ' '.join( actionArgs ) )

    # User data
    result = self.__createUserDataScript()
    if not result['OK']:
      return result
#    actionArgs += ['--context', 'user_data=%s' % str( result['Value'] )]
    f = tempfile.NamedTemporaryFile( delete = False )
    f.write( str( result['Value'] ) )
    f.close()
    self.log.debug( 'Write user_data to temp file:', f.name )
    actionArgs += ['--context', 'user_data=file://%s' % f.name ]

    # Create the VM instance now
    result = self.__occiCommand( actionArgs )
    os.unlink( f.name )
    if not result['OK']:
      errmsg = 'Error in rOCCI create instances: %s' % result['Message']
      self.log.error( errmsg )
      return S_ERROR( errmsg )

    occiId = result['Value'].strip()

    # Properties of the instance
    nodeDict = {}
    nodeDict['InstanceID'] = instanceID
    result = self.__occiCommand( ['--action', 'describe', '--resource', occiId] )
    if result['OK']:
      nodeInfo = json.loads( result['Value'] )
      try:
        nodeDict['NumberOfCPUs'] = nodeInfo[0]['attributes']['occi']['compute']['cores']
        nodeDict['RAM']          = nodeInfo[0]['attributes']['occi']['compute']['memory']
      except Exception as e:
        nodeDict['NumberOfCPUs'] = 1
    else:
      nodeDict['NumberOfCPUs'] = 1

    return S_OK( ( occiId, nodeDict ) )

  def stopVM( self, nodeID, publicIP = '' ):
    actionArgs = ['--action', 'delete', '--resource', nodeID]
    result = self.__occiCommand( actionArgs )
    if not result['OK']:
      errmsg = 'Exception terminate instance %s: %s' % ( nodeID, e )
      self.log.error( errmsg )
      return S_ERROR( errmsg )

    return S_OK()
