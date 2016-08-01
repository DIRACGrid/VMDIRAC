###########################################################
# $HeadURL$
###########################################################

"""
   CloudEndpoint is a base class for the clients used to connect to different
   cloud providers
"""

from VMDIRAC.Resources.Cloud.Utilities import createMimeData

__RCSID__ = '$Id$'

class Endpoint( object ):
  """ Endpoint base class
  """
  def __init__( self, parameters = {} ):
    """
    """
    # logger
    self.parameters = parameters
    self.valid = False

  def isValid( self ):
    return self.valid

  def setParameters( self, parameters ):
    self.parameters = parameters

  def getParameterDict( self ):
    return self.parameters

  def initialize( self ):
    pass

  def _createUserDataScript( self ):

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
