import sys
import os

from DIRAC import S_OK, S_ERROR
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

STATE_MAP = { 0: 'RUNNING',
              1: 'REBOOTING',
              2: 'TERMINATED',
              3: 'PENDING',
              4: 'UNKNOWN',
              5: 'STOPPED',
              6: 'SUSPENDED',
              7: 'ERROR',
              8: 'PAUSED' }

def createMimeData( userDataTuple ):

  userData = MIMEMultipart()
  for contents, mtype, fname in userDataTuple:
    try:
      mimeText = MIMEText(contents, mtype, sys.getdefaultencoding())
      mimeText.add_header('Content-Disposition', 'attachment; filename="%s"' % fname )
      userData.attach( mimeText )
    except Exception as e:
      return S_ERROR( str( e ) )

  return S_OK( userData )

def createPilotDataScript(vmParameters, bootstrapParameters):

  userDataDict = {}

  # Arguments to the vm-bootstrap command
  parameters = dict(vmParameters)
  parameters.update(bootstrapParameters)
  bootstrapArgs = { 'dirac-site': parameters.get('Site'),
                    'submit-pool': parameters.get('SubmitPool', ''),
                    'ce-name': parameters.get('CEName'),
                    'image-name': parameters.get('Image'),
                    'vm-uuid': parameters.get('VMUUID'),
                    'vmtype': parameters.get('VMType'),
                    'vo': parameters.get('VO',''),
                    'running-pod': parameters.get('RunningPod',parameters.get('VO','')),
                    'cvmfs-proxy': parameters.get( 'CVMFSProxy', 'DIRECT' ),
                    'cs-servers': ','.join( parameters.get( 'CSServers', [])),
                    'number-of-processors': parameters.get( 'NumberOfProcessors', 1 ),
                    'whole-node': parameters.get( 'WholeNode', True ),
                    'required-tag': parameters.get( 'RequiredTag', '' ),
                    'release-version': parameters.get('Version'),
                    'release-project': parameters.get('Project'),
                    'setup': parameters.get('Setup')}

  bootstrapString = ''
  for key, value in bootstrapArgs.items():
    bootstrapString += " --%s=%s \\\n" % ( key, value )
  userDataDict['bootstrapArgs'] = bootstrapString

  userDataDict['user_data_commands_base_url'] = bootstrapParameters.get( 'user_data_commands_base_url' )
  if not userDataDict['user_data_commands_base_url']:
    return S_ERROR( 'user_data_commands_base_url is not defined' )
  with open( bootstrapParameters['CloudPilotCert'] ) as cfile:
    userDataDict['user_data_file_hostkey'] = cfile.read().strip()
  with open( bootstrapParameters['CloudPilotKey'] ) as kfile:
    userDataDict['user_data_file_hostcert'] = kfile.read().strip()
  sshKey = None
  userDataDict['add_root_ssh_key'] = ""
  if 'SshKey' in parameters:
    with open(parameters['SshKey'] ) as sfile:
      sshKey = sfile.read().strip()
      userDataDict['add_root_ssh_key'] = """
# Allow root login
sed -i 's/PermitRootLogin no/PermitRootLogin yes/g' /etc/ssh/sshd_config
# Copy id_rsa.pub to authorized_keys
echo \" """ + sshKey + """\" > /root/.ssh/authorized_keys
service sshd restart
echo CKM-best | passwd --stdin root
"""

  # List of commands to be downloaded
  bootstrapCommands = bootstrapParameters.get( 'user_data_commands' )
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
%(add_root_ssh_key)s
chmod +x vm-bootstrap
/var/spool/checkout/context/vm-bootstrap %(bootstrapArgs)s
#/sbin/shutdown -h now
    """ % userDataDict

  if "HEPIX" in vmParameters:
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
  # Also try to add ssh key using standart cloudinit approach(may not work)
  if sshKey:
    cloud_config += """
users:
  - name: diracroot
    sudo: ALL=(ALL) NOPASSWD:ALL
    lock_passwd: true
    ssh-authorized-keys:
      - ssh-rsa %s
    """ % sshKey

  #print "AT >>> user_data", user_data
  #print "AT >>> cloud_config",  cloud_config

  return createMimeData( ( ( user_data, 'x-shellscript', 'dirac_boot.sh' ),
                           ( cloud_config, 'cloud-config', 'cloud-config') ) )

def createUserDataScript(parameters):

  defaultUser = os.environ['USER']
  sshUser = parameters.get('SshUser', defaultUser)
  defaultKey = os.path.expandvars('$HOME/.ssh/id_rsa.pub')
  sshKeyFile = parameters.get('SshKey', defaultKey)
  with open(sshKeyFile) as skf:
    sshKey = skf.read().strip()

  script = """
# Allow root login
sed -i 's/PermitRootLogin no/PermitRootLogin yes/g' /etc/ssh/sshd_config
sed -i 's/PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config
# Copy id_rsa.pub to authorized_keys
echo \" """ + sshKey + """\" > /root/.ssh/authorized_keys
service sshd restart
"""

  if "HEPIX" in parameters:
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

  if sshKey:
    cloud_config += """
users:
  - name: %s
    sudo: ALL=(ALL) NOPASSWD:ALL
    lock_passwd: false
    passwd: $1$SaltSalt$YnAUwnq0wHdTmxfRXAbFU.
    ssh-authorized-keys:
      - %s
    """ % (sshUser, sshKey)

    mime = createMimeData( ( ( user_data, 'x-shellscript', 'dirac_boot.sh' ),
                           ( cloud_config, 'cloud-config', 'cloud-config') ) )
    return mime
