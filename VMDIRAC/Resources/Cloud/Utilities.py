import sys

from DIRAC import S_OK, S_ERROR
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

STATE_MAP = {0: 'RUNNING',
             1: 'REBOOTING',
             2: 'TERMINATED',
             3: 'PENDING',
             4: 'UNKNOWN',
             5: 'STOPPED',
             6: 'SUSPENDED',
             7: 'ERROR',
             8: 'PAUSED'}


def createMimeData(userDataTuple):

  userData = MIMEMultipart()
  for contents, mtype, fname in userDataTuple:
    try:
      mimeText = MIMEText(contents, mtype, sys.getdefaultencoding())
      mimeText.add_header('Content-Disposition', 'attachment; filename="%s"' % fname)
      userData.attach(mimeText)
    except Exception as e:
      return S_ERROR(str(e))

  return S_OK(userData)


def createUserDataScript(vmParameters, bootstrapParameters):

  userDataDict = {}

  # Arguments to the vm-bootstrap command
  bootstrapArgs = {'dirac-site': vmParameters['Site'],
                   'submit-pool': vmParameters.get('SubmitPool', ''),
                   'ce-name': vmParameters['CEName'],
                   'image-name': vmParameters['Image'],
                   'vm-uuid': vmParameters['VMUUID'],
                   'vmtype': vmParameters['VMType'],
                   'vo': vmParameters['VO'],
                   'running-pod': vmParameters.get('RunningPod', ''),
                   'cvmfs-proxy': vmParameters.get('CVMFSProxy', 'None'),
                   'cs-servers': ','.join(vmParameters.get('CSServers', [])),
                   'number-of-processors': vmParameters.get('NumberOfProcessors', 1),
                   'whole-node': vmParameters.get('WholeNode', True),
                   'required-tag': vmParameters.get('RequiredTag', ''),
                   'release-version': bootstrapParameters['Version'],
                   'release-project': bootstrapParameters['Project'],
                   'setup': bootstrapParameters['Setup']}

  print "AT >>> bootstrapArgs", bootstrapArgs

  bootstrapString = ''
  for key, value in bootstrapArgs.items():
    bootstrapString += " --%s=%s \\\n" % (key, value)
  userDataDict['bootstrapArgs'] = bootstrapString

  userDataDict['user_data_commands_base_url'] = bootstrapParameters.get('user_data_commands_base_url')
  if not userDataDict['user_data_commands_base_url']:
    return S_ERROR('user_data_commands_base_url is not defined')
  with open(bootstrapParameters['HostCert']) as cfile:
    userDataDict['user_data_file_hostkey'] = cfile.read().strip()
  with open(bootstrapParameters['HostKey']) as kfile:
    userDataDict['user_data_file_hostcert'] = kfile.read().strip()
  sshKey = None
  userDataDict['add_root_ssh_key'] = ""
  if 'SshKey' in bootstrapParameters:
    with open(bootstrapParameters['SshKey']) as sfile:
      sshKey = sfile.read().strip()
      userDataDict['add_root_ssh_key'] = """
        # Allow root login
        sed -i 's/PermitRootLogin no/PermitRootLogin yes/g' /etc/ssh/sshd_config
        # Copy id_rsa.pub to authorized_keys
        echo \" """ + sshKey + """\" > /root/.ssh/authorized_keys
        service sshd restart"""

  # List of commands to be downloaded
  bootstrapCommands = bootstrapParameters.get('user_data_commands')
  if isinstance(bootstrapCommands, basestring):
    bootstrapCommands = bootstrapCommands.split(',')
  if not bootstrapCommands:
    return S_ERROR('user_data_commands list is not defined')
  userDataDict['bootstrapCommands'] = ' '.join(bootstrapCommands)

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

  return createMimeData(((user_data, 'text/x-shellscript', 'dirac_boot.sh'),
                         (cloud_config, 'text/cloud-config', 'cloud-config')))
