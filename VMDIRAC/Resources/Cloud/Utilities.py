import sys

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