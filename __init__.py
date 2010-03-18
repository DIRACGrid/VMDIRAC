# $HeadURL$
from pkgutil import extend_path
__path__ = extend_path( __path__, __name__ )

__RCSID__ = "$Id$"

# Define Version

majorVersion = 1
minorVersion = 0
patchLevel = 0
preVersion = 1

version = "v%sr%s" % ( majorVersion, minorVersion )
buildVersion = "v%dr%d" % ( majorVersion, minorVersion )

if patchLevel:
  version = "%sp%s" % ( version, patchLevel )
  buildVersion = "%s build %s" % ( buildVersion, patchLevel )
if preVersion:
  version = "%s-pre%s" % ( version, preVersion )
  buildVersion = "%s pre %s" % ( buildVersion, preVersion )
