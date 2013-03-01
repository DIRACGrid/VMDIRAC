# $HeadURL$
''' VMDIRAC package
  
  DIRAC extension providing all necessary components to overcome the incompatibilities
  of Grid software on a CloudComputing environment.
  
  DIRACWeb extension with controllers to enhance user experience providing a GUI
  where to use the package.
  
  Two main components: Web ( DIRACWeb extension ) and WorkloadManagementSystem ( DIRAC ).
       
'''

from pkgutil import extend_path

__RCSID__ = '$Id: __init__.py 46 2010-03-18 14:52:51Z adriancasajus@gmail.com $'

#FIXME: why defined here ?
__path__ = extend_path( __path__, __name__ )

# Define Version

majorVersion = 1
minorVersion = 0
patchLevel   = 0
preVersion   = 1

version      = "v%sr%s" % ( majorVersion, minorVersion )
buildVersion = "v%dr%d" % ( majorVersion, minorVersion )

if patchLevel:
  version      = "%sp%s"       % ( version, patchLevel )
  buildVersion = "%s build %s" % ( buildVersion, patchLevel )

if preVersion:
  version      = "%s-pre%s"  % ( version, preVersion )
  buildVersion = "%s pre %s" % ( buildVersion, preVersion )
  
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF