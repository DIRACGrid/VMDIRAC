""" KeystoneClient class encapsulates the work with the keystone service interface
"""

__RCSID__ = '$Id$'

import requests
import json

from DIRAC import S_OK, S_ERROR, gLogger

class KeystoneClient():
  """
  """
  def __init__(self, url, parameters):
    self.log = gLogger.getSubLogger("Keystone")
    self.url = url
    self.apiVersion = 2
    if "v3" in url:
      self.apiVersion = 3
    self.parameters = parameters
    self.token = None
    self.project = self.parameters.get('Project')
    self.projectID = ''
    self.computeURL = None
    self.imageURL = None
    self.networkURL = None

    #result = self.getToken()

  def getToken(self):

    self.log.debug("Initializing for API version %d" % self.apiVersion)

    if self.apiVersion == 2:
      result = self.__getToken2()
    else:
      result = self.__getToken3()

    return result

  def __getToken2(self):

    if self.token is not None:
      return S_OK(self.token)

    user = self.parameters.get('User')
    password = self.parameters.get('Password')
    if user and password:
      authDict = {'auth': {"passwordCredentials": {"username": user,
                                                   "password": password}
                          }
                 }
      if self.project:
        authDict['auth']['tenantName'] = self.project
    elif self.parameters.get('Auth') == "voms":
      authDict = {'auth': {'voms': True}}

    authArgs = {}
    caPath = self.parameters.get('CAPath')
    if caPath:
      authArgs['verify'] = caPath
    if self.parameters.get('Proxy'):
      authArgs['cert'] = self.parameters.get('Proxy')

    authJson = json.dumps(authDict)

    try:
      result = requests.post("%s/tokens" % self.url,
                             headers = {"Content-Type": "application/json",
                                        'Content-Length': str(len(json.dumps(authJson)))},
                             data = authJson,
                             **authArgs)
    except Exception as exc:
      print "AT >> getToken", exc
      return S_ERROR('Failed to get keystone token: %s', str(exc))

    output = result.json()

    import pprint
    pprint.pprint(output)

    self.token = str(output['access']['token']['id'])
    for endpoint in output['access']['serviceCatalog']:
      if endpoint['type'] == 'compute':
        self.computeURL = str(endpoint['endpoints'][0]['publicURL'])
      elif endpoint['type'] == 'image':
        self.imageURL = str(endpoint['endpoints'][0]['publicURL'])
      elif endpoint['type'] == 'network':
        self.networkURL = str(endpoint['endpoints'][0]['publicURL'])
    return S_OK(self.token)

  def __getToken3(self):

    if self.token is not None:
      return S_OK(self.token)

    domain = self.parameters.get('Domain', "Default")
    user = self.parameters.get('User')
    password = self.parameters.get('Password')
    authDict = {}
    if user and password:
      authDict = {'auth': {"identity": {"methods": ["password"],
                                        "password": {"user": {"name": user,
                                                              "domain": {"name": domain},
                                                              "password": password
                                                              }
                                                    }
                                        }
                           }
                  }

    elif self.parameters.get('Auth') == "voms":
      authDict['auth'] = {'voms': True}

    if self.project:
      authDict['auth']['scope'] = {"project": {"domain": {"name": domain},
                                       "name": self.project
                                       }
                           }


    authArgs = {}
    caPath = self.parameters.get('CAPath')
    if caPath:
      authArgs['verify'] = caPath
    if self.parameters.get('Proxy'):
      authArgs['cert'] = self.parameters.get('Proxy')

    authJson = json.dumps(authDict)

    try:
      result = requests.post("%s/auth/tokens" % self.url,
                             headers = {"Content-Type": "application/json",
                                        'Content-Length': str(len(json.dumps(authJson)))},
                             data = authJson,
                             **authArgs)
    except Exception as exc:
      print "AT >> getToken", exc
      return S_ERROR('Failed to get keystone token: %s', str(exc))

    try:
      self.token = result.headers['X-Subject-Token']
    except Exception as exc:
      return S_ERROR('Failed to get keystone token: %s', str(exc))

    output = result.json()
    import pprint
    pprint.pprint(output)

    if output['token']['project']['name'] == self.project:
      self.projectID = output['token']['project']['id']


    for service in output['token']['catalog']:
      if service['type'] == 'compute':
        for endpoint in service['endpoints']:
          if endpoint['interface'] == 'public':
            self.computeURL = str(endpoint['url'])

      elif service['type'] == 'image':
        for endpoint in service['endpoints']:
          if endpoint['interface'] == 'public':
            self.imageURL = str(endpoint['url'])

      elif service['type'] == 'network':
        for endpoint in service['endpoints']:
          if endpoint['interface'] == 'public':
            self.networkURL = str(endpoint['url'])

    return S_OK(self.token)

