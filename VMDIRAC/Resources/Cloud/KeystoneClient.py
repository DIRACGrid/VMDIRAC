""" KeystoneClient class encapsulates the work with the keystone service interface
"""

__RCSID__ = '$Id$'

import requests
import json

from DIRAC import S_OK, S_ERROR


class KeystoneClient():
  """
  """

  def __init__(self, url, authArgs):
    self.url = url
    self.authArgs = authArgs
    self.token = None

  def getToken(self):

    if self.token is not None:
      return S_OK(self.token)

    try:
      result = requests.post("%s/v2.0/tokens" % self.url,
                             headers={"Content-Type": "application/json"},
                             **self.authArgs).json()
    except Exception as exc:
      #print "AT >> getToken", exc
      return S_ERROR('Failed to get keystone token: %s', str(exc))

    #print "AT >>> getToken",result

    self.token = str(result['access']['token']['id'])
    return S_OK(self.token)

  def getTenants(self):

    try:
      result = requests.get("%s/v2.0/tenants/" % self.url,
                            headers={"Content-Type": "application/json",
                                     "X-Auth-Token": self.token},
                            **self.authArgs).json()
    except Exception as exc:
      return S_ERROR('Failed to get tenants: %s' % str(exc))

    tenants = [tenant['name'] for tenant in result['tenants']]
    return S_OK(tenants)

  def getTenantToken(self, tenants):

    authArgs = dict(self.authArgs)
    del authArgs['data']

    for tenant in tenants:
      try:
        data = '{"auth": {"voms": true, "tenantName": "%s"}}' % tenant
        result = requests.post("%s/v2.0/tokens" % self.url,
                               data=data,
                               headers={'Accept': 'application/json',
                                        'X-Auth-Token': self.token,
                                        'User-Agent': 'VMDIRAC v3r0' + ' ( OCCI/1.1 )',
                                        'Content-Type': 'application/json',
                                        'Content-Length': str(len(json.dumps(data)))},
                               **authArgs).json()
      except Exception as exc:
        return S_ERROR('Can not get token for tenant', '%s: %s' % (tenant, repr(exc)))

      if 'access' in result:
        return S_OK(result['access']['token']['id'])

    return S_ERROR('Failed to get token for any tenant')
