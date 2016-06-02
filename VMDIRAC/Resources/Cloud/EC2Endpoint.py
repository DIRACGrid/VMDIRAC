""" EC2Endpoint class is the implementation of the EC2 interface to
    a cloud endpoint
"""

__RCSID__ = '$Id$'

from libcloud.compute.drivers.ec2 import REGION_DETAILS

REGION_DETAILS["cn-north-1"] = {
        'endpoint': 'ec2.cn-north-1.amazonaws.cn',
        'api_name': 'ec2_cn_north',
        'country': 'China',
        'signature_version': '2',
        'instance_types': [
            't1.micro',
            'm1.small',
            'm1.medium',
            'm1.large',
            'm1.xlarge',
            'm2.xlarge',
            'm2.2xlarge',
            'm2.4xlarge',
            'm3.medium',
            'm3.large',
            'm3.xlarge',
            'm3.2xlarge',
            'm4.large',
            'm4.xlarge',
            'm4.2xlarge',
            'm4.4xlarge',
            'm4.10xlarge',
            'c1.medium',
            'c1.xlarge',
            'cc2.8xlarge',
            'c3.large',
            'c3.xlarge',
            'c3.2xlarge',
            'c3.4xlarge',
            'c3.8xlarge',
            'c4.large',
            'c4.xlarge',
            'c4.2xlarge',
            'c4.4xlarge',
            'c4.8xlarge',
            'cg1.4xlarge',
            'g2.2xlarge',
            'g2.8xlarge',
            'cr1.8xlarge',
            'hs1.8xlarge',
            'i2.xlarge',
            'i2.2xlarge',
            'i2.4xlarge',
            'i2.8xlarge',
            'd2.xlarge',
            'd2.2xlarge',
            'd2.4xlarge',
            'd2.8xlarge',
            'r3.large',
            'r3.xlarge',
            'r3.2xlarge',
            'r3.4xlarge',
            'r3.8xlarge',
            't2.nano',
            't2.micro',
            't2.small',
            't2.medium',
            't2.large'
        ]
    }

from DIRAC import gLogger
from VMDIRAC.Resources.Cloud.CloudEndpoint import CloudEndpoint
from VMDIRAC.Resources.Cloud.Endpoint import Endpoint

class EC2Endpoint( CloudEndpoint ):

  def __init__( self, parameters = {} ):
    """
    """
    Endpoint.__init__( self, parameters = parameters )
    # logger
    self.log = gLogger.getSubLogger( 'EC2Endpoint' )
    self.valid = False
    result = self.initialize()
    if result['OK']:
      self.log.debug( 'EC2Endpoint created and validated' )
      self.valid = True