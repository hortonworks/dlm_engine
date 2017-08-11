
"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your
company and Hortonworks, Inc. or an authorized affiliate or partner
thereof, any use, reproduction, modification, redistribution, sharing,
lending or other exploitation of all or any part of the contents of this
software is strictly prohibited.
"""

from resource_management import *
from ambari_commons import OSConst
from ambari_commons.os_family_impl import OsFamilyFuncImpl, OsFamilyImpl

class BeaconServiceCheck(Script):
  pass

@OsFamilyImpl(os_family=OsFamilyImpl.DEFAULT)
class BeaconServiceCheckLinux(BeaconServiceCheck):
  def service_check(self, env):
    pass


if __name__ == "__main__":
  BeaconServiceCheck().execute()
