# Copyright 2020 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket

from .lib import base_requires

from charms.reactive import (
    when,
)
from charmhelpers.contrib.storage.linux.ceph import (
    CephBrokerRq,
)


class CephClient(base_requires.CephRequires):

    ceph_pool_app_name = 'cephfs'

    @when('endpoint.{endpoint_name}.joined')
    def joined(self):
        super().joined()

    @when('endpoint.{endpoint_name}.changed')
    def changed(self):
        super().changed()

    @when('endpoint.{endpoint_name}.departed')
    def departed(self):
        super().changed()

    @when('endpoint.{endpoint_name}.broken')
    def broken(self):
        super().broken()

    @property
    def fsid(self):
        return self._fsid()

    def _fsid(self):
        return self.all_joined_units.received.get('fsid')

    def mds_key(self):
        """Retrieve the cephx key for the local mds unit"""
        return self.all_joined_units.received.get(
            '{}_mds_key'.format(socket.gethostname()))

    def initial_ceph_response(self):
        data = {
            'mds_key': self.mds_key(),
            'fsid': self.fsid,
            'auth': self.auth,
            'mon_hosts': self.mon_hosts()
        }
        return data

    def announce_mds_name(self):
        for relation in self.relations:
            relation.to_publish_raw['mds-name'] = socket.gethostname()

    def request_cephfs(self, name):
        rq = self.get_current_request() or CephBrokerRq()
        rq.add_op({
            'op': 'create-cephfs',
            'mds_name': name,
            'data_pool': "{}_data".format(name),
            'metadata_pool': "{}_metadata".format(name)})
        self.send_request_if_needed(rq)

    def initialize_mds(self, name, replicas=3):
        """
        Request pool setup and mds creation

        @param name: name of mds pools to create
        @param replicas: number of replicas for supporting pools
        """
        self.create_replicated_pool(
            name="{}_data".format(name),
            replicas=replicas,
            weight=None,
            app_name=self.ceph_pool_app_name)
        self.create_replicated_pool(
            name="{}_metadata".format(name),
            replicas=replicas,
            weight=None,
            app_name=self.ceph_pool_app_name)
        self.request_cephfs(name)
