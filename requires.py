# Copyright 2017 Canonical Ltd
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

import json

from charms.reactive import hook
from charms.reactive import RelationBase
from charms.reactive import scopes
from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import log
from charmhelpers.contrib.network.ip import format_ipv6_addr

from charmhelpers.contrib.storage.linux.ceph import (
    CephBrokerRq,
    is_request_complete,
    send_request_if_needed,
)


class CephClientRequires(RelationBase):
    scope = scopes.GLOBAL

    auto_accessors = ['auth', 'key']

    @hook('{requires:ceph-client}-relation-{joined}')
    def joined(self):
        self.set_state('{relation_name}.connected')

    @hook('{requires:ceph-client}-relation-{changed,departed}')
    def changed(self):
        data = {
            'key': self.key(),
            'auth': self.auth(),
            'mon_hosts': self.mon_hosts()
        }
        if all(data.values()):
            self.set_state('{relation_name}.available')

        all_requests = self.get_local(key='broker_reqs')
        if all_requests:
            incomplete = []
            for name, json_rq in all_requests.items():
                req = json.loads(json_rq)
                if not is_request_complete(req['ops']):
                    incomplete.append(name)
            if len(incomplete) == 0:
                self.set_state('{relation_name}.pools.available')
            else:
                log("incomplete requests {}.".format(incomplete.join(', ')))

    @hook('{requires:ceph-client}-relation-{broken}')
    def broken(self):
        self.remove_state('{relation_name}.available')
        self.remove_state('{relation_name}.connected')
        self.remove_state('{relation_name}.pools.available')

    def create_pool(self, name, replicas=3, weight=None, pg_num=None,
                    group=None, namespace=None):
        """
        Request pool setup

        @param name: Name of pool to create
        @param replicas: Number of replicas for supporting pools
        @param weight: The percentage of data the pool makes up
        @param pg_num: If not provided, this value will be calculated by the
                       broker based on how many OSDs are in the cluster at the
                       time of creation. Note that, if provided, this value
                       will be capped at the current available maximum.
        @param group: Group to add pool to.
        @param namespace: A group can optionally have a namespace defined that
                          will be used to further restrict pool access.
        """
        # json.dumps of the CephBrokerRq()
        requests = self.get_local(key='broker_reqs') or {}

        if name not in requests:
            rq = CephBrokerRq()
            rq.add_op_create_pool(name="{}".format(name),
                                  replica_count=replicas,
                                  pg_num=pg_num,
                                  weight=weight,
                                  group=group,
                                  namespace=namespace)
            if not requests:
                requests = {}

            requests[name] = rq.request
            self.set_local(key='broker_reqs', value=requests)
            send_request_if_needed(rq, relation=self.relation_name)
            self.remove_state('{relation_name}.pools.available')
        else:
            rq = CephBrokerRq()
            try:
                j = json.loads(requests[name])
                log("Json request: {}".format(requests[name]))
                rq.ops = j['ops']
                send_request_if_needed(rq, relation=self.relation_name)
            except ValueError as err:
                log("Unable to decode broker_req: {}.  Error: {}".format(
                    requests[name], err))

    def create_pools(self, names, replicas=3, weight=None, pg_num=None,
                     group=None, namespace=None):
        """
        Request pools setup

        @param name: list of pool names to create
        @param replicas: number of replicas for supporting pools
        @param weight: The percentage of data the pool makes up
        @param pg_num: If not provided, this value will be calculated by the
                       broker based on how many OSDs are in the cluster at the
                       time of creation. Note that, if provided, this value
                       will be capped at the current available maximum.
        @param group: Group to add pool to.
        @param namespace: A group can optionally have a namespace defined that
                          will be used to further restrict pool access.
        """
        # json.dumps of the CephBrokerRq()
        requests = self.get_local(key='broker_reqs') or {}

        new_names = [name for name in names if name not in requests]

        # existing names get ignored here
        # new names get added to a single request
        if new_names:
            rq = CephBrokerRq()
            for name in new_names:
                rq.add_op_create_pool(name="{}".format(name),
                                      replica_count=replicas,
                                      pg_num=pg_num,
                                      weight=weight,
                                      group=group,
                                      namespace=namespace)
                requests[name] = rq.request
            self.set_local(key='broker_reqs', value=requests)
            send_request_if_needed(rq, relation=self.relation_name)
            self.remove_state('{relation_name}.pools.available')

    def request_access_to_group(self, name, namespace=None, permission=None,
                                key_name=None, object_prefix_permissions=None):
        """
        Adds the requested permissions to service's Ceph key

        Adds the requested permissions to the current service's Ceph key,
        allowing the key to access only the specified pools or
        object prefixes. object_prefix_permissions should be a dictionary
        keyed on the permission with the corresponding value being a list
        of prefixes to apply that permission to.
            {
                'rwx': ['prefix1', 'prefix2'],
                'class-read': ['prefix3']}
        @param name: Target group name for permissions request.
        @param namespace: namespace to further restrict pool access.
        @param permission: Permission to be requested against pool
        @param key_name: userid to grant permission to
        @param object_prefix_permissions: Add object_prefix permissions.
        """
        current_request = self.get_current_request()
        current_request.add_op_request_access_to_group(
            name,
            namespace=namespace,
            permission=permission,
            key_name=key_name,
            object_prefix_permissions=object_prefix_permissions)
        self.set_local(key='broker_req', value=current_request.request)
        send_request_if_needed(current_request, relation=self.relation_name)

    def get_remote_all(self, key, default=None):
        """Return a list of all values presented by remote units for key"""
        # TODO: might be a nicer way todo this - written a while back!
        values = []
        for conversation in self.conversations():
            for relation_id in conversation.relation_ids:
                for unit in hookenv.related_units(relation_id):
                    value = hookenv.relation_get(key,
                                                 unit,
                                                 relation_id) or default
                    if value:
                        values.append(value)
        return list(set(values))

    def mon_hosts(self):
        """List of all monitor host public addresses"""
        hosts = []
        addrs = self.get_remote_all('ceph-public-address')
        for ceph_addrs in addrs:
            # NOTE(jamespage): This looks odd but deals with
            #                  use with ceph-proxy which
            #                  presents all monitors in
            #                  a single space delimited field.
            for addr in ceph_addrs.split(' '):
                hosts.append(format_ipv6_addr(addr) or addr)
        hosts.sort()
        return hosts
