# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import unittest
from unittest import mock

from charmhelpers.contrib.storage.linux.ceph import (
    CephBrokerRq,
)

with mock.patch('charmhelpers.core.hookenv.metadata') as _meta:
    _meta.return_Value = 'ss'
    from ceph_client import requires

import charmhelpers


_hook_args = {}

TO_PATCH = []
TO_PATCH_BASE_REQUIRES = [
    'is_request_complete',
]


def mock_hook(*args, **kwargs):

    def inner(f):
        # remember what we were passed.  Note that we can't actually determine
        # the class we're attached to, as the decorator only gets the function.
        _hook_args[f.__name__] = dict(args=args, kwargs=kwargs)
        return f
    return inner


class DummyRequest(CephBrokerRq):

    def __init__(self, req_json=None, request_id=12):
        super().__init__(request_id=request_id)
        if req_json:
            self.set_ops(json.loads(req_json)['ops'])


class TestCephClientRequires(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._patched_hook = mock.patch('charms.reactive.when', mock_hook)
        cls._patched_hook_started = cls._patched_hook.start()
        # force requires to rerun the mock_hook decorator:
        # try except is Python2/Python3 compatibility as Python3 has moved
        # reload to importlib.
        try:
            reload(requires)
        except NameError:
            import importlib
            importlib.reload(requires)

    @classmethod
    def tearDownClass(cls):
        cls._patched_hook.stop()
        cls._patched_hook_started = None
        cls._patched_hook = None
        # and fix any breakage we did to the module
        try:
            reload(requires)
        except NameError:
            import importlib
            importlib.reload(requires)

    def patch(self, method, obj=None):
        target_obj = obj or self.obj
        _m = mock.patch.object(target_obj, method)
        _mock = _m.start()
        self.addCleanup(_m.stop)
        return _mock

    def setUp(self):
        self.cr = requires.CephClientRequires('some-relation', [])
        self._patches = {}
        self._patches_start = {}
        self.obj = requires
        for method in TO_PATCH:
            setattr(self, method, self.patch(method))
        for method in TO_PATCH_BASE_REQUIRES:
            setattr(self, method, self.patch(method, requires.base_requires))
        self.patch_object(requires.base_requires.reactive, 'set_flag')
        self.patch_object(requires.base_requires.reactive, 'clear_flag')

    def tearDown(self):
        self.cr = None
        for k, v in self._patches.items():
            v.stop()
            setattr(self, k, None)
        self._patches = None
        self._patches_start = None

    def patch_kr(self, attr, return_value=None):
        mocked = mock.patch.object(self.cr, attr)
        self._patches[attr] = mocked
        started = mocked.start()
        started.return_value = return_value
        self._patches_start[attr] = started
        setattr(self, attr, started)

    def patch_object(self, obj, attr, name=None, **kwargs):
        """Patch a patchable thing.  Uses mock.patch.object() to do the work.
        Automatically unpatches at the end of the test.

        The mock gets added to the test object (self) using 'name' or the attr
        passed in the arguments.

        :param obj: an object that needs to have an attribute patched.
        :param attr: <string> that represents the attribute being patched.
        :param name: optional <string> name to call the mock.
        :param **kwargs: any other args to pass to mock.patch()
        """
        mocked = mock.patch.object(obj, attr, **kwargs)
        if name is None:
            name = attr
        started = mocked.start()
        self._patches[name] = mocked
        self._patches_start[name] = started
        setattr(self, name, started)

    def test_registered_hooks(self):
        # test that the decorators actually registered the relation
        # expressions that are meaningful for this interface: this is to
        # handle regressions.
        # The keys are the function names that the hook attaches to.
        hook_patterns = {
            'data_changed': ('endpoint.{endpoint_name}.changed', ),
            'joined': ('endpoint.{endpoint_name}.joined', ),
            'broken': ('endpoint.{endpoint_name}.broken', ),
            'changed': ('endpoint.{endpoint_name}.changed', ),
            'departed': ('endpoint.{endpoint_name}.departed', ),
        }
        for k, v in _hook_args.items():
            self.assertEqual(hook_patterns[k], v['args'])

    def test_data_changed(self):
        self.patch_kr('_key', 'key1')
        self.patch_kr('_auth', 'auth1')
        self.patch_kr('mon_hosts', 'host1')
        self.cr.changed()
        self.set_flag.assert_called_once_with('some-relation.available')

    def test_data_changed_incomplete(self):
        self.patch_kr('_key', 'key1')
        self.patch_kr('_auth', None)
        self.patch_kr('mon_hosts', 'host1')
        self.cr.changed()
        self.assertFalse(self.set_flag.called)

    def test_data_changed_existing_broker_rq(self):
        self.patch_kr('_key', 'key1')
        self.patch_kr('_auth', 'auth1')
        self.patch_kr('mon_hosts', 'host1')
        self.patch_kr('get_current_request', DummyRequest())
        self.is_request_complete.return_value = True
        self.cr.changed()
        self.set_flag.assert_has_calls([
            mock.call('some-relation.available'),
            mock.call('some-relation.pools.available')])

    def test_date_changed_existing_broker_rq_incomplete(self):
        self.patch_kr('_key', 'key1')
        self.patch_kr('_auth', 'auth1')
        self.patch_kr('mon_hosts', 'host1')
        self.is_request_complete.return_value = False
        self.cr.changed()
        # Side effect of asserting pools.available was not set.
        self.set_flag.assert_called_once_with('some-relation.available')

    def test_broken(self):
        self.cr.broken()
        self.clear_flag.assert_has_calls([
            mock.call('some-relation.available'),
            mock.call('some-relation.connected'),
            mock.call('some-relation.pools.available')])

    @mock.patch.object(charmhelpers.contrib.storage.linux.ceph.uuid, 'uuid1')
    def test_create_pool_new_request(self, _uuid1):
        self.patch_kr('get_current_request', None)
        self.patch_kr('send_request_if_needed')
        _uuid1.return_value = '9e34123e-fa0c-11e8-ad9c-fa163ed1cc55'
        self.cr.create_pool('bob')
        ceph_broker_rq = self.send_request_if_needed.mock_calls[0][1][0]
        self.assertEqual(
            ceph_broker_rq.ops,
            [{
                'op': 'create-pool',
                'name': 'bob',
                'replicas': 3,
                'group': None,
                'group-namespace': None,
                'pg_num': None,
                'weight': None,
                'app-name': None,
                'max-bytes': None,
                'max-objects': None}])

    @mock.patch.object(charmhelpers.contrib.storage.linux.ceph.uuid, 'uuid1')
    def test_create_pool_existing_request(self, _uuid1):
        self.patch_kr('send_request_if_needed')
        _uuid1.return_value = '9e34123e-fa0c-11e8-ad9c-fa163ed1cc55'
        req = (
            '{"api-version": 1, '
            '"ops": [{"op": "create-pool", "name": "bob", "replicas": 3, '
            '"pg_num": null, "weight": null, "group": null, '
            '"group-namespace": null, "app-name": null, "max-bytes": null, '
            '"max-objects": null}], '
            '"request-id": "9e34123e-fa0c-11e8-ad9c-fa163ed1cc55"}')
        existing_request = DummyRequest(req_json=req)
        self.patch_kr('get_current_request', existing_request)
        self.cr.create_pool('bob')
        ceph_broker_rq = self.send_request_if_needed.mock_calls[0][1][0]
        self.assertEqual(
            ceph_broker_rq.ops,
            [{
                'op': 'create-pool',
                'name': 'bob',
                'replicas': 3,
                'group': None,
                'group-namespace': None,
                'pg_num': None,
                'max-bytes': None,
                'max-objects': None,
                'app-name': None,
                'weight': None}])

    def test_request_access_to_group_new_request(self):
        self.patch_kr('send_request_if_needed')
        self.cr.request_access_to_group(
            'volumes',
            key_name='cinder',
            object_prefix_permissions={'class-read': ['rbd_children']},
            permission='rwx')
        ceph_broker_rq = self.send_request_if_needed.mock_calls[0][1][0]
        self.assertEqual(
            ceph_broker_rq.ops,
            [{
                'group': 'volumes',
                'group-permission': 'rwx',
                'name': 'cinder',
                'namespace': None,
                'object-prefix-permissions': {'class-read': ['rbd_children']},
                'op': 'add-permissions-to-key'}])

    def test_request_access_to_group_existing_request(self):
        self.patch_kr('send_request_if_needed')
        req = (
            '{"api-version": 1, '
            '"ops": [{"op": "create-pool", "name": "volumes", "replicas": 3, '
            '"pg_num": null, "weight": null, "group": null, '
            '"group-namespace": null}], '
            '"request-id": "9e34123e-fa0c-11e8-ad9c-fa163ed1cc55"}')
        existing_request = DummyRequest(req_json=req)
        self.patch_kr('get_current_request', existing_request)
        self.cr.request_access_to_group(
            'volumes',
            key_name='cinder',
            object_prefix_permissions={'class-read': ['rbd_children']},
            permission='rwx')
        self.assertEqual(
            existing_request.ops,
            [
                {
                    'op': 'create-pool',
                    'name': 'volumes',
                    'replicas': 3,
                    'group': None,
                    'group-namespace': None,
                    'pg_num': None,
                    'weight': None},
                {
                    'group': 'volumes',
                    'group-permission': 'rwx',
                    'name': 'cinder',
                    'namespace': None,
                    'object-prefix-permissions': {
                        'class-read': ['rbd_children']},
                    'op': 'add-permissions-to-key'}])

    def test_get_remote_all(self):
        unit_data = {
            'rid:1': {
                'app1/0': {
                    'key1': 'value1',
                    'key2': 'value2'},
                'app1/1': {
                    'key1': 'value1',
                    'key2': 'value3'}},
            'rid:2': {
                'app2/0': {
                    'key1': 'value1',
                    'key2': 'value3'}},
            'rid:3': {}}
        unit0_r1_mock = mock.MagicMock()
        unit0_r1_mock.received = unit_data['rid:1']['app1/0']
        unit1_r1_mock = mock.MagicMock()
        unit1_r1_mock.received = unit_data['rid:1']['app1/1']
        unit0_r2_mock = mock.MagicMock()
        unit0_r2_mock.received = unit_data['rid:2']['app2/0']
        rel1 = mock.MagicMock()
        rel1.units = [unit0_r1_mock, unit1_r1_mock]
        rel2 = mock.MagicMock()
        rel2.units = [unit0_r2_mock]
        rel3 = mock.MagicMock()
        rel3.units = []

        self.patch_kr('_relations')
        self._relations.__iter__.return_value = [rel1, rel2, rel3]
        # Check de-duplication:
        self.assertEqual(
            self.cr.get_remote_all('key1'),
            ['value1'])
        # Check multiple values:
        self.assertEqual(
            self.cr.get_remote_all('key2'),
            ['value2', 'value3'])
        # Check missing key
        self.assertEqual(
            self.cr.get_remote_all('key100'),
            [])
        # Check missing key with default
        self.assertEqual(
            self.cr.get_remote_all('key100', default='defaultvalue'),
            ['defaultvalue'])

    def test_mon_hosts(self):
        self.patch_kr('get_remote_all', ['10.0.0.10 10.0.0.12', '10.0.0.23'])
        self.assertEqual(
            self.cr.mon_hosts(),
            ['10.0.0.10', '10.0.0.12', '10.0.0.23'])
