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


import unittest
import mock


with mock.patch('charmhelpers.core.hookenv.metadata') as _meta:
    _meta.return_Value = 'ss'
    import requires

import charmhelpers


_hook_args = {}

TO_PATCH = [
    'is_request_complete',
    'send_request_if_needed',
]


def mock_hook(*args, **kwargs):

    def inner(f):
        # remember what we were passed.  Note that we can't actually determine
        # the class we're attached to, as the decorator only gets the function.
        _hook_args[f.__name__] = dict(args=args, kwargs=kwargs)
        return f
    return inner


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

    def patch(self, method):
        _m = mock.patch.object(self.obj, method)
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

    def test_registered_hooks(self):
        # test that the decorators actually registered the relation
        # expressions that are meaningful for this interface: this is to
        # handle regressions.
        # The keys are the function names that the hook attaches to.
        hook_patterns = {
            'data_changed': ('endpoint.{endpoint_name}.changed', ),
            'joined': ('endpoint.{endpoint_name}.joined', ),
            'broken': ('endpoint.{endpoint_name}.joined', ),
        }
        for k, v in _hook_args.items():
            self.assertEqual(hook_patterns[k], v['args'])

    def test_date_changed(self):
        self.patch_kr('key', 'key1')
        self.patch_kr('auth', 'auth1')
        self.patch_kr('mon_hosts', 'host1')
        self.patch_kr('get_local', None)
        self.patch_kr('set_state')
        self.cr.changed()
        self.set_state.assert_called_once_with('{relation_name}.available')

    def test_date_changed_incomplete(self):
        self.patch_kr('key', 'key1')
        self.patch_kr('auth', None)
        self.patch_kr('mon_hosts', 'host1')
        self.patch_kr('get_local', None)
        self.patch_kr('set_state')
        self.cr.changed()
        self.assertFalse(self.set_state.called)

    def test_date_changed_existing_broker_rq(self):
        broker_req = (
            '{"api-version": 1, '
            '"request-id": "4f7e247d-f953-11e8-a4f3-fa163e55565e",'
            '"ops": [{"group": "volumes", "name": "cinder-ceph", '
            '"weight": 40, "replicas": 3, "pg_num": null, '
            '"group-namespace": null, "op": "create-pool"}]}')
        self.patch_kr('key', 'key1')
        self.patch_kr('auth', 'auth1')
        self.patch_kr('mon_hosts', 'host1')
        self.patch_kr('get_local', broker_req)
        self.patch_kr('set_state')
        self.is_request_complete.return_value = True
        self.cr.changed()
        self.set_state.assert_has_calls([
            mock.call('{relation_name}.available'),
            mock.call('{relation_name}.pools.available')])

    def test_date_changed_existing_broker_rq_incomplete(self):
        broker_req = (
            '{"api-version": 1, '
            '"request-id": "4f7e247d-f953-11e8-a4f3-fa163e55565e",'
            '"ops": [{"group": "volumes", "name": "cinder-ceph", '
            '"weight": 40, "replicas": 3, "pg_num": null, '
            '"group-namespace": null, "op": "create-pool"}]}')
        self.patch_kr('key', 'key1')
        self.patch_kr('auth', 'auth1')
        self.patch_kr('mon_hosts', 'host1')
        self.patch_kr('get_local', broker_req)
        self.patch_kr('set_state')
        self.is_request_complete.return_value = False
        self.cr.changed()
        # Side effect of asserting pools.available was not set.
        self.set_state.assert_called_once_with('{relation_name}.available')

    def test_broken(self):
        self.patch_kr('remove_state')
        self.cr.broken()
        self.remove_state.assert_has_calls([
            mock.call('{relation_name}.available'),
            mock.call('{relation_name}.connected'),
            mock.call('{relation_name}.pools.available')])

    @mock.patch.object(charmhelpers.contrib.storage.linux.ceph.uuid, 'uuid1')
    def test_create_pool_new_request(self, _uuid1):
        _uuid1.return_value = '9e34123e-fa0c-11e8-ad9c-fa163ed1cc55'
        req = (
            '{"api-version": 1, '
            '"ops": [{"op": "create-pool", "name": "bob", "replicas": 3, '
            '"pg_num": null, "weight": null, "group": null, '
            '"group-namespace": null, "app-name": null, "max-bytes": null, '
            '"max-objects": null}], '
            '"request-id": "9e34123e-fa0c-11e8-ad9c-fa163ed1cc55"}')
        self.patch_kr('get_local', None)
        self.patch_kr('set_local')
        self.cr.create_pool('bob')
        self.set_local.assert_called_once_with(key='broker_req', value=req)
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
        _uuid1.return_value = '9e34123e-fa0c-11e8-ad9c-fa163ed1cc55'
        req = (
            '{"api-version": 1, '
            '"ops": [{"op": "create-pool", "name": "bob", "replicas": 3, '
            '"pg_num": null, "weight": null, "group": null, '
            '"group-namespace": null}], '
            '"request-id": "9e34123e-fa0c-11e8-ad9c-fa163ed1cc55"}')
        self.patch_kr('get_local', req)
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
                'weight': None}])

    def test_request_access_to_group_new_request(self):
        self.patch_kr('get_local', '{"ops": []}')
        self.patch_kr('set_local')
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
        req = (
            '{"api-version": 1, '
            '"ops": [{"op": "create-pool", "name": "volumes", "replicas": 3, '
            '"pg_num": null, "weight": null, "group": null, '
            '"group-namespace": null}], '
            '"request-id": "9e34123e-fa0c-11e8-ad9c-fa163ed1cc55"}')
        self.patch_kr('get_local', req)
        self.cr.request_access_to_group(
            'volumes',
            key_name='cinder',
            object_prefix_permissions={'class-read': ['rbd_children']},
            permission='rwx')
        ceph_broker_rq = self.send_request_if_needed.mock_calls[0][1][0]
        self.assertEqual(
            ceph_broker_rq.ops,
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

    @mock.patch.object(requires.hookenv, 'related_units')
    @mock.patch.object(requires.hookenv, 'relation_get')
    def test_get_remote_all(self, relation_get, related_units):
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

        def get_unit_data(key, unit, relation_id):
            return unit_data[relation_id].get(unit, {}).get(key, {})
        conv1 = mock.MagicMock()
        conv1.relation_ids = ['rid:1', 'rid:2']
        conv2 = mock.MagicMock()
        conv2.relation_ids = ['rid:3']
        self.patch_kr('conversations', [conv1, conv2])
        related_units.side_effect = lambda x: unit_data[x].keys()
        relation_get.side_effect = get_unit_data
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
