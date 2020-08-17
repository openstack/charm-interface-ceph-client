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
from unittest import mock


with mock.patch('charmhelpers.core.hookenv.metadata') as _meta:
    _meta.return_Value = 'ss'
    from ceph_client import provides

_hook_args = {}

TO_PATCH = []
TO_PATCH_BASE_PROVIDES = [
    'relation_set',
]


def mock_hook(*args, **kwargs):

    def inner(f):
        # remember what we were passed.  Note that we can't actually determine
        # the class we're attached to, as the decorator only gets the function.
        _hook_args[f.__name__] = dict(args=args, kwargs=kwargs)
        return f
    return inner


class TestCephClientProvider(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._patched_hook = mock.patch('charms.reactive.when', mock_hook)
        cls._patched_hook_started = cls._patched_hook.start()
        # force requires to rerun the mock_hook decorator:
        # try except is Python2/Python3 compatibility as Python3 has moved
        # reload to importlib.
        try:
            reload(provides)
        except NameError:
            import importlib
            importlib.reload(provides)

    @classmethod
    def tearDownClass(cls):
        cls._patched_hook.stop()
        cls._patched_hook_started = None
        cls._patched_hook = None
        # and fix any breakage we did to the module
        try:
            reload(provides)
        except NameError:
            import importlib
            importlib.reload(provides)

    def patch(self, method, obj=None):
        target_obj = obj or self.obj
        _m = mock.patch.object(target_obj, method)
        _mock = _m.start()
        self.addCleanup(_m.stop)
        return _mock

    def setUp(self):
        self.cr = provides.CephClientProvider('some-relation', [])
        self._patches = {}
        self._patches_start = {}
        self.obj = provides
        for method in TO_PATCH:
            setattr(self, method, self.patch(method))
        for method in TO_PATCH_BASE_PROVIDES:
            setattr(self, method, self.patch(method, provides.base_provides))

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

    def test_changed(self):
        conv1 = mock.MagicMock()
        conv1.get_remote.return_value = True
        self.patch_kr('conversation', conv1)
        self.patch_kr('set_state')
        self.cr.changed()
        self.set_state.assert_has_calls([
            mock.call('{relation_name}.connected'),
            mock.call('{relation_name}.broker_requested')])

    def test_changed_no_request(self):
        conv1 = mock.MagicMock()
        conv1.get_remote.return_value = None
        self.patch_kr('conversation', conv1)
        self.patch_kr('set_state')
        self.cr.changed()
        self.set_state.assert_called_once_with('{relation_name}.connected')

    def test_provide_auth(self):
        conv1 = mock.MagicMock()
        conv1.get_remote.return_value = None
        conv1.namespace = 'ns1'
        self.patch_kr('conversation', conv1)
        self.cr.provide_auth(
            'svc1',
            'secret',
            'ssl1.0',
            '10.0.0.10')
        expect = {
            'auth': 'ssl1.0',
            'ceph-public-address': '10.0.0.10'}
        conv1.set_remote.assert_called_once_with(**expect)
        self.relation_set.assert_called_once_with(
            relation_id='ns1',
            relation_settings={'key': 'secret'})

    def test_requested_keys(self):
        conv1 = mock.MagicMock()
        conv1.scope = 'svc1'
        conv2 = mock.MagicMock()
        conv2.scope = 'svc2'
        self.patch_kr('conversations', [conv1, conv2])
        self.patch_kr('requested_key')
        keys = {
            'svc1': 'key'}
        self.requested_key.side_effect = lambda x: keys.get(x)
        self.assertEqual(
            list(self.cr.requested_keys()),
            ['svc2'])

    def test_requested_key(self):
        conv1 = mock.MagicMock()
        self.patch_kr('conversation', conv1)
        self.cr.requested_key('svc1')
        self.conversation.assert_called_once_with(scope='svc1')
        self.conversation(scope='svc1').get_remote.assert_called_once_with(
            'key')

    def test_provide_broker_token(self):
        conv1 = mock.MagicMock()
        self.patch_kr('conversation', conv1)
        self.cr.provide_broker_token('svc1', 'urkey', 'token1')
        conv1.set_remote.assert_called_once_with(
            broker_rsp='token1',
            urkey='token1')

    def test_requested_tokens(self):
        conv1 = mock.MagicMock()
        conv1.scope = 'svc1'
        conv2 = mock.MagicMock()
        conv2.scope = 'svc2'
        self.patch_kr('conversations', [conv1, conv2])
        tokens = {
            'svc1': 'token1',
            'svc2': 'token2'}
        self.patch_kr('requested_token')
        self.requested_token.side_effect = lambda x: tokens.get(x)
        self.assertEqual(
            list(self.cr.requested_tokens()),
            [('svc1', 'token1'), ('svc2', 'token2')])

    def test_requested_token(self):
        conv1 = mock.MagicMock()
        self.patch_kr('conversation', conv1)
        self.cr.requested_token('svc1')
        self.conversation.assert_called_once_with(scope='svc1')
        self.conversation(scope='svc1').get_remote.assert_called_once_with(
            'broker_req')
