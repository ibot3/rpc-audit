import time
import unittest
from unittest import TestCase

from rpc_audit.modules.oslo_messaging import builder


class Struct:
    def __init__(self, entries: dict):
        self.__dict__.update(entries)


class TestOsloMessaging(TestCase):
    ctxt = Struct({
        'user': '30992343-4236-4607-93e3-2f24fbba85ff',
        'user_name': 'test-user',
        'user_domain': 'test-user-domain',
        'auth_token': '73adeeaf0c6a4ec9264e19aae44014f2244ff416ed3de915d576f597fe313db5',
        'remote_address': '10.11.12.13',
        'project_domain': 'test-project-domain',
        'project_id': '8b6e9330-16b4-4ee4-8154-e00b6ba51442',
        'project_name': 'test-project',
        'is_admin': True,
        'is_admin_project': True,
        'roles': [
            'role1',
            'role2',
            'role3'
        ],
        'request_id': '03a45f869c02d955453c4e1afb8f1b49',
    })

    params = {
        'instance': {
            'uuid': 'f120c8b6-9d37-476c-a80d-22b33478b079',
            'hostname': 'hostname.test',
            'node': 'test-host',
        }
    }

    target = Struct({
        'topic': 'compute'
    })

    context = {
        'ctxt': ctxt,
        'target': target
    }

    event = None

    def __init__(self, *args, **kwargs):
        self.b_env = builder
        self.b_env.callback = self.generation_callback

        super().__init__(*args, **kwargs)

    def setUp(self) -> None:
        self.event = None
        
        super(TestOsloMessaging, self).setUp()

    def generation_callback(self, event: dict):
        self.event = event
        print(event)

    def test_hard_reboot(self):
        self.b_env.rpc_called(
            context=self.context,
            method='reboot_instance',
            args=self.params,
        )

        time.sleep(1)

        self.assertIsNotNone(self.event)

        del self.event['eventTime']
        del self.event['id']

        self.assertEqual(self.event, reboot_result)


reboot_result = {
    "action": "start",
    "attachments": [
        {
            "content": {
                "domain": "test-project-domain",
                "id": "8b6e9330-16b4-4ee4-8154-e00b6ba51442",
                "name": "test-project",
            },
            "name": "project",
            "typeURI": "python/dict",
        },
        {
            "content": {
                "is_admin": True,
                "is_admin_project": True,
                "roles": ["role1", "role2", "role3"],
            },
            "name": "permissions",
            "typeURI": "python/dict",
        },
        {
            "content": "03a45f869c02d955453c4e1afb8f1b49",
            "name": "request_id",
            "typeURI": "python/str",
        },
        {
            "content": {
                "args": {"instance": {"uuid": "f120c8b6-9d37-476c-a80d-22b33478b079"}},
                "method": "reboot_instance",
                "role": "SENDER",
            },
            "name": "rpc_method",
            "typeURI": "python/dict",
        },
        {
            "content": {
                "algorithm": "SHA256",
                "hash": "3f0d7d89b018ec48b26e93ee6ee06755a6d85a5cac038f70c0f24f4e23de7eda",
            },
            "name": "request_hash",
            "typeURI": "python/dict",
        },
    ],
    # "eventTime": "2020-12-16T19:21:20.276660 0000",
    "eventType": "activity",
    # "id": "237ccf60-c796-55fe-a8b3-177ff88e5d34",
    "initiator": {
        "credential": {"token": "73adeeaf xxxxxxxx fe313db5"},
        "domain": "test-user-domain",
        "host": {"address": "10.11.12.13"},
        "id": "30992343-4236-4607-93e3-2f24fbba85ff",
        "name": "test-user",
        "typeURI": "service/security/account/user",
    },
    "observer": {"id": "topic/compute", "typeURI": "service"},
    "outcome": "unknown",
    "tags": ["oslo.messaging", "rpc"],
    "target": {
        "domain": "test-project-domain",
        "host": {"address": "test-host"},
        "id": "f120c8b6-9d37-476c-a80d-22b33478b079",
        "name": "hostname.test",
        "typeURI": "compute/machine",
    },
    "typeURI": "http://schemas.dmtf.org/cloud/audit/1.0/event",
}


if __name__ == '__main__':
    unittest.main()
