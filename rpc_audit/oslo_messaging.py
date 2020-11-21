import json
from hashlib import sha256

from pycadf.attachment import Attachment
from pycadf.cadftaxonomy import UNKNOWN, OUTCOME_SUCCESS, ACCOUNT_USER
from pycadf.credential import Credential
from pycadf.endpoint import Endpoint
from pycadf.event import EVENT_KEYNAME_ACTION, EVENT_KEYNAME_OUTCOME, EVENT_KEYNAME_INITIATOR, \
    EVENT_KEYNAME_ATTACHMENTS, EVENT_KEYNAME_TARGET
from pycadf.host import Host
from pycadf.resource import Resource


from .os_map import rpc_method_to_cadf_action
from .base import CADFBuilderEnv, BuilderType

builder_empty = CADFBuilderEnv()

# context:  {'rpc_client': ..., 'ctxt': ...}


@builder_empty.builder(EVENT_KEYNAME_ACTION, BuilderType.REPLACE)
def build_action(context, method, args, result=None):
    topic = context['rpc_client'].target.topic

    submap = rpc_method_to_cadf_action.get(topic)

    if submap is not None:
        action = submap.get(method)

        if action is not None:
            return action

    return UNKNOWN


@builder_empty.builder(EVENT_KEYNAME_OUTCOME, BuilderType.REPLACE)
def build_outcome(context, method, args, result=None):
    if result is None:
        return UNKNOWN
    else:
        if result:
            return OUTCOME_SUCCESS


@builder_empty.builder(EVENT_KEYNAME_INITIATOR, BuilderType.REPLACE)
def build_initiator(context, method, args, result=None):
    id = context['ctxt'].user
    type_uri = ACCOUNT_USER
    name = context['ctxt'].user_name
    domain = context['ctxt'].user_domain
    credential = Credential(context['ctxt'].auth_token,
                            {'user_identity': context['ctxt'].user_identify,
                             'is_admin': context['ctxt'].is_admin,
                             'is_admin_project': context['ctxt'].is_admin_project,
                             'roles': context['ctxt'].roles})
    host = Host(address=context['ctxt'].remote_address)

    return Resource(id, type_uri, name, domain=domain, credential=credential,
                    host=host)


@builder_empty.builder(EVENT_KEYNAME_TARGET, BuilderType.REPLACE)
def build_target(context, method, args, result=None):
    targets = []

    if args.get('instance') is not None or args.get('instances') is not None:
        instances = []

        if args.get('instances') is not None:
            instances += args.get('instances')
        else:
            instances.append(args.get('instance'))

        for instance in instances:
            id = instance.uuid
            type_uri = 'compute/machine'
            name = instance.hostname
            domain = context['ctxt'].project_domain
            host = Host(address=instance.node)

            targets.append(Resource(id, type_uri, name, domain=domain, host=host))

    if len(targets) == 1:
        targets = targets[0]
    elif len(targets) == 0:
        targets = None

    return targets


@builder_empty.builder(EVENT_KEYNAME_ATTACHMENTS, BuilderType.REPLACE)
def build_attachments(context, method, args, result=None):
    attachments = [Attachment(name='project', content={
        'id': context['ctxt'].project,
        'name': context['ctxt'].project_name,
        'domain': context['ctxt'].project_domain
    }), Attachment(name='request_hash', content={
        'algorithm': 'SHA256',
        'hash': sha256('{}_{}'.format(method, json.dumps(args)).encode('utf-8')).hexdigest()
    })]

    return attachments


builder = builder_empty
