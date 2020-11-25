from hashlib import sha256

from oslo_serialization import jsonutils
from pycadf.attachment import Attachment
from pycadf.cadftaxonomy import UNKNOWN, OUTCOME_SUCCESS, ACCOUNT_USER
from pycadf.credential import Credential
from pycadf.event import EVENT_KEYNAME_ACTION, EVENT_KEYNAME_OUTCOME, EVENT_KEYNAME_INITIATOR, \
    EVENT_KEYNAME_ATTACHMENTS, EVENT_KEYNAME_TARGET
from pycadf.host import Host
from pycadf.resource import Resource


from .os_map import rpc_method_to_cadf_action
from ..base import CADFBuilderEnv, BuilderType, LOG

builder = CADFBuilderEnv()

# context:  {'target': ..., 'ctxt': ...}


@builder.builder(EVENT_KEYNAME_ACTION, BuilderType.REPLACE)
def build_action(context, method, args, role, result=None):
    """
    Tries to find an fitting action for the method.
    To achieve this, the topic of the MQ is used to lookup the action in a map.
    """

    topic = context['target'].topic

    LOG.debug("topic: %s", topic)

    submap = rpc_method_to_cadf_action.get(topic)

    LOG.debug("submap: %s", submap)

    if submap is not None:
        action = submap.get(method)

        if action is not None:
            return action

    return UNKNOWN


@builder.builder(EVENT_KEYNAME_OUTCOME, BuilderType.REPLACE)
def build_outcome(context, method, args, role, result=None):
    """
    Checks if the result is not None and then returns "success".
    If the result is None, "unknown" will be returned.
    """
    if result is None:
        return UNKNOWN
    else:
        if result:
            return OUTCOME_SUCCESS


@builder.builder(EVENT_KEYNAME_INITIATOR, BuilderType.REPLACE)
def build_initiator(context, method, args, role, result=None):
    """
    Builds the initiator from the available context information.
    :return:
    """

    id = context['ctxt'].user
    type_uri = ACCOUNT_USER
    name = context['ctxt'].user_name
    domain = context['ctxt'].user_domain
    credential = Credential(context['ctxt'].auth_token)
    host = Host(address=context['ctxt'].remote_address)

    return Resource(id, type_uri, name, domain=domain, credential=credential,
                    host=host)


@builder.builder(EVENT_KEYNAME_TARGET, BuilderType.REPLACE)
def build_target(context, method, args, role, result=None):
    """
    Builds the targets from the arguments.
    Returns a list of targets, if multiple targets are given.
    """

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


@builder.builder(EVENT_KEYNAME_ATTACHMENTS, BuilderType.APPEND)
def build_attachments(context, method, args, role, result=None):
    """
    Builds the following attachments:
    - Information about the project
    - A hash of the method and arguments
    - Information about the permissions of the initiator
    """

    args_json = jsonutils.to_primitive(args, convert_instances=True)

    attachments = [Attachment(name='project', typeURI="python/dict", content={
        'id': context['ctxt'].project_id,
        'name': context['ctxt'].project_name,
        'domain': context['ctxt'].project_domain
    }), Attachment(name='request_hash', typeURI="python/dict", content={
        'algorithm': 'SHA256',
        'hash': str(sha256('{}_{}'.format(method, args_json).encode('utf-8')).hexdigest())
    }), Attachment(name='permissions', typeURI="python/dict", content={
        'is_admin': context['ctxt'].is_admin,
        'is_admin_project': context['ctxt'].is_admin_project,
        'roles': context['ctxt'].roles
    })]

    return attachments
