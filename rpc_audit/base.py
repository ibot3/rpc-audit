import json
import logging
from _thread import start_new_thread
from enum import Enum
from hashlib import sha256
from typing import Dict, List, Optional, Any

from oslo_messaging.notify._impl_https import HttpsDriver
from pycadf.attachment import Attachment
from pycadf.cadftype import EVENTTYPE_ACTIVITY
from pycadf.event import EVENT_KEYNAMES, Event, EVENT_KEYNAME_EVENTTYPE, EVENT_KEYNAME_TAGS, EVENT_KEYNAME_ATTACHMENTS
from pycadf.identifier import generate_uuid

# Create logger
LOG = logging.getLogger('rpc_audit')
fh = logging.FileHandler('/tmp/rpc-audit.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(funcName)s:%(lineno)d %(message)s')
fh.setFormatter(formatter)
LOG.addHandler(fh)
LOG.addHandler(logging.StreamHandler())

LOG.info("Running RPC Audit")


class ObserverRole(Enum):
    SENDER = 1
    RECEIVER = 2


class BuilderType(Enum):
    # Replace all existing data with the data returned by the builder.
    REPLACE = 1

    # Merge the exiting data with the data returned by the builder.
    APPEND = 2


class Builder:
    """
    A Builder object is responsible for returning the data for one attribute.
    """

    # Specifies how the returned data is treated, if multiple builders are registered for one attribute.
    builder_type: BuilderType = None
    func = None

    def __init__(self, builder_type: BuilderType, func):
        self.builder_type = builder_type
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


def merge(source, destination):
    """
    Merges two dicts or lists. "source" has priority, if not mergable
    """

    if type(source) == list and (destination is None or type(destination) == list):
        return source + (destination or [])
    elif type(source) == dict:
        for key, value in source.items():
            if isinstance(value, dict):
                node = destination.setdefault(key, {})
                merge(value, node)
            elif isinstance(value, list) and isinstance(destination.get(key, []), list):
                destination[key] = value + destination.get(key, [])
            else:
                destination[key] = value

    return destination


def prune_dict(dct, mask):
    """
    Removes all keys from a dict that are not contained in the mask

    Source: https://stackoverflow.com/q/46311253/6644547
    """

    result = {}
    for k, v in mask.items():
        if isinstance(v, dict):
            value = prune_dict(dct[k], v)
            if value: # check that dict is non-empty
                result[k] = value
        elif v:
            result[k] = dct[k]
    return result


def build_event_from_data(event_data: dict) -> Optional[Event]:
    """
    Builds a CADF Event Object.

    Some list attributes (attachments, tags, measurements, reportersteps) cannot be set in the Event constructor.
    Therefore, these attributes must be extracted and given into the according methods.

    :param event_data: Dictionary with all required attributes.
    :return: Generated event
    """

    event_data_raw = event_data

    try:
        # Extract some attributes, because the have extra methods and cannot be used in the Event constructor
        attachments = event_data.get('attachments', [])
        tags = event_data.get('tags', [])
        measurements = event_data.get('measurements', [])
        reportersteps = event_data.get('reportersteps', [])

        event_data.pop('attachments', None)
        event_data.pop('tags', None)
        event_data.pop('measurements', None)
        event_data.pop('reportersteps', None)

        # Build event
        event = Event(**event_data)

        # Add extracted attributes
        for attachment in attachments:
            LOG.debug("ATTACHMENT: %s", attachment.as_dict())
            event.add_attachment(attachment)

        for tag in tags:
            event.add_tag(tag)

        for measurement in measurements:
            event.add_measurement(measurement)

        for reporterstep in reportersteps:
            event.add_reporterstep(reporterstep)

        return event
    except ValueError as e:
        LOG.error(f"Could not create event: {e} | Data: %s", event_data_raw, exc_info=True)
        return None


def send_to_audit_api(event: Event, role: ObserverRole):
    """
    Send an event to the audit API via http.
    """

    project_id = None

    for att in event.attachments:
        if att.name == 'project':
            project_id = att.content.get('id')

    data = {
        'message_id': event.id,
        'publisher_id': 'rpc_mw',
        'event_type': 'audit.rpc.{}'.format('call' if role == ObserverRole.SENDER else 'receive'),
        'priority': 'INFO',
        'payload': event.as_dict(),
        'project_id': project_id,
    }

    try:
        api_client = HttpsDriver(None, None, None)
        api_client.notify(None, data, "None", 1)
    except Exception as e:
        LOG.error("Failed sending event to API:  %s", e, exc_info=True)


class CADFBuildingEnv:
    """
    Builder Environment.

    This is the central class for this application. Modules will create an instance of it and register builders
    that are used to generate the application specific CADF attributes.
    """

    # This map contains all registered builders.
    builder_map: Dict[str, List[Builder]] = {}

    # This map filters, which RPC method parameters should be added to the event
    filter_args: Optional[Dict[str, Dict]] = None

    def __init__(self):
        LOG.debug("BuilderEnv Init")

        def build_event_type(*args, **kwargs):
            """
            Default builder to set the event type. Always returns "activity".
            """
            return EVENTTYPE_ACTIVITY

        def build_tags(*args, **kwargs):
            """
            Default builder for tags. Always adds the "rpc" tag.
            """
            return ['rpc']

        def build_attachments(context, method, args, role, result=None):
            """
            Default builder for attachments. Add the following attachments:
            - The called RPC method and parameters.
            - The result after the method has been executed.
            """

            args_raw = context.get("args_raw")
            hash_args = args if args_raw is None else args_raw
            args_filtered = dict(args)

            hash_args_json = json.dumps(hash_args)

            if self.filter_args is not None:
                mask = self.filter_args.get(method, {})
                args_filtered = prune_dict(args, mask)

            attachments = [Attachment(typeURI="python/dict",
                                      content={'method': method, 'role': role.name, 'args': args_filtered},
                                      name="rpc_method"),
                           Attachment(name='request_hash', typeURI="python/dict", content={
                               'algorithm': 'SHA256',
                               'hash': str(sha256('{}_{}'.format(method, hash_args_json).encode('utf-8')).hexdigest())
                           })]

            if result:
                attachments.append(Attachment(typeURI="any",
                                              content=result,
                                              name="result"))

            return attachments

        # Register the above defined builders
        self.register_builder(EVENT_KEYNAME_EVENTTYPE, BuilderType.REPLACE, build_event_type)
        self.register_builder(EVENT_KEYNAME_TAGS, BuilderType.REPLACE, build_tags)
        self.register_builder(EVENT_KEYNAME_ATTACHMENTS, BuilderType.APPEND, build_attachments)

    def register_builder(self, attr: str, builder_type: BuilderType, func):
        """
        Registeres a given builder for an attribute.

        :param attr: The attribute that the builder returns.
        :param builder_type: The type of the builder.
        :param func: The function that should be executed.
        """
        LOG.debug("Registered builder: %s", attr)

        if attr not in EVENT_KEYNAMES:
            raise ValueError("Unknown CADF attribute")

        if attr not in self.builder_map:
            self.builder_map[attr] = []

        self.builder_map[attr].append(Builder(builder_type, func))

    def builder(self, attr: str, builder_type: BuilderType):
        """
        Decorator for the `register_builder` method.
        """

        def decorator(f):
            self.register_builder(attr, builder_type, f)

        return decorator

    def build_events(self, context: Any, method: str, args: Optional[Dict[str, Any]], role: ObserverRole,
                     result: Any = None) -> List[Event]:
        """
        Executes all builders and aggregates the data into Event objects.

        Usually, one Event is created. However, if multiple targets are given, multiple events will be created.
        For allowing to group the events after their creation, a tag containing the UUID of the first event is added.

        :param context: Environment specific metadata.
        :param method: The name of the called method
        :param args: The parameters for the called method
        :param role: The role of the observing service (client/server)
        :param result: The returned result after executing the method
        :return:
        """

        LOG.debug("Building events, map: %s", self.builder_map)
        LOG.debug("Building events, method: %s %s", method, args)
        LOG.debug("Building events, result: %s", result)

        for key, value in context.items():
            if callable(getattr(value, "as_dict", None)):
                value = value.as_dict()

            LOG.debug("Building events, context[%s]: %s", key, value)

        events = []
        event_data = {}

        # Iterate above all registered attributes.
        for attr, builders in self.builder_map.items():
            # Iterate above all builders for that attribute.
            for builder in builders:
                # Execute the builder
                data = builder(context, method, args, role, result)

                debug_data = data.as_dict() if getattr(data, "as_dict", None) else data
                LOG.debug("Executed builder %s, mode: %s, result: %s", attr, builder.builder_type, debug_data)

                # Replace the content if no content exists yet, or the BuilderType is "REPLACE"
                if attr not in event_data or builder.builder_type == BuilderType.REPLACE:
                    event_data[attr] = data
                elif builder.builder_type == BuilderType.APPEND:
                    if type(event_data[attr]) not in (dict, list):
                        # Replace the content, if the existing data is not of type dict or list
                        event_data[attr] = data
                    else:
                        # Merge the content, new data has priority
                        event_data[attr] = merge(data, event_data[attr])

        LOG.debug("Event data: %s", event_data)

        if type(event_data['target']) == list:
            # Create multiple events if multiple targets exist
            targets = iter(event_data['target'])

            first_target = next(targets)
            event_data['target'] = first_target
            events.append(build_event_from_data(event_data))

            if event_data.get('tags') is None:
                event_data['tags'] = []

            # Add tag to allow grouping of all generated events
            event_data['tags'].append(event_data['id'])

            for target in targets:
                new_data = event_data

                new_data['id'] = generate_uuid()
                new_data['target'] = target

                events.append(build_event_from_data(new_data))
        else:
            # Just build one event
            events.append(build_event_from_data(event_data))

        return events

    def build_and_save_events(self, context, method, args, role: ObserverRole, result=None):
        """
        Generates events and saves them to a persistent storage afterwards.

        Will catch all errors and log them.
        """
        try:
            events = self.build_events(context, method, args, role, result)

            for event in events:
                if event is None:
                    LOG.warning("Discarded one invalid RPC-Audit event!")
                else:
                    LOG.debug("Saving event %s", event.id)

                    send_to_audit_api(event, role)

                    with open("/tmp/rpc_events.txt", "a") as event_file:
                        event_file.write(json.dumps(event.as_dict()))
                        event_file.write('\n')
        except Exception as e:
            LOG.error(e, exc_info=True)

        return

    def process_async(self, context, method: str, args: Optional[Dict], role: ObserverRole, result=None, ):
        """
        Starts the event generation in a new thread.
        """

        start_new_thread(self.build_and_save_events, (context, method, args, role, result))

    def rpc_received(self, context, method: str, args: Optional[Dict], result=None):
        """
        Should be called when an rpc call has been received.
        """

        self.process_async(context, method, args, ObserverRole.RECEIVER, result)

    def rpc_called(self, context, method: str, args: Optional[Dict], result=None):
        """
        Should be called when an rpc call has been sent.
        """

        self.process_async(context, method, args, ObserverRole.SENDER, result)
