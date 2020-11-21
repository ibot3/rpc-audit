import json
import logging
from enum import Enum
from typing import Dict, List

from pycadf.attachment import ATTACHMENT_KEYNAME_TYPEURI
from pycadf.cadftype import EVENTTYPE_ACTIVITY
from pycadf.event import EVENT_KEYNAMES, Event, EVENT_KEYNAME_EVENTTYPE, EVENT_KEYNAME_TAGS
from pycadf.identifier import generate_uuid


LOG = logging.getLogger('rpc_audit')
fh = logging.FileHandler('/tmp/rpc-audit.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(funcName)s:%(lineno)d %(message)s')
fh.setFormatter(formatter)
LOG.addHandler(fh)
LOG.addHandler(logging.StreamHandler())

LOG.info("Running RPC Audit")


class BuilderType(Enum):
    REPLACE = 1
    APPEND = 2


class Builder:
    builder_type: BuilderType = None
    func = None

    def __init__(self, builder_type: BuilderType, func):
        self.builder_type = builder_type
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


def merge_dict(source, destination):
    for key, value in source.items():
        if isinstance(value, dict):
            # get node or create one
            node = destination.setdefault(key, {})
            merge_dict(value, node)
        else:
            destination[key] = value
    return destination


def build_event_from_data(event_data):
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
            # Automatically convert to json if no schema is defined
            if attachment.typeURI is None:
                attachment.typeURI = "https://json-schema.org/draft/2019-09/schema"
                attachment.content = json.dumps(attachment.content)

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
        return False


class CADFBuilderEnv:
    builder_map: Dict[str, List[Builder]] = {}

    def __init__(self):
        LOG.debug("BuilderEnv Init")

        def build_event_type(*args, **kwargs):
            return EVENTTYPE_ACTIVITY

        def build_tags(*args, **kwargs):
            return ['rpc']

        self.register_builder(EVENT_KEYNAME_EVENTTYPE, BuilderType.REPLACE, build_event_type)
        self.register_builder(EVENT_KEYNAME_TAGS, BuilderType.REPLACE, build_tags)

    def register_builder(self, attr: str, builder_type: BuilderType, func):
        LOG.debug("Registered builder: %s", attr)

        if attr not in EVENT_KEYNAMES:
            raise ValueError("Unknown CADF attribute")

        if attr not in self.builder_map:
            self.builder_map[attr] = []

        self.builder_map[attr].append(Builder(builder_type, func))

    def builder(self, attr: str, builder_type: BuilderType):
        def decorator(f):
            self.register_builder(attr, builder_type, f)

        return decorator

    def build_events(self, context, method, args, result=None):
        LOG.debug("Building events, map: %s", self.builder_map)
        LOG.debug("Building events, method: %s %s", method, args)
        LOG.debug("Building events, result: %s", result)

        for key, value in context.items():
            if callable(getattr(value, "as_dict", None)):
                value = value.as_dict()

            LOG.debug("Building events, context[%s]: %s", key, value)

        events = []
        event_data = {}

        for attr, builders in self.builder_map.items():
            for builder in builders:
                data = builder(context, method, args, result)

                debug_data = data.as_dict() if getattr(data, "as_dict", None) else data
                LOG.debug("Executed builder %s, mode: %s, result: %s", attr, builder.builder_type, debug_data)

                if attr not in event_data or builder.builder_type == BuilderType.replace:
                    event_data[attr] = data
                elif builder.builder_type == BuilderType.append:
                    if attr not in event_data or type(event_data[attr]) != dict:
                        event_data[attr] = data
                    else:
                        event_data[attr] = merge_dict(data, event_data[attr])

        LOG.debug("Event data: %s", event_data)

        if type(event_data['target']) == list:
            targets = iter(event_data['target'])

            first_target = next(targets)
            event_data['target'] = first_target
            events.append(build_event_from_data(event_data))

            if event_data.get('tags') is None:
                event_data['tags'] = []

            event_data['tags'].append(event_data['id'])

            for target in targets:
                new_data = event_data

                new_data['id'] = generate_uuid()
                new_data['target'] = target

                events.append(build_event_from_data(new_data))
        else:
            events.append(build_event_from_data(event_data))

        return events

    def build_and_save_events(self, context, method, args, result=None):
        try:
            events = self.build_events(context, method, args, result)

            for event in events:
                if not event:
                    LOG.warning("Discarded one invalid RPC-Audit event!")
                else:
                    LOG.debug("Saving event %s", event.id)

                    with open("/tmp/rpc_events.txt", "a") as event_file:
                        event_file.write(event.as_dict())
                        event_file.write('\n')
        except Exception as e:
            LOG.error(e, exc_info=True)

    def rpc_received(self, context, method, args):
        self.build_and_save_events(context, method, args)

    def rpc_called(self, context, method, args, result=None):
        LOG.debug("RPC Call")

        self.build_and_save_events(context, method, args, result=result)
