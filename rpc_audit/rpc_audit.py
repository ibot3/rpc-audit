import logging
from enum import Enum
from typing import Dict

from pycadf.attachment import Attachment
from pycadf.cadftype import EVENTTYPE_ACTIVITY
from pycadf.event import EVENT_KEYNAMES, Event, EVENT_KEYNAME_EVENTTYPE, EVENT_KEYNAME_ID, EVENT_KEYNAME_TAGS
from pycadf.identifier import generate_uuid

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s', handlers=[
    logging.FileHandler("/tmp/rpc-audit.log"),
    logging.StreamHandler()
])

LOG = logging.getLogger(__name__)


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
    try:
        event = Event(**event_data)
        return event
    except ValueError as e:
        LOG.critical(f"Could not create event: {e} | Data: %s", event_data)
        return False


class CADFBuilderEnv:
    builder_map: Dict[str] = {}

    def __init__(self):
        self.register_builder(EVENT_KEYNAME_EVENTTYPE,
                              BuilderType.REPLACE,
                              lambda: EVENTTYPE_ACTIVITY)

        self.register_builder(EVENT_KEYNAME_TAGS,
                              BuilderType.REPLACE,
                              lambda: ['rpc'])

    def register_builder(self, attr: str, builder_type: BuilderType, func):
        if attr not in EVENT_KEYNAMES:
            raise ValueError("Unknown CADF attribute")

        if attr not in self.builder_map:
            self.builder_map[attr] = []

        self.builder_map[attr].append(Builder(builder_type, func))

    def builder(self, attr: str, builder_type: BuilderType):
        def wrap(f):
            def wrapped_f(*args):
                self.register_builder(attr, builder_type, f)
            return wrapped_f
        return wrap

    def build_events(self, context, method, args, result=None):
        events = []
        event_data = {}

        for attr, builders in self.builder_map:
            for builder in builders:
                data = builder(context, method, args, result)

                if attr.name not in event_data or builder.builder_type == BuilderType.replace:
                    event_data[attr.name] = data
                elif builder.builder_type == BuilderType.append:
                    if attr.name not in event_data or type(event_data[attr.name]) != dict:
                        event_data[attr.name] = data
                    else:
                        event_data[attr.name] = merge_dict(data, event_data[attr.name])

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
        events = self.build_events(context, method, args, result)

        for event in events:
            if not event:
                LOG.warning("Discarded one invalid RPC-Audit event!")
            else:
                with open("/tmp/rpc_events.txt", "a") as event_file:
                    event_file.write(event.as_dict())
                    event_file.write('\n')

    def rpc_received(self, context, method, args):
        self.build_and_save_events(context, method, args)

    def rpc_called(self, context, method, args, result):
        self.build_and_save_events(context, method, args, result=result)