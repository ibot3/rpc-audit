# RPC Audit

This packet contains methods and classes that allow to audit/log RPC calls.

## Usage
To use this packet, a fitting module (in the modules folder) must exist, where one or more `Builing Environments` 
(`CADFBuildingEnv`) instances are created and `builders` defined for them. 

Afterwards, the correct `CADFBuildingEnv` instance must be imported into the used RPC library.
The RPC library must be modified to call the `rpc_called` method whenever an RPC call is made and the `rpc_received`
method, when an RPC request has been received and processed.
If the RPC library supports hooks or something similar, they can be used instead of modifying the library.

The `context` attribute can contain additional information that can be used by the registered builders to generate
their output. 

As an example there is a module for the [oslo.messaging](https://docs.openstack.org/oslo.messaging/latest/) RPC library.

## Builders
Builders are methods that are responsible for building one CADF event attribute.
For every attribute, multiple builders can be registered.
The type of the builder specifies, if the returned data should replace the already present data, or should append
the new data by merging dictionaries.

The builders must return valid CADF values according to the standard.

## Event output
The events are currently stored at `/tmp/rpc_events.txt`.
Additionally, the [Audit API](https://publicgitlab.cloudandheat.com/cloud-kritis/audit-api) is used.

## Attribute filter
By default, all parameters of the RPC method are put into the event as attachment.
There can be supplied a filter dictionary (`BuilderEnv.filter_args`), where a mask dictionary, can be supplied for
every RPC method. As soon as the `filter_args` are set, only the specified parameters will be saved.

Example:

```
building_env.filter_args = {
    'reboot_instance': {
        'instance': {
            'uuid': True
        }
    }
}
```
