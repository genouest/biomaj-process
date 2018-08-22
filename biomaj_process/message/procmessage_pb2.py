# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: procmessage.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='procmessage.proto',
  package='biomaj',
  serialized_pb=_b('\n\x11procmessage.proto\x12\x06\x62iomaj\"\xec\x02\n\x07Process\x12\x0c\n\x04\x62\x61nk\x18\x01 \x02(\t\x12\x0f\n\x07session\x18\x02 \x02(\t\x12\x0f\n\x07log_dir\x18\x03 \x02(\t\x12\x0b\n\x03\x65xe\x18\x04 \x02(\t\x12\x0c\n\x04\x61rgs\x18\x05 \x03(\t\x12(\n\x08\x65nv_vars\x18\x06 \x03(\x0b\x32\x16.biomaj.Process.EnvVar\x12\x1b\n\x0cshell_expand\x18\x07 \x01(\x08:\x05\x66\x61lse\x12\x0c\n\x04name\x18\x08 \x01(\t\x12\x13\n\x0b\x64\x65scription\x18\t \x01(\t\x12\x11\n\tproc_type\x18\n \x01(\t\x12\x18\n\tis_docker\x18\x0b \x01(\x08:\x05\x66\x61lse\x12&\n\x06\x64ocker\x18\x0c \x01(\x0b\x32\x16.biomaj.Process.Docker\x1a%\n\x06\x45nvVar\x12\x0c\n\x04name\x18\x01 \x02(\t\x12\r\n\x05value\x18\x02 \x02(\t\x1a\x30\n\x06\x44ocker\x12\r\n\x05image\x18\x01 \x02(\t\x12\x17\n\x08use_sudo\x18\x02 \x01(\x08:\x05\x66\x61lse\"\xcf\x01\n\tOperation\x12 \n\x07process\x18\x01 \x01(\x0b\x32\x0f.biomaj.Process\x12\x32\n\x04type\x18\x02 \x02(\x0e\x32\x1b.biomaj.Operation.OPERATION:\x07\x45XECUTE\x12&\n\x05trace\x18\x03 \x01(\x0b\x32\x17.biomaj.Operation.Trace\x1a*\n\x05Trace\x12\x10\n\x08trace_id\x18\x01 \x02(\t\x12\x0f\n\x07span_id\x18\x02 \x02(\t\"\x18\n\tOPERATION\x12\x0b\n\x07\x45XECUTE\x10\x01')
)
_sym_db.RegisterFileDescriptor(DESCRIPTOR)



_OPERATION_OPERATION = _descriptor.EnumDescriptor(
  name='OPERATION',
  full_name='biomaj.Operation.OPERATION',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='EXECUTE', index=0, number=1,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=580,
  serialized_end=604,
)
_sym_db.RegisterEnumDescriptor(_OPERATION_OPERATION)


_PROCESS_ENVVAR = _descriptor.Descriptor(
  name='EnvVar',
  full_name='biomaj.Process.EnvVar',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='biomaj.Process.EnvVar.name', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='value', full_name='biomaj.Process.EnvVar.value', index=1,
      number=2, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=307,
  serialized_end=344,
)

_PROCESS_DOCKER = _descriptor.Descriptor(
  name='Docker',
  full_name='biomaj.Process.Docker',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='image', full_name='biomaj.Process.Docker.image', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='use_sudo', full_name='biomaj.Process.Docker.use_sudo', index=1,
      number=2, type=8, cpp_type=7, label=1,
      has_default_value=True, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=346,
  serialized_end=394,
)

_PROCESS = _descriptor.Descriptor(
  name='Process',
  full_name='biomaj.Process',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='bank', full_name='biomaj.Process.bank', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='session', full_name='biomaj.Process.session', index=1,
      number=2, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='log_dir', full_name='biomaj.Process.log_dir', index=2,
      number=3, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='exe', full_name='biomaj.Process.exe', index=3,
      number=4, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='args', full_name='biomaj.Process.args', index=4,
      number=5, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='env_vars', full_name='biomaj.Process.env_vars', index=5,
      number=6, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='shell_expand', full_name='biomaj.Process.shell_expand', index=6,
      number=7, type=8, cpp_type=7, label=1,
      has_default_value=True, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='name', full_name='biomaj.Process.name', index=7,
      number=8, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='description', full_name='biomaj.Process.description', index=8,
      number=9, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='proc_type', full_name='biomaj.Process.proc_type', index=9,
      number=10, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='is_docker', full_name='biomaj.Process.is_docker', index=10,
      number=11, type=8, cpp_type=7, label=1,
      has_default_value=True, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='docker', full_name='biomaj.Process.docker', index=11,
      number=12, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_PROCESS_ENVVAR, _PROCESS_DOCKER, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=30,
  serialized_end=394,
)


_OPERATION_TRACE = _descriptor.Descriptor(
  name='Trace',
  full_name='biomaj.Operation.Trace',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='trace_id', full_name='biomaj.Operation.Trace.trace_id', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='span_id', full_name='biomaj.Operation.Trace.span_id', index=1,
      number=2, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=536,
  serialized_end=578,
)

_OPERATION = _descriptor.Descriptor(
  name='Operation',
  full_name='biomaj.Operation',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='process', full_name='biomaj.Operation.process', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='type', full_name='biomaj.Operation.type', index=1,
      number=2, type=14, cpp_type=8, label=2,
      has_default_value=True, default_value=1,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='trace', full_name='biomaj.Operation.trace', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_OPERATION_TRACE, ],
  enum_types=[
    _OPERATION_OPERATION,
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=397,
  serialized_end=604,
)

_PROCESS_ENVVAR.containing_type = _PROCESS
_PROCESS_DOCKER.containing_type = _PROCESS
_PROCESS.fields_by_name['env_vars'].message_type = _PROCESS_ENVVAR
_PROCESS.fields_by_name['docker'].message_type = _PROCESS_DOCKER
_OPERATION_TRACE.containing_type = _OPERATION
_OPERATION.fields_by_name['process'].message_type = _PROCESS
_OPERATION.fields_by_name['type'].enum_type = _OPERATION_OPERATION
_OPERATION.fields_by_name['trace'].message_type = _OPERATION_TRACE
_OPERATION_OPERATION.containing_type = _OPERATION
DESCRIPTOR.message_types_by_name['Process'] = _PROCESS
DESCRIPTOR.message_types_by_name['Operation'] = _OPERATION

Process = _reflection.GeneratedProtocolMessageType('Process', (_message.Message,), dict(

  EnvVar = _reflection.GeneratedProtocolMessageType('EnvVar', (_message.Message,), dict(
    DESCRIPTOR = _PROCESS_ENVVAR,
    __module__ = 'procmessage_pb2'
    # @@protoc_insertion_point(class_scope:biomaj.Process.EnvVar)
    ))
  ,

  Docker = _reflection.GeneratedProtocolMessageType('Docker', (_message.Message,), dict(
    DESCRIPTOR = _PROCESS_DOCKER,
    __module__ = 'procmessage_pb2'
    # @@protoc_insertion_point(class_scope:biomaj.Process.Docker)
    ))
  ,
  DESCRIPTOR = _PROCESS,
  __module__ = 'procmessage_pb2'
  # @@protoc_insertion_point(class_scope:biomaj.Process)
  ))
_sym_db.RegisterMessage(Process)
_sym_db.RegisterMessage(Process.EnvVar)
_sym_db.RegisterMessage(Process.Docker)

Operation = _reflection.GeneratedProtocolMessageType('Operation', (_message.Message,), dict(

  Trace = _reflection.GeneratedProtocolMessageType('Trace', (_message.Message,), dict(
    DESCRIPTOR = _OPERATION_TRACE,
    __module__ = 'procmessage_pb2'
    # @@protoc_insertion_point(class_scope:biomaj.Operation.Trace)
    ))
  ,
  DESCRIPTOR = _OPERATION,
  __module__ = 'procmessage_pb2'
  # @@protoc_insertion_point(class_scope:biomaj.Operation)
  ))
_sym_db.RegisterMessage(Operation)
_sym_db.RegisterMessage(Operation.Trace)


# @@protoc_insertion_point(module_scope)