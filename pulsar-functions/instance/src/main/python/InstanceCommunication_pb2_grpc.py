# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import InstanceCommunication_pb2 as InstanceCommunication__pb2
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


class InstanceControlStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.GetFunctionStatus = channel.unary_unary(
        '/proto.InstanceControl/GetFunctionStatus',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=InstanceCommunication__pb2.FunctionStatus.FromString,
        )
    self.GetAndResetMetrics = channel.unary_unary(
        '/proto.InstanceControl/GetAndResetMetrics',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=InstanceCommunication__pb2.MetricsData.FromString,
        )
    self.ResetMetrics = channel.unary_unary(
        '/proto.InstanceControl/ResetMetrics',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )
    self.GetMetrics = channel.unary_unary(
        '/proto.InstanceControl/GetMetrics',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=InstanceCommunication__pb2.MetricsData.FromString,
        )
    self.HealthCheck = channel.unary_unary(
        '/proto.InstanceControl/HealthCheck',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=InstanceCommunication__pb2.HealthCheckResult.FromString,
        )


class InstanceControlServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def GetFunctionStatus(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetAndResetMetrics(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ResetMetrics(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetMetrics(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def HealthCheck(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_InstanceControlServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'GetFunctionStatus': grpc.unary_unary_rpc_method_handler(
          servicer.GetFunctionStatus,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=InstanceCommunication__pb2.FunctionStatus.SerializeToString,
      ),
      'GetAndResetMetrics': grpc.unary_unary_rpc_method_handler(
          servicer.GetAndResetMetrics,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=InstanceCommunication__pb2.MetricsData.SerializeToString,
      ),
      'ResetMetrics': grpc.unary_unary_rpc_method_handler(
          servicer.ResetMetrics,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
      'GetMetrics': grpc.unary_unary_rpc_method_handler(
          servicer.GetMetrics,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=InstanceCommunication__pb2.MetricsData.SerializeToString,
      ),
      'HealthCheck': grpc.unary_unary_rpc_method_handler(
          servicer.HealthCheck,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=InstanceCommunication__pb2.HealthCheckResult.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'proto.InstanceControl', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
