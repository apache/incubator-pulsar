// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.pulsar.io.kinesis.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Message extends Table {
  public static Message getRootAsMessage(ByteBuffer _bb) { return getRootAsMessage(_bb, new Message()); }
  public static Message getRootAsMessage(ByteBuffer _bb, Message obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public Message __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public EncryptionCtx encryptionCtx() { return encryptionCtx(new EncryptionCtx()); }
  public EncryptionCtx encryptionCtx(EncryptionCtx obj) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  public KeyValue properties(int j) { return properties(new KeyValue(), j); }
  public KeyValue properties(KeyValue obj, int j) { int o = __offset(6); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int propertiesLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public byte payload(int j) { int o = __offset(8); return o != 0 ? bb.get(__vector(o) + j * 1) : 0; }
  public int payloadLength() { int o = __offset(8); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer payloadAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public ByteBuffer payloadInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 8, 1); }

  public static int createMessage(FlatBufferBuilder builder,
      int encryptionCtxOffset,
      int propertiesOffset,
      int payloadOffset) {
    builder.startObject(3);
    Message.addPayload(builder, payloadOffset);
    Message.addProperties(builder, propertiesOffset);
    Message.addEncryptionCtx(builder, encryptionCtxOffset);
    return Message.endMessage(builder);
  }

  public static void startMessage(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addEncryptionCtx(FlatBufferBuilder builder, int encryptionCtxOffset) { builder.addOffset(0, encryptionCtxOffset, 0); }
  public static void addProperties(FlatBufferBuilder builder, int propertiesOffset) { builder.addOffset(1, propertiesOffset, 0); }
  public static int createPropertiesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startPropertiesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addPayload(FlatBufferBuilder builder, int payloadOffset) { builder.addOffset(2, payloadOffset, 0); }
  public static int createPayloadVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startPayloadVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static int endMessage(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishMessageBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedMessageBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }
}

