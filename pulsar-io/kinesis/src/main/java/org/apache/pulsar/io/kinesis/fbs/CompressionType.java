// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.pulsar.io.kinesis.fbs;

public final class CompressionType {
  private CompressionType() { }
  public static final byte NONE = 0;
  public static final byte LZ4 = 1;
  public static final byte ZLIB = 2;

  public static final String[] names = { "NONE", "LZ4", "ZLIB", };

  public static String name(int e) { return names[e]; }
}

