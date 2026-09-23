package io.aiven.commons.kafka.connector.source;

import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.task.Context;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ExampleSourceNativeInfo extends AbstractSourceNativeInfo<String, ByteBuffer> {
  /**
   * Constructor.
   *
   * @param nativeInfo the native info to process.
   */
  public ExampleSourceNativeInfo(NativeInfo<String, ByteBuffer> nativeInfo) {
    super(nativeInfo);
  }

  @Override
  public Context getContext() {
    String[] parts = nativeInfo.nativeKey().split("/");
    Context.Builder<?> builder = Context.builder(nativeInfo.nativeKey());
    builder.topic(parts[0]);
    try {
      builder.partition(Integer.parseInt(parts[1]));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(
              "Partition part of native key '%s' is not a valid number: %s",
              nativeInfo.nativeKey(), e.getMessage()));
    }
    return builder.build();
  }

  @Override
  protected InputStream getInputStream() throws IOException, UnsupportedOperationException {
    return new ByteArrayInputStream(nativeInfo.nativeItem().array());
  }

  @Override
  public long estimateInputStreamLength() throws UnsupportedOperationException {
    return nativeInfo.nativeItem().capacity();
  }
}
