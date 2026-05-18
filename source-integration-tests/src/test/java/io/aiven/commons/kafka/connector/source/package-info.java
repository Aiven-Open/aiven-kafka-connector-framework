/**
 * A collection of classes that implement a simple storage solution. In particular the storage is
 * created in a temporary directory the key is a string that contains, the topic, partition and a
 * ULID identifier concatenated as a path. For example the-topic/1/ULID. The ULID is automatically
 * generated.
 *
 * <p>The data are returned as a ByteBuffer.
 *
 * <p>Thus, the {@link io.aiven.commons.kafka.connector.common.NativeInfo} is defined as {@code
 * NativeInfo<String, ByteBuffer>}.
 */
package io.aiven.commons.kafka.connector.source;
