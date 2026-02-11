/*
 * Copyright 2025 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.commons.kafka.connector.source;

import java.io.InputStream;
import java.util.Optional;
import java.util.stream.Stream;

import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.task.Context;
import org.apache.commons.io.function.IOSupplier;

/**
 * A data source for native info from a source.
 * 
 * @param <K>
 *            the key type for the native object.
 * @param <N>
 *            the native object type.
 * @param <O>
 *            the OffsetManagerEntry for the iterator.
 * @param <T>
 *            the implementation class for AbstractSourceRecord.
 */
public interface NativeSourceData<K extends Comparable<K>, N, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<K, N, O, T>> {
	/**
	 * Gets the name for the source type.
	 * 
	 * @return the common name for the data source. For example "AWS S3 Storage" or
	 *         "AMQP Stream source".
	 */
	String getSourceName();

	/**
	 * Get a stream of Native object from the underlying storage layer. The
	 * implementation must return the native objects in a repeatable order based on
	 * the key. In addition, the underlying storage must be able to start streaming
	 * from a specific previously returned key.
	 *
	 * @param offset
	 *            the native key to start from. May be {@code null} ot indicate
	 *            start at the beginning.
	 * @return A stream of native objects. May be empty but not {@code null}.
	 */
	Stream<N> getNativeItemStream(K offset);

	/**
	 * Gets an IOSupplier for the specific source record.
	 *
	 * The implementation should extract an input stream from native item in the
	 * source record.
	 *
	 * @param sourceRecord
	 *            the source record to get the input stream from.
	 * @return the IOSupplier that retrieves an InputStream from the source record.
	 * @throws UnsupportedOperationException
	 *             If the source record does not provide an input stream.
	 */
	IOSupplier<InputStream> getInputStream(T sourceRecord);

	/**
	 * Retrieves the native key for the underlying storage that is associated with
	 * the native object.
	 *
	 * @param nativeObject
	 *            the native object to retrieve the native key for.
	 * @return The native key for the native object.
	 */
	K getNativeKey(N nativeObject);

	/**
	 * Retrieves the native key for the underlying storage that is represented by
	 * the string.
	 *
	 * @param keyString
	 *            the string representation of the kye.
	 * @return The native key for the string representation..
	 */
	K parseNativeKey(String keyString);

	/**
	 * Creates an instance of the concrete implementation of AbstractSourceRecord
	 * for the native object. The AbstractSourceRecord need only contain the
	 * {@link NativeInfo} instance.
	 *
	 * @param nativeObject
	 *            the native object to get the AbstractSourceRecord for.
	 * @return the AbstractSourceRecord for the native object.
	 */
	T createSourceRecord(N nativeObject);

	/**
	 * Creates an OffsetManagerEntry for a native object.
	 *
	 * @param nativeObject
	 *            the native object to create the OffsetManagerEntry for.
	 * @return An OffsetManagerEntry for a native object.
	 */
	O createOffsetManagerEntry(N nativeObject);

	/**
	 * Creates an offset manager key for the native key.
	 *
	 * @param nativeKey
	 *            THe native key to create an offset manager key for.
	 * @return An offset manager key.
	 */
	OffsetManager.OffsetManagerKey getOffsetManagerKey(K nativeKey);

	/**
	 * Extract context information from the native item. There are three possible
	 * returns from this method.
	 * <ol>
	 * <li>The item contains information that should be in the context: Create a
	 * populated Context and return it.</li>
	 * <li>The item does not contain information that should be in the context:
	 * Create a Context object containing only the native key and return it.</li>
	 * <li>The item should not be processed: Return an {@link Optional#empty()}</li>
	 * </ol>
	 *
	 * @param nativeItem
	 *            the native item to extract context from.
	 * @return An Optional containing the extracted context if possible.
	 */
	Optional<Context<K>> extractContext(final N nativeItem);
}
