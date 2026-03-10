/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.commons.kafka.connector.source;

import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.task.Context;
import org.apache.commons.io.function.IOSupplier;

import java.io.IOException;
import java.io.InputStream;

/**
 * An abstract implementation of NativeInfo handling for Source.
 * 
 * @param <K>
 *            the native key type.
 * @param <N>
 *            the native data type.
 */
public abstract class AbstractSourceNativeInfo<K extends Comparable<K>, N>
		implements
			Comparable<AbstractSourceNativeInfo<K, N>> {
	/**
	 * Value to be returned when the length of the stream is unknown.
	 */
	public static final long UNKNOWN_STREAM_LENGTH = -1;

	/**
	 * Package protected for NativeSourceData use
	 */
	protected final NativeInfo<K, N> nativeInfo;

	/**
	 * Constructor.
	 * 
	 * @param nativeInfo
	 *            the native info to process.
	 */
	protected AbstractSourceNativeInfo(NativeInfo<K, N> nativeInfo) {
		this.nativeInfo = nativeInfo;
	}

	/**
	 * Gets the native key.
	 * 
	 * @return the native key.
	 */
	public K nativeKey() {
		return nativeInfo.nativeKey();
	}

	@Override
	public int compareTo(AbstractSourceNativeInfo<K, N> other) {
		return this.nativeKey().compareTo(other.nativeKey());
	}

	/**
	 * Creates the context for the native info.
	 * 
	 * @return the context for the native Info.
	 */
	public abstract Context getContext();

	@Override
	public String toString() {
		return nativeKey().toString();
	}

	/**
	 * Gets an InputStream supplier.
	 * 
	 * @return the InputStream supplier.
	 * @throws UnsupportedOperationException
	 *             if the underlying NativeInfo does not support input streams.
	 */
	public IOSupplier<InputStream> getInputStreamSupplier() throws UnsupportedOperationException {
		return this::getInputStream;
	}

	/**
	 * Read the input data from the nativeInfo.
	 * 
	 * @return the input stream
	 * @throws IOException
	 *             on IO error.
	 * @throws UnsupportedOperationException
	 *             if the underlying NativeInfo does not support input streams.
	 */
	protected abstract InputStream getInputStream() throws IOException, UnsupportedOperationException;

	/**
	 * Gets an estimate of the input stream length.
	 * 
	 * @return an estimate of the input stream length, or
	 *         {@link #UNKNOWN_STREAM_LENGTH} if not known.
	 * @throws UnsupportedOperationException
	 *             if the underlying NativeInfo does not support input streams.
	 */
	public abstract long estimateInputStreamLength() throws UnsupportedOperationException;

}
