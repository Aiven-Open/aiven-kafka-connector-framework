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
package io.aiven.commons.kafka.connector.source.lookback;

import io.aiven.commons.util.collections.RingBuffer;

/**
 * Lookback that uses a buffer to track the last N records.
 * 
 * @param <K>
 *            the key type.
 */
public class Buffer<K extends Comparable<K>> implements Lookback<K> {
	private int bufferSize;
	private RingBuffer<K> ringBuffer;

	/**
	 * Constructor.
	 * 
	 * @param bufferSize
	 *            the size of the buffer
	 */
	Buffer(int bufferSize) {
		this.bufferSize = bufferSize;
		ringBuffer = new RingBuffer<>(bufferSize, RingBuffer.DuplicateHandling.REJECT, Comparable::compareTo);
	}

	@Override
	public void add(K key) {
		ringBuffer.add(key);
	}

	@Override
	public K get() {
		return ringBuffer.head();
	}

	@Override
	public boolean contains(K key) {
		return ringBuffer.contains(key);
	}

	@Override
	public int size() {
		return bufferSize;
	}

	@Override
	public Lookback<K> resize(int size) {
		if (bufferSize == size) {
			return this;
		}
		if (size <= 0) {
			return new None<>();
		}
		if (size == 1) {
			LastKey<K> lookback = new LastKey<>();
			lookback.add(ringBuffer.tail());
			return lookback;
		}
		RingBuffer<K> newBuffer = new RingBuffer<>(size);
		K key = ringBuffer.head();
		while (key != null) {
			newBuffer.add(key);
			ringBuffer.remove(key);
			key = ringBuffer.head();
		}
		ringBuffer = newBuffer;
		bufferSize = size;
		return this;
	}
}
