/*
 * Copyright 2026 Aiven Oy
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

package io.aiven.commons.kafka.connector.source.impl.csv;

import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AbstractSourceRecord implementation for the NativeObject.
 */
final public class ExampleCsvSourceRecord extends EvolvingSourceRecord<String, String> {
	private static final Logger LOGGER = LoggerFactory.getLogger(ExampleCsvSourceRecord.class);

	/**
	 * Constructor.
	 *
	 * @param nativeItem
	 *            The native item
	 */
	public ExampleCsvSourceRecord(final String nativeItem) {
		super(new NativeInfo<String, String>() {
			@Override
			public String getNativeItem() {
				return nativeItem;
			}

			/**
			 * Probably need to hash this for the key
			 * 
			 * @return The key for the csv value
			 */
			@Override
			public String getNativeKey() {
				return nativeItem;
			}

			@Override
			public long getNativeItemSize() {
				return nativeItem.length();
			}
		});
	}

	/**
	 * A copy constructor.
	 *
	 * @param source
	 *            the source record to copy.
	 */
	public ExampleCsvSourceRecord(final ExampleCsvSourceRecord source) {
		super(source);
	}

	@Override
	public ExampleCsvSourceRecord duplicate() {
		return new ExampleCsvSourceRecord(this);
	}

}
