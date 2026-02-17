/*
         Copyright 2026 Aiven Oy and project contributors

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing,
        software distributed under the License is distributed on an
        "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
        KIND, either express or implied.  See the License for the
        specific language governing permissions and limitations
        under the License.

        SPDX-License-Identifier: Apache-2
 */
package io.aiven.commons.kafka.connector.common;

/**
 * Information about the Native object. The NativeInfo ties a Native key to a
 * Native object. In many cases the storage provides a key directly to the
 * native object and this implementation can simply call that. In other cases
 * the key and object are separate and the implementation will tie them
 * together. In the streaming data case there is no key to return to so the key
 * will be constructed when the object is available.
 *
 * @param <K>
 *            The native key type.
 * @param <N>
 *            The native object type.
 * @param nativeKey
 *            The native key for the storage.
 * @param nativeItem
 *            the native item for the storage.
 */
public record NativeInfo<K extends Comparable<K>, N>(K nativeKey,
		N nativeItem) implements Comparable<NativeInfo<K, N>> {
	@Override
	public int compareTo(NativeInfo<K, N> o) {
		return this.nativeKey.compareTo(o.nativeKey());
	}
}