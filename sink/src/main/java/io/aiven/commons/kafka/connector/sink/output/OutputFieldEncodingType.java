/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.commons.kafka.connector.sink.output;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum OutputFieldEncodingType {
    NONE("none", Function.identity(), Function.identity()), BASE64("base64", Base64.getEncoder()::encode, Base64.getDecoder()::decode);

    public static final String SUPPORTED_FIELD_ENCODING_TYPES = OutputFieldEncodingType.names()
            .stream()
            .map(c -> String.format("'%s'", c))
            .collect(Collectors.joining(", "));

    public final String name;
    public final Function<byte[], byte[]> encoder;
    public final Function<byte[], byte[]> decoder;

    OutputFieldEncodingType(final String name, Function<byte[], byte[]> encoder, Function<byte[], byte[]> decoder) {
        this.name = name;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    public static OutputFieldEncodingType forName(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        for (final OutputFieldEncodingType ofet : OutputFieldEncodingType.values()) {
            if (ofet.name.equalsIgnoreCase(name)) {
                return ofet;
            }
        }
        throw new IllegalArgumentException("Unknown output field encoding type: " + name);
    }

    public static boolean isValidName(final String name) {
        return names().contains(name.toLowerCase(Locale.getDefault()));
    }

    public static Collection<String> names() {
        return Arrays.stream(values()).map(v -> v.name).collect(Collectors.toList());
    }
}
