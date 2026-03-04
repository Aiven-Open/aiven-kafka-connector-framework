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
package io.aiven.commons.kafka.connector.common.templating;

import java.util.Map;
import java.util.function.Function;

/**
 * A fragment of the template as segregated by the parser.
 * The part must implement toString() to produce a string representation of itself.
 */
public interface TemplatePart {
    /**
     * Renders the template part using the bindings as necessary.
     * @param bindings the binding to map parameters with.
     * @return the rendered string.
     */
    String render(Map<String, Function<Parameter, String>> bindings);

    /**
     * Extracts the original string from the template.
     * @param captureGroup the capture group to use for extraction.
     * @return the original string for this template.
     */
    String extract(Map<String, String> captureGroup);

}
