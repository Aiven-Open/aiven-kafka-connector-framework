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

import org.apache.kafka.common.config.ConfigException;

import java.util.Map;
import java.util.function.Function;

public class VariableTemplatePart implements TemplatePart {

    private final String variableName;

    private final Parameter parameter;

    private final String originalPlaceholder;


    protected VariableTemplatePart(final String variableName, final String originalPlaceholder) {
        this(variableName, Parameter.EMPTY, originalPlaceholder);
    }

    protected VariableTemplatePart(final String variableName, final Parameter parameter,
                                   final String originalPlaceholder) {
        this.variableName = variableName;
        this.parameter = parameter;
        this.originalPlaceholder = originalPlaceholder;
    }

    @Deprecated
    public final String getVariableName() {
        return variableName;
    }

    @Deprecated
    public final Parameter getParameter() {
        return parameter;
    }

    @Deprecated
    public final String getOriginalPlaceholder() {
        return originalPlaceholder;
    }

    @Override
    public String render(final Map<String, Function<Parameter, String>> bindings) {
        final Function<Parameter, String> binding = bindings.get(variableName);
        return binding == null ? originalPlaceholder : binding.apply(parameter);
    }

    @Override
    public String extract(final Map<String, String> captureGroups) {
        String capture = captureGroups.get(variableName);
        if (capture != null) {
            // If the name was already used, we can capture it with a backreference.
            return String.format("\\k<%s>", capture);
        }
        String name = "capture" + captureGroups.size();
        captureGroups.put(variableName, name);
        return String.format("(?<%s>.*?)", name);
    }


}
