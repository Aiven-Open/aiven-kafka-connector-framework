package io.aiven.commons.kafka.connector.common.config.validator;

import io.aiven.commons.kafka.connector.common.templating.TemplateParser;
import io.aiven.commons.kafka.connector.common.templating.TemplateVariableRegistry;
import org.apache.kafka.common.config.ConfigDef;

public final class TemplateValidator implements ConfigDef.Validator {
    private final TemplateVariableRegistry registry;

    public TemplateValidator(TemplateVariableRegistry registry) {
        this.registry = registry;
    }
    @Override
    public void ensureValid(String name, Object value) {
        TemplateParser.validate(name, value.toString(), registry);
    }
}
