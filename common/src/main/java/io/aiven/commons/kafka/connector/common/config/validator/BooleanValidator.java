package io.aiven.commons.kafka.connector.common.config.validator;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class BooleanValidator implements ConfigDef.Validator {
    private static ConfigDef.Validator inner = ConfigDef.CaseInsensitiveValidString.in("true", "false");

    public static BooleanValidator INSTANCE = new BooleanValidator();

    private BooleanValidator() {}

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            throw new ConfigException(name, null, "Value must be non-null");
        }
        inner.ensureValid(name, value);
    }
}
