package io.aiven.commons.kafka.connector.source.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.BiConsumer;

public final class SchemaAndValueFactory {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final SchemaGenerator generator = new SchemaGenerator(new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12,
            OptionPreset.PLAIN_JSON).build());

    private SchemaAndValueFactory() {
        // do not instantiate.
    }

    public static SchemaAndValue createSchemaAndValue(Object o) {
        SchemaAndValueCreator consumer = new SchemaAndValueCreator();
        consumer.accept("root", o);
        return new SchemaAndValue( consumer.getSchema().field("root").schema(), consumer.getValue());
    }

    private static class SchemaAndValueCreator implements BiConsumer<String, Object> {
        private final ObjectNode root = mapper.createObjectNode();
        private final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

        public ObjectNode getValue() {
            return root;
        }

        public SchemaBuilder getSchema() {
            return schemaBuilder;
        }

        @Override
        public void accept(String s, Object o) {
            if (o == null) {
                schemaBuilder.field(s, Schema.OPTIONAL_BYTES_SCHEMA);
                root.putNull(s);
            } else if (o instanceof Number n) {
                if (o instanceof Byte) {
                    schemaBuilder.field(s, Schema.INT8_SCHEMA);
                    root.put(s, n.byteValue());
                } else if (o instanceof Short) {
                    schemaBuilder.field(s, Schema.INT16_SCHEMA);
                    root.put(s, n.shortValue());
                } else if (o instanceof Integer) {
                    schemaBuilder.field(s, Schema.INT32_SCHEMA);
                    root.put(s, n.intValue());
                } else if (o instanceof Long) {
                    schemaBuilder.field(s, Schema.INT64_SCHEMA);
                    root.put(s, n.longValue());
                } else if (o instanceof Float) {
                    schemaBuilder.field(s, Schema.FLOAT32_SCHEMA);
                    root.put(s, n.floatValue());
                } else if (o instanceof Double) {
                    schemaBuilder.field(s, Schema.FLOAT64_SCHEMA);
                    root.put(s, n.floatValue());
                } else if (o instanceof BigDecimal bd) {
                    schemaBuilder.field(s,
                            SchemaBuilder.struct().name("BigDecimal").field("value", Schema.STRING_SCHEMA));
                    root.put(s, root.objectNode().put("value", bd.toString()));
                } else if (o instanceof BigInteger bi) {
                    schemaBuilder.field(s,
                            SchemaBuilder.struct().name("BigInteger").field("value", Schema.STRING_SCHEMA));
                    root.put(s, root.objectNode().put("value", bi.toString()));
                }
            } else if (o instanceof String) {
                schemaBuilder.field(s, Schema.STRING_SCHEMA);
                root.put(s, (String) o);
            } else if (o instanceof Boolean) {
                schemaBuilder.field(s, Schema.BOOLEAN_SCHEMA);
                root.put(s, (Boolean) o);
            } else if (o instanceof byte[]) {
                schemaBuilder.field(s, Schema.BYTES_SCHEMA);
                root.put(s, (byte[]) o);
            } else {
                schemaBuilder.field(s, SchemaBuilder.struct().name("Object").field("class", Schema.STRING_SCHEMA)
                        .field("json", Schema.STRING_SCHEMA));
                root.set(s, root.objectNode().put("class", o.getClass().getCanonicalName()).put("json",
                        generator.generateSchema(o.getClass()).toPrettyString()));
            }
        }
    }
}
