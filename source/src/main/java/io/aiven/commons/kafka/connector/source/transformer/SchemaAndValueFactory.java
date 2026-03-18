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

/**
 * A factory to create {@link SchemaAndValue} instances from arbitrary Objects.
 */
public final class SchemaAndValueFactory {
	/** The object mapper to use. */
	private static final ObjectMapper mapper = new ObjectMapper();
	/** the SchemaGenerator to use */
	private static final SchemaGenerator generator = new SchemaGenerator(
			new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON).build());

	/**
	 * Default constructor
	 */
	private SchemaAndValueFactory() {
		// do not instantiate.
	}

	/**
	 * Creates a SchemaAndValue for the Object.
	 * 
	 * @param object
	 *            the Object to generate the schema and value for.
	 * @return the SchemaAndValue object.
	 */
	public static SchemaAndValue createSchemaAndValue(Object object) {
		SchemaAndValueCreator consumer = new SchemaAndValueCreator();
		consumer.accept("root", object);
		return new SchemaAndValue(consumer.getSchema().field("root").schema(), consumer.getValue());
	}

	/**
	 * A class that processes objects recursively to produce a schema and a Jason
	 * object.
	 */
	private static class SchemaAndValueCreator implements BiConsumer<String, Object> {
		/** The root of the constructed JSON object */
		private final ObjectNode root = mapper.createObjectNode();
		/** The schema for the JSON object */
		private final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

		/**
		 * Gets the JSON object.
		 * 
		 * @return The JSON object value for the processed object.
		 */
		public ObjectNode getValue() {
			return root;
		}

		/**
		 * Gets the Schema for the processed object.
		 * 
		 * @return the schema for the processed object.
		 */
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
