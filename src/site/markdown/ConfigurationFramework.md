<!--
    Copyright 2026 Aiven Oy and project contributors

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

    SPDX-License-Identifier: Apache-2
-->
# Configuration Framework

The Configuration framework is based on the framework in `aiven.commons:kafka-config`.  In this framework, there are 3 major components:

1. The configuration(`CommonConfig`) - the configuration for a connector.
2. The configuration definition (`ConfigDef`) - the definition of the configuration.
3. The Fragment (`ConfigFragment`) - the definition of a shared piece of configuration.

## The Fragment

Starting with the `Fragment`, it is used to implement shared functionality across sink and source connectors.  For example an Amazon S3 connector needs to connect to S3, both the source and the sink connectors need the same capability, so an S3Fragment would be created to handle adding the connection configuration options to the configuration definition.  The `aiven.commons:kafka-config` `CommonConfig` uses the `CommonConfigFragment` to define the `max.tasks` and `max.id` properties in the configuration file.  The `CommonConfig` also uses the `BackoffPolicyFragment` to define the backoff policy for communicating with Kafka.  

Fragments must implement a static method with this exact signature: `public static void update(final ConfigDef configDef)`.  This method adds the keys for the Fragment to the configDef argument and returns the result. 

Fragments also have a hook to provide validation between property values.  For example if setting a `min` and a `max` value for something the fragment validation could check that the `min` was actually less than the `max`.  This check is performed by the `ConfigFragment.validate(final Map<String, ConfigValue> configMap)` method.  Implementations of the `validate` method, when the detect an error, should call `registerIssue(configMap, name, value, message)` where `configMap` is the configMap that was passed into the validate method, `name` is the name of the property that has the issue, `value` is the value the property has, and `message` is the message for the user to understand the issue and how to fix it.

Each `Fragment` implements a `Setter` that should be retrieved with a static method `public static Setter setter(final Map<String, String> data)`.  The `data` is the map of properties and values to update, the Setter method itself should have `setX` methods for each property that it creates.  This significantly simplifies the construction of test cases.  In some cases there may be multiple `setX()` methods, one taking the final datatype (for example an enum) and another taking a String so that invalid values can be tested.

The static variables that name the properties handled by the fragment should be private if possible.  This will encapsulate the naming and allow for easier deprecation of properties.

## The Configuration Definition

The configuration definition is constructed by extending the parent configuration definition. Each configuration definition is an instance of the Kafka `ConfigDef` class.  The configuration definition is responsible for adding the appropriate `Fragment`(s) and for ensuring that the validation of the fragments is performed.

To add a `Fragment` the definition the configuration definition constructor calls the fragment `update` method passing itself as the argument.  The standard constructor will look like:

```java
		public SomeConfigDef() {
			super();
			SomeConfigFragment.update(this);
		}
```

The configuration definition also performs the validation for the fragments by overriding the `multiValidate` method.  The default implementaiton of the  `multiValidate` method does nothing. Implementations must call the `super.multiValidate(valueMap)` method.  An example of an impelementation might look like:

```java
		@Override
		public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
			final Map<String, ConfigValue> values = super.multiValidate(valueMap);
			final FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(valueMap);
			new SomeFragment(fragmentDataAccess).validate(values);
			return values;
		}
```

## Configuration

The Configuration provides access to the data as defined by the Fragments in the associated configuration definition.  This means that the configuration will have an instance of each fragment that it encompasses and will delegate the `getX` calls to the fragment instance.

For the configuration classes that are not terminal (that is not the final configuraiton class but a parent of the final class) should accept their specific ConfigDef instance as well as the Map of original properties.   For example:

```java
	public SomeCommonConfig(final SomeConfigDef definition, final Map<String, String> originals) {
		super(definition, originals);
        final FragmentDataAccess dataAccess = FragmentDataAccess.from(this);
		someFragment = new SomeFragment(dataAccess);
	}
```

Terminal configuration classes should construct the terminal configuration def.  For example:

```java
    public TerminalConfig(final Map<String, String> properties) {
        super(new TerminalConfigDef(), properties);
        final FragmentDataAccess dataAccess = FragmentDataAccess.from(this);
        terminalFragment = new TerminalFragment(dataAccess);
    }
```


