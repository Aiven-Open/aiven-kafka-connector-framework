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
/**
 * Contains code to support Templating in connectors.
 *
 * <h2>Strategy</h2>
 * <p>
 * The templating framework allows users to specify variables that will be
 * placed into strings when they are rendered during the retrieval of the data
 * from the configuration file. This is done by introducing a pattern: {@code {{
 * foo }}} where the variable {@code foo} will be replaced during processing.
 * </p>
 *
 * <h2>Variable format</h2>
 * <p>
 * The variable comprises 2 parts: the {@code variableName} and an optional
 * {@code parameter}. The parts are separated by a colon. There may not be any
 * white space in the variable.
 * </p>
 * <p>
 * The variable is enclosed within pairs of curly braces, There may be spaces
 * between the braces and the variable. For example {@code {{foo}}} and
 * {@code {{ foo }}} are both valid.
 * </p>
 * <h3>Parameter format</h3>
 * <p>
 * A parameter, if it is present, consists of the separating colon, the
 * {@code parameterName}, and equals sign, and the {@code parameterValue}. The
 * parameter is used to provide data to a method that retrieves the data. For
 * example a 'timestamp' variable might take a 'format' parameter that specifies
 * the Java time format string.
 * </p>
 * <h3>Examples</h3>
 * <dl>
 * <dt>{@code {{ foo }}}</dt>
 * <dd>Defines a variable with the name "foo" that has no parameters.</dd>
 * <dt>{@code {{ foo:bar=baz }}}</dt>
 * <dd>Defines a variable with the name "foo" that has a parameter "bar" with a
 * value of "baz".</dd>
 * <dt>{@code {{ foo:bad = space }}}</dt>
 * <dd>Is an illegal formation as there are spaces in the parameter
 * definition.</dd>
 * <dt>{@code {{ foo bad:bar=baz }}}</dt>
 * <dd>Is an illegal formation as there are spaces in the
 * {@code variableName}.</dd>
 * <dt>{@code {{ foo :bad=space }}}</dt>
 * <dd>Is an illegal formation as there are spaces between the
 * {@code variableName} and the parameter definition.</dd>
 * </dl>
 *
 * <h2>Defining Variables</h2>
 * <p>
 * TempalteVariables are defined using the {@code TemplateVariable.Builder}
 * returned from the {@code TemplateVariable.builder(java.lang.String)} method.
 * In addition to the @{variableName} that is provided in the {@code builder}
 * call, variables have an optional description and optional
 * {@code ParameterDescriptor}. If the description is provided it is used when
 * generating documentation for users. The {@code ParameterDescriptor} must be
 * provided if the variable has a parameter.
 * </p>
 * <h3>Defining Parameters</h3>
 * <p>
 * As noted above, parameters are defined by a {@code ParameterDescriptor}. This
 * descriptor is created using the {@code ParameterDescriptor.Builder} returned
 * from {@code ParameterDescriptor.builder(java.lang.String)}. In addition to
 * the required @{code parameterName} the parameters have a required flag to
 * indicate the parameter is required, an optional description that is used for
 * user documentation, and an optional validator.
 * </p>
 * <p>
 * The validator, if present, is a {@code ConfigDef.Validator} and is used to
 * validate that the values specified in the template are valid. In addition,
 * the validator documentation is used ot provide additional user documentation.
 * </p>
 *
 * <h2>The TemplateVariableRegistry</h2>
 * <p>
 * The {@code TemplateVariableRegistry} is used to restrict the possible
 * parameters that can be used in a template and to provide documentation to
 * users. The TemplateVariableRegistry is built using the
 * {@code TemplateVariableRegistry.Builder} returned from
 * {@code TemplateVariableRegistry.builder()}. The TemplateVariableRegistry is
 * immutable.
 * </p>
 * <h2>The TemplateParser</h2>
 * <p>
 * The {@code TemplateParser} parses the template from the textual
 * representation. It will also validate that a textual representation is valid.
 * If the @{code parse} or {@code validate} methods are called with a
 * {@code null} TemplateVariableRegistry there will be no restrictions on the
 * valid variable names. If the registry is provided, then variable names that
 * are not in the registry will be accepted.
 * </p>
 * <h2>The TemplateValidator</h2>
 * <p>
 * The template validator is used to ensure that the template patterns specified
 * in the configuration file are valid. It should be used in the
 * {@code ConfigDef} to ensure that the data are valid before the connector
 * starts running. In addition, the validator will provide information for the
 * user configuration documentation.
 * </p>
 */
package io.aiven.commons.kafka.connector.common.templating;
