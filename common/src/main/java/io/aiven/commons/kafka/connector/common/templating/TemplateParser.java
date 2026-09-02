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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A parser for a template */
public final class TemplateParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemplateParser.class);

  /** Matches invalid name characters */
  static final Pattern INVALID_NAME = Pattern.compile("\\W");

  private TemplateParser() {}

  /**
   * Validates that the template string is parsable.
   *
   * @param configurationName the name of the template string. Generally the name of the
   *     configuration option that provided the template. pattern.
   * @param templatePattern the template string.
   * @param registry the registry of permitted TemplateVariables
   */
  public static void validate(
      final String configurationName,
      final String templatePattern,
      final TemplateVariableRegistry registry) {
    // generating the context performs the validation.
    new Context(configurationName, templatePattern, registry);
  }

  /**
   * Parses the template.
   *
   * @param templatePattern the template string.
   * @param registry the template variable registry.
   * @return The parsed template object.
   */
  public static Template parse(
      final String templatePattern, final TemplateVariableRegistry registry) {
    LOGGER.debug("Parse template: {}", templatePattern);
    Context context = new Context(null, templatePattern, registry);
    return new Template(templatePattern, context.getTemplateParts());
  }

  /**
   * Determines if a template or parameter name is valid. Valid names may not be empty and must
   * consist of letters, digits, {@code _}, {@code -}, and {@code .} only.
   *
   * @param name the name to check
   * @return {@code true} if name comprises letters, digits, {@code _}, {@code -}, and {@code .}
   *     only.
   */
  public static boolean isValidName(String name) {
    String validChars = "_-.";
    if (StringUtils.isEmpty(name)) {
      return false;
    }
    final int sz = name.length();
    for (int i = 0; i < sz; i++) {
      char c = name.charAt(i);
      if (!(Character.isLetterOrDigit(c) || validChars.indexOf(c) != -1)) {
        return false;
      }
    }
    return true;
  }

  /** The context for a parsing event. */
  private static class Context {
    private final List<TemplatePart> templateParts = new ArrayList<>();
    private final String configurationName;
    private final String templatePattern;
    private final TemplateVariableRegistry registry;

    private static final String VARIABLE_START = "{{";
    private static final String VARIABLE_END = "}}";

    Context(
        final String configurationName,
        final String templatePattern,
        final TemplateVariableRegistry registry) {
      this.configurationName = configurationName;
      this.templatePattern = templatePattern;
      this.registry = registry;

      int pos = 0;
      while (pos < templatePattern.length()) {
        pos = parseParts(pos);
      }
    }

    /**
     * Starting at a position in the templatePattern add the next text or variable part to the
     * templateParts.
     *
     * @param startPos the position to start scanning from.
     * @return the next start position to continue scanning.
     * @throws ConfigException on parsing error.
     */
    private int parseParts(int startPos) {
      int patternStart = templatePattern.indexOf(VARIABLE_START, startPos);
      if (patternStart == startPos) {
        // at start of pattern
        int patternEnd = templatePattern.indexOf(VARIABLE_END, patternStart);
        if (patternEnd == -1) {
          templateParts.add(new TextTemplatePart(templatePattern.substring(startPos)));
          return templatePattern.length();
        }
        parseVariable(
            templatePattern.substring(patternStart + VARIABLE_START.length(), patternEnd));
        return patternEnd + VARIABLE_END.length();
      }
      if (patternStart == -1) {
        // no pattern found
        templateParts.add(new TextTemplatePart(templatePattern.substring(startPos)));
        return templatePattern.length();
      }
      // text before pattern
      templateParts.add(new TextTemplatePart(templatePattern.substring(startPos, patternStart)));
      return patternStart;
    }

    /**
     * throws a detailed configuration exception with the specified error message.
     *
     * @param message the error message for the exception.
     */
    void errMsg(final String message) {
      if (configurationName == null) {
        throw new ConfigException(String.format("'%s' has error: %s", templatePattern, message));
      } else {
        throw new ConfigException(configurationName, templatePattern, message);
      }
    }

    /**
     * Parse a variable and add it to the templateParts.
     *
     * @param rawPattern the string from between a {@link #VARIABLE_START} and {@link #VARIABLE_END}
     *     pair.
     * @throws ConfigException on parsing error.
     */
    private void parseVariable(String rawPattern) {
      String pattern = rawPattern.trim();
      String templatePattern = VARIABLE_START + rawPattern + VARIABLE_END;
      if (StringUtils.isBlank(pattern)) {
        errMsg("Variable name hasn't been set for template");
      }

      int paramPos = pattern.indexOf(':');
      if (paramPos == -1) {
        templateParts.add(createTemplatePart(pattern, Parameter.EMPTY, templatePattern));
      } else {
        if (paramPos == 0) {
          errMsg(
              String.format(
                  "Variable name has not been set, '%s' may not start with a ':'", pattern));
        }
        String variableName = pattern.substring(0, paramPos++).trim();
        if (!isValidName(variableName)) {
          errMsg(String.format("'%s' is not a valid variable name", variableName));
        }
        if (registry != null && !registry.has(variableName)) {
          errMsg(String.format("'%s' is not defined in the variable registry", variableName));
        }
        String paramText = pattern.substring(paramPos).trim();
        if (paramText.isEmpty()) {
          errMsg(String.format("'%s' may not end with a ':'", pattern));
        }
        int eqPos = paramText.indexOf('=');
        if (eqPos == -1) {
          errMsg(
              String.format(
                  "Parameter '%s' of '%s' does not contain an '='", paramText, variableName));
        }
        if (eqPos == 0) {
          errMsg(
              String.format(
                  "Parameter '%s' of '%s' may not start with an '='", paramText, variableName));
        }
        String parameterName = paramText.substring(0, eqPos++).trim();
        String parameterValue = paramText.substring(eqPos);
        if (StringUtils.isEmpty(parameterValue)) {
          errMsg(String.format("Parameter '%s' value may not be empty", parameterName));
        }
        if (!isValidName(parameterName)) {
          errMsg(String.format("'%s' is not a valid parameter name", parameterName));
        }
        Parameter parameter = Parameter.of(parameterName, parameterValue);

        templateParts.add(createTemplatePart(variableName, parameter, templatePattern));
      }
    }

    /**
     * Creates a VariableTemplatePart.
     *
     * @param variableName the name of the variable.
     * @param parameter the Parameter for the variable.
     * @param variablePattern the original variable pattern a extracted from the template.
     * @throws ConfigException on parsing error.
     */
    private VariableTemplatePart createTemplatePart(
        String variableName, Parameter parameter, String variablePattern) {
      if (!isValidName(variableName)) {
        errMsg(String.format("'%s' is not a valid variable name", variableName));
      }
      if (registry != null) {
        if (!registry.has(variableName)) {
          errMsg(String.format("'%s' is not defined in the variable registry", variableName));
        } else {
          final String errName =
              configurationName == null
                  ? String.format("template variable '%s'", variableName)
                  : String.format("%s template variable '%s'", configurationName, variableName);
          registry.get(variableName).validate(errName, templatePattern, parameter);
        }
      }

      return new VariableTemplatePart(variableName, parameter, variablePattern);
    }

    /**
     * Gets the list of template parts.
     *
     * @return the list of template parts.
     */
    public List<TemplatePart> getTemplateParts() {
      return templateParts;
    }
  }
}
