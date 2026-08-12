package io.aiven.commons.kafka.connector.sink.grouper;

import io.aiven.commons.kafka.connector.common.templating.TemplateVariableRegistry;

/**
 * A simple templated grouperKey that uses the {@link TemplateVariableRegistry#STANDARD_SINK}
 * variable definitions.
 */
public class SimpleTemplatedGrouperKey extends RecordGrouperKey {
  /** The template. */
  private final String templatePattern;

  /**
   * Constructor.
   *
   * @param templatePattern the template to use. May only use variables defined in the {@link
   *     TemplateVariableRegistry#STANDARD_SINK} variable definitions.
   */
  public SimpleTemplatedGrouperKey(final String templatePattern) {
    this.templatePattern = templatePattern;
  }

  @Override
  protected String getTemplatePattern() {
    return templatePattern;
  }
}
