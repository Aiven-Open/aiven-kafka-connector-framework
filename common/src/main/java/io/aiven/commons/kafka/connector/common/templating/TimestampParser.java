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

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/** Timestamp parser utilities. */
public final class TimestampParser {
  /** Definition of UTC Timezone */
  public static final TimeZone UTC = TimeZone.getTimeZone("UCT");

  private TimestampParser() {
    // do not instantiate
  }

  /**
   * Gets a SimpleDateFormat for the timestamp template. By default, the timezone is set to UTC.
   *
   * @param timestampTemplatePart the timestamp template part
   * @return the SimpleDateFormat with UTC timezone.
   */
  public static SimpleDateFormat getFormatter(VariableTemplatePart timestampTemplatePart) {
    return getFormatter(timestampTemplatePart.getParameter().getValue());
  }

  /**
   * Gets a SimpleDateFormat for the timestamp template. By default, the timezone is set to UTC.
   *
   * @param format the format for the timestamp
   * @return the SimpleDateFormat with UTC timezone.
   */
  public static SimpleDateFormat getFormatter(String format) {
    SimpleDateFormat sdf = new SimpleDateFormat(format, Locale.ROOT);
    sdf.setTimeZone(UTC);
    return sdf;
  }

  /** A validator for a timestamp template part. */
  public static ConfigDef.Validator VALIDATOR =
      new ConfigDef.Validator() {

        @Override
        public void ensureValid(String name, Object value) {
          if (value instanceof String strValue) {
            try {
              new SimpleDateFormat(strValue, Locale.ROOT);
            } catch (IllegalArgumentException e) {
              throw new ConfigException(name, value, e.getMessage());
            }
          } else {
            throw new ConfigException(name, value, "Value must be a string");
          }
        }

        @Override
        public String toString() {
          return "Value must be a valid SimpleDateFormat.  See https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html for a list of valid options.";
        }
      };
}
