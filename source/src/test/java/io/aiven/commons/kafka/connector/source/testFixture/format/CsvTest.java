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
package io.aiven.commons.kafka.connector.source.testFixture.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;

public class CsvTest {
  @Test
  public void roundTripTest() throws IOException {
    byte[] data = CsvTestDataFixture.generateCsvData(5);
    List<CSVRecord> lst = CsvTestDataFixture.readCsvRecords(data);
    assertThat(lst.size()).isEqualTo(5);
    for (int i = 0; i < 5; i++) {
      CSVRecord record = lst.get(i);
      assertThat(record.get(0)).isEqualTo(Integer.toString(i));
      assertThat(record.get(1)).isEqualTo("test message");
      assertThat(record.get(2)).isEqualTo(CsvTestDataFixture.MESSAGE_PREFIX + i);
    }
  }
}
