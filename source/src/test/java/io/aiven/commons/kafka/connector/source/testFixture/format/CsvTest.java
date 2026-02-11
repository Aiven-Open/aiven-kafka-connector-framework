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

import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvTest {
	@Test
	public void roundTripTest() throws IOException {
		byte[] data = CsvTestDataFixture.generateCsvData(5);
		List<CSVRecord> lst = CsvTestDataFixture.readCsvRecords(data);
		// 5 headers //5 records
		assertThat(lst.size()).isEqualTo(6);
		for (int i = 1; i < 6; i++) {
			CSVRecord record = lst.get(i);
			assertThat(record.get(0)).isEqualTo(Integer.toString(i - 1));
			assertThat(record.get(1)).isEqualTo("test message");
			assertThat(record.get(2)).isEqualTo(String.format("value-%s", i - 1));
		}
	}
}
