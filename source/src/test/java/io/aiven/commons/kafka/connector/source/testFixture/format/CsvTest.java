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
        List<CSVRecord> lst =  CsvTestDataFixture.readCsvRecords(data);
        assertThat(lst.size()).isEqualTo(5);
        for (int i=0; i<5; i++) {
            CSVRecord record = lst.get(i);
            assertThat(record.get(0)).isEqualTo(Integer.toString(i));
            assertThat(record.get(1)).isEqualTo("test message");
            assertThat(record.get(2)).isEqualTo(String.format("value-%s", i));
        }
    }
}
