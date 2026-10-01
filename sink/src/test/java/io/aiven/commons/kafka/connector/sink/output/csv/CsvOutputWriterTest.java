package io.aiven.commons.kafka.connector.sink.output.csv;

import io.aiven.commons.kafka.connector.sink.TestingHeader;
import io.aiven.commons.kafka.connector.sink.output.OutputField;
import io.aiven.commons.kafka.connector.sink.output.OutputFieldEncodingType;
import io.aiven.commons.kafka.connector.sink.output.OutputFieldType;
import io.aiven.commons.kafka.connector.sink.template.SinkRecordBinding;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Java6Assertions.assertThat;

class CsvOutputWriterTest {
    private CsvOutputWriter underTest;
    private final List<OutputField> outputFields = new ArrayList<>();
    private static long TIMESTAMP = 1786716781355L;

    @Test
    void x() throws IOException {
        outputFields.add(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE));
        outputFields.add(new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE));
        outputFields.add(new OutputField(OutputFieldType.ORIGINAL_OFFSET, OutputFieldEncodingType.NONE));
        outputFields.add(new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE));
        outputFields.add(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE));
        outputFields.add(new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE));

        List<Header> headers = List.of(
                new TestingHeader("stringHeader", Schema.STRING_SCHEMA, "stringValue"),
                new TestingHeader("longHeader", Schema.OPTIONAL_INT64_SCHEMA, Long.valueOf(42)));

        /** SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset,
         Long timestamp, TimestampType timestampType, Iterable<Header> headers, String originalTopic,
         Integer originalKafkaPartition, long originalKafkaOffset)*/

        SinkRecord record = new SinkRecord("topic", 1, null, "key".getBytes(StandardCharsets.UTF_8), null, "value".getBytes(StandardCharsets.UTF_8), 2, TIMESTAMP, TimestampType.CREATE_TIME, headers, "original.topic" ,
                3, 4);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        underTest = new CsvOutputWriter(outputFields, baos);
        underTest.writeRecord(record);
        underTest.close();
        String value = baos.toString();
        assertThat(value).isEqualToIgnoringNewLines("key,2,4,1786716781355,value,c3RyaW5nSGVhZGVy:c3RyaW5nVmFsdWU=;bG9uZ0hlYWRlcg==:NDI=;");


        outputFields.clear();
        outputFields.add(new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.BASE64));
        outputFields.add(new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.BASE64));
        outputFields.add(new OutputField(OutputFieldType.ORIGINAL_OFFSET, OutputFieldEncodingType.BASE64));
        outputFields.add(new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.BASE64));
        outputFields.add(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64));
        outputFields.add(new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.BASE64));

         baos = new ByteArrayOutputStream();
        underTest = new CsvOutputWriter(outputFields, baos);
        underTest.writeRecord(record);
        underTest.close();
         value = baos.toString();
        assertThat(value).isEqualToIgnoringNewLines("key,2,4,1786716781355,dmFsdWU=,c3RyaW5nSGVhZGVy:c3RyaW5nVmFsdWU=;bG9uZ0hlYWRlcg==:NDI=;");
    }
}
