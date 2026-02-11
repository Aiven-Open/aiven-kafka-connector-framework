/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.commons.kafka.connector.source.integration;


import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.connector.source.AbstractSourceRecord;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.task.DistributionType;
import io.aiven.commons.kafka.connector.source.transformer.ByteArrayTransformer;
import io.aiven.commons.kafka.connector.source.transformer.Transformer;
import io.aiven.commons.kafka.testkit.KafkaManager;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

//import static io.aiven.kafka.connect.common.source.AbstractSourceRecordIteratorTest.FILE_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/**
 * Test to verify OffsetManager and OffsetManagerKey implementations work as expected
 *
 * @param <K>
 *            the native key type.
 * @param <N>
 *            the native object type
 * @param <O>
 *            The {@link OffsetManager.OffsetManagerEntry} implementation.
 * @param <T>
 *            The implementation of the {@link AbstractSourceRecord}
 */
public abstract class AbstractOffsetManagerIntegrationTest<K extends Comparable<K>, N, O extends OffsetManager.OffsetManagerEntry<O>, T extends AbstractSourceRecord<K, N, O, T>>
        extends
            AbstractSourceIntegrationBase<K, N, O, T> {

    /**
     * Static to indicate that the TASK is not set.
     */
    private static final int TASK_NOT_SET = -1;

    /**
     * Tests offset manager ability to read data from the Kafka context by:
     * <ol>
     * <li>writing 4 records</li>
     * <li>reading them</li>
     * <li>verifying proper values were read</li>
     * <li>disabling the ring buffer and restarting the connector</li>
     * <li>writing one record</li>
     * <li>reading it</li>
     * <li>verifying that the proper value was read</li>
     * </ol>
     *
     * <p>
     * By disabling the ring buffer the filtering of seen records falls to the OffsetManager exclusively.
     * </p>
     * <p>
     * If the wrong value is read or no records are read then there was an error in the OffsetManager's attempt to read
     * and parse the context information.
     * </p>
     */
    @Test
    void offsetManagerRead() {
        final String topic = getTopic();

        final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
        final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
        final String testData3 = "Hello, Kafka Connect S3 Source! object 3";

        final Map<OffsetManager.OffsetManagerKey, Long> expectedOffsetRecords = new HashMap<>();
        // write 4 objects
        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(),
                1L);

        expectedOffsetRecords.put(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(),
                1L);

        try {
            // Start the Connector
            Map<String, String> props = CommonConfigFragment.setter(createConfig(topic, ByteArrayTransformer.class))
                    .keyConverter(ByteArrayConverter.class)
                    .valueConverter(ByteArrayConverter.class)
                            .data();
            SourceConfigFragment.setter(props).distributionType(DistributionType.PARTITION);

            final KafkaManager kafkaManager = setupKafka();
            kafkaManager.createTopic(topic);
            kafkaManager.configureConnector(getConnectorName(), props);

            assertThat(getNativeStorage()).hasSize(4);

            // Poll messages from the Kafka topic and verify the consumed data
            List<String> records = messageConsumer().consumeByteMessages(topic, 4, Duration.ofSeconds(10));

            // Verify that the correct data is read from the S3 bucket and pushed to Kafka
            assertThat(records).containsOnly(testData1, testData2);

            SourceConfigFragment.setter(props).ringBufferSize(0);

            getLogger().info(">>>>> RESTARTING");
            kafkaManager.configureConnector(getConnectorName(), props);
            getLogger().info(">>>>> RESTARTED");

            expectedOffsetRecords.put(write(topic, testData3.getBytes(StandardCharsets.UTF_8), 1).getOffsetManagerKey(),
                    1L);

            records = messageConsumer().consumeByteMessages(topic, 1, Duration.ofSeconds(30));

            assertThat(records).containsOnly(testData3);

        } catch (IOException | ExecutionException | InterruptedException e) {
            getLogger().error("Error", e);
            fail(e);
        } finally {
            deleteConnector();
        }
    }
}
