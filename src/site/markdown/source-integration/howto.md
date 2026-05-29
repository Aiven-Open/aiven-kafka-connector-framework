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

    SPDX-License-Identifier: Apache-2.0
-->
# Howto Implement Source Integration Tests

The source integration tests provide a simple framework to create integration tests for various configurations of a Connector.  The kafka-testkit project provides the foundation for the framework integration tests, and provides a good foundation for project integration tests that do not need a running Kafka.

## Framework integration components

The framework requires implementation of two classes to drive the tests: `SourceStorage` and `TestConfig`.

### SourceStorage

`SourceStorage` is an interface that provides access to the data source from outside the Kafka connector framework.  This class is used to write data to the store for the test to read, and to read some data from the data store to verify what should be retrieved by the connector.

`SourceStorage` extends `StorageBase` so the complete set of methods are

 - `getConnectorClass()` - Returns the Class for the connector that is being implemented.
 - `createStorage()` - Creates storage for a topic.  Some implementations do not differentiate between topics in which case this may be a no-op.
 - `removeStorage()` - Indicates that the storage for the topic is no longer needed and may be removed.  
 - `getNativeInfo()` - Returns a list of all `NativeInfo` objects in the data source that are associated with the topic from the last `createStorage` call.
 - `createNativeKey()` - Creates a new testing native key taking into account the specified topic and partition.  If the store does not differentiate topic and/or partitions they may be ignored but the method must produce something that is a `native key`.  This `native key` will be passed to the `writeWithKey()` method. 
 - `writeWithKey()` Writes a data blob to the Storage using the specified `native key`.  This method returns a `WriteResult` which is a Java record that contains the `native key` from the write as well as the expected `OffsetManagerKey` for data.

SourceStorage also defines the `TestData` record.  This is a record that contains the data that were written as well as the expected result when read from Kafka.

### TestConfig

`TestConfig` defines the configuration for a test.  It comprises a name, a generator of `TestData`, an initial configuration for the connector.  It has a methods to write `TestData` to the storage for a specific Kafka topic.  For some implementations the data source does not differentiate between different topic stores.  

`TestData` also has a method that consumes messages from Kafka and verifies the result against what was expected.  This method is passed a `MessageConsumer` which will read from the correct topic, the topic that is being read, the `TestData` records that are expected, the `WriteResults` that are expected, and a Duration in which the results should be returned. The implementation should develop a list of expected results from the `TestData` or `WriteResults` depending on what is being tested, and then read the data and verify that the results were retrieved.

For example, a test that expects that the Kafka SourceRecord will contain byte[] might have an implementation like:

````java 
public void consumeMessages(
AbstractSourceIntegrationBase.MessageConsumer messageConsumer,
  String topic,
  List<SourceStorage.TestData> testData,
  List<SourceStorage.WriteResult> writeResults,
  Duration timeout) {
    // in this implementation:
    //   1. every native object produces a single Kafka SourceRecord
    //   2. Kafka SourceRecord is expected to have a byte[] value.

    // first extract all the expected values from the TestData.
    List<byte[]> expected =
        SourceStorage.TestData.expected(testData).stream().map(o -> (byte[]) o).toList();
    
    // create a predicate to verify that a byte[] is in the list of expected byte[]
    Predicate<byte[]> p =
        x -> {
          for (byte[] y : expected) {
            if (Arrays.equals(x, y)) {
              return true;
            }
          }
          return false;
        };

    // read the messages via the message consumer.  In this case we are consumingByteMessages 
    // so we just read them in other cases we might be reading String, JSON or Avro objects 
    // so we could just read those types.  If the test requires other information from the Kafka 
    // SourceRecord then source records could be read.
    List<byte[]> records = messageConsumer.consumeByteMessages(topic, testData.size(), timeout);
    assertThat(records).allMatch(p);
}
````

The `consumeMessages()` method allows different `TestConfig` implementations to test different output based on the connector configuration. 


## Tests that do not require Kafka

In most cases the NativeSourceData and AbstractSourceTask classes are the only implementation classes that require integration testing.  However they do require a data store implementation.  The data store implementation should be contained within a `TestingContainer`.  Many data stores have `TestContainer` implementations and in cases where the `TestContainer` is not provided using a standard Dockerized data source is trivial.  In addition these test should extend `io.aiven.commons.kafka.testkit.KafkaIntegrationTestBase`

It is generally best if the `org.testcontainers.junit.jupiter.Testcontainers` annotation is used on the test class to indicate that one or more containers need to be controlled by the testing framework.  Inside the class there should be an annotated container.  Something like:
```java

import io.aiven.commons.kafka.testkit.KafkaIntegrationTestBase;

@Testcontainers
public class myIntegrationTest extends KafkaIntegrationTestBase {

  @Container static MyContainer myContainer = new MyContainer(some, parameters, here);
  
    ...
```

The container may be static if the test cases will correctly execute against a container without restarting.  If the test cases interfere with each other then a non-static container should be used.  However, this will restart the container before each test and will result in longer test runs. 

### SourceStorage integration tests

It may seem strange to perform integration tests on the SourceStorage implementation but this may uncover any incorrect assumptions about the data source and how to interact with it.  Validating that the data written with `writeWithKey()` is returned in a `getNativeInfo()` call will ensure that other integration test failures are failures of the connector and not communication with the underlying data system.

### NativeSouceData integration tests

Write some data to the container using the test fixtures in the framework source test-jar or code a library that will produce `N` records on demand.  Create a number of records and write them with the `SourceStorage.writeWithKey` method.  Then use awaitility to wait until the NativeSourceData returns an iterator that contains data.

```java
    final Iterator[] iter = new Iterator[1];
    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              iter[0] = underTest.getNativeItemIterator(null);
              return iter[0].hasNext();
            });
    AmqpSourceNativeInfo nativeInfo = (AmqpSourceNativeInfo) iter[0].next();
    assertThat(iter[0]).isExhausted();
```

continue by validating that the nativeInfo contains the expected data.

An additional test to write multiple records and the call `underTest.getNativeItemIterator()` passing the `native key` from one of the middle writes to veriify tha the first record returned is the `native key` that was the call parameter is also advised.

### AbstractSourceTask integration tests.

The AbstractSourceTask test should verify that the record(s) to be written to Kafka is what is expected from the data.  Basically this test should:
1. Setup the SourceStorage.
2. Construct a SourceTask.
3. Write some data to the SourceStorage,
4. Initialize the task.
5. Start the task.
6. Wait for the task to be running.
7. Wait until one or more calls to`task.poll()` returns the number of expected results.
8. Verify that the records received contain the expected data.

```java

import io.aiven.commons.kafka.testkit.KafkaIntegrationTestBase;

@Testcontainers
public class mySourceTaskTest extends KafkaIntegrationTestBase {

    @Container static MyContainer myContainer = new MyContainer(some, parameters, here);
    
    private MySourceTask underTest;
    private SourceStorage<String, byte[]> sourceStorage = new MySourceStorage();
    
    
    @Test
    void testMessageRead() throws IOException, ExecutionException, InterruptedException {
        // 1. Setup SourceStorage. getTopic() create a topic name from the test method name.
        sourceStorage.createStorage(getTopic());

        // 2. Construct the source task.
        underTest = new MySourceTask();
        
        // 3. Write some test data to storeage
        Map<String, String> config = sourceStorage.createConnectorConfig();
        CommonConfigFragment.setter(config).maxTasks(1);
        SourceConfigFragment.setter(config).targetTopic(getTopic());

        LOGGER.info("{}", config);

        byte[] body = "hello world".getBytes(StandardCharsets.UTF_8);
        String key = SourceStorage.createKey(getTopic(), 1);
        SourceStorage.WriteResult writeResult =
                sourceStorage.writeWithKey(sourceStorage.createKey(getTopic(), 1), body);
        assertThat(writeResult).isNotNull();

        // 4. initialize the task
        SourceTaskContext context = mock(SourceTaskContext.class);
        when(context.offsetStorageReader()).thenReturn(mock(OffsetStorageReader.class));
        when(context.offsetStorageReader().offset(any(Map.class))).thenReturn(null);
        underTest.initialize(context);

        // 5. start the task
        // Poll messages from the Kafka topic and verify the consumed data
        underTest.start(config);
        
        // 6. wait until task is running
        await().atMost(Duration.ofSeconds(5)).until(() -> underTest.isRunning());

        // 7. Wait until one or more calls to`task.poll()` returns the number of expected results.
        final List<SourceRecord> result = new ArrayList<>();
        await()
                .atMost(Duration.ofMinutes(2))
                .until(
                        () -> {
                            List<SourceRecord> newLst = underTest.poll();
                            if (newLst != null) {
                                result.addAll(newLst);
                            }
                            // 1 = number of records expected, normally this would be determined
                            // by the number of members in a list of expected results.
                            return newLst == null && result.size() == 1;
                        });
        
        // 8. Verify the results are correct.
        SourceRecord sourceRecord = result.get(0);

        assertThat(sourceRecord.valueSchema()).isEqualTo(Schema.BYTE_SCHEMA);
        assertThat(sourceRecord.value()).containsExactly(body);
        // other tests should be added here to ensure tha tthe offset records and key schema and
        // value are correct.
    }
}
```

## Tests that do require Kafka

When tests require kafka we utilize the Aiven [kakfa-testkit](https://aiven-open.github.io/test-kit) library.  The test kit makes it easy to start, stop, and query a Kafka connector system.

### Source Connector integration tests

The `AbstractSourceConnectorIntegrationTest` provides a minimally complete test for a connector and `TestConfig`.  It is uses generics to define the `native key` (K) and `native object` (N) types and requires implementation of two methods:

 * `protected TestConfig getTestConfig()`: gets the `TestConfig` implementation for the test.
 * `protected SourceStorage<K, N> getSourceStorage()`: gets the `SourceStorage` implementation for the test.

The tests verify that the Connector properly handles cases where data is updated between runs and after initial exhaustion.  The integration test framework is designed so that multiple configurations can be easily tested against a single data source.






