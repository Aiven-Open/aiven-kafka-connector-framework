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

    SPDX-License-Identifier: Apache-2
-->

Design Goals
============

 - Handle common issue where Apache Kafka polls for data at a different frequency than the back end data production.
 - Handle the complexity in generating a proper source messages.
 - Focus developer efforts on extracting records from the data source.
 - Simplify the data extraction through the use of abstract classes.

Design Block Diagram
====================

Kafka connect view
------------------

```
  [ Kafka Connect System ]
     ^   |                        (read) 
     |   +------> [ Task.poll() ] -----> [                ]
     |        (return result)            [ Internal queue ]  
     +---------------------------------- [                ]
```

Data source view
-----------------

```
 [ Data source ]
         | (generate iterator on collection of native data objects)
         |
         v
      [ extract data from native key ] --+
`                                         |
                                         V
                         [ extract Kafka OffsetManager data ]
                                         |
                                         v
                         [ extract data from native object ]
                         [        "Extractor phase"        ]
                         [ (potentially multiple records)  ]
                                         |
                                         v
                         [ create one or more SourceRecords ] -+
                                                               |
   [                ]    (write data to internal queue)        |
   [ Internal queue ] <----------------------------------------+           
   [                ]
```

Developer Focus
==============

Developers should focus on implementing five classes:
 - Configuration that extends SourceCommonConfig.
 - AbstractSourceTask
 - AbstractSourceNativeInfo
 - NativeSourceData
 - OffsetManagerEntry

The framework calls the Object returned by the data source `Native Object` and the associated key `Native Key`.  The only constraint placed on the classes of these objects is that the `Native Key` must implement `Comparable`.  The native key is identified in Java generics as `<K>` and the native object is identified as `<N>`.

In addition to the five classes, there are several configuration options introduced by the framework that determine how the data are extracted from the native object and how the native keys are tracked.

The five classes
----------------

### Configuration


The Configuration will specify the information necessary to connect to the data source and extract data from it.  Typically, these are things like userId, userPassword, Host name, port, etc.

The Configuration class will need a Configuration definition that specifies the names and datatypes for the configuration properties.

###vAbstractSourceTask


The abstract source task requires the implementation of 3 methods.

#### protected EvolvingSourceRecordIterator getIterator(final SourceCommonConfig config)

This method accepts the configuration object and creates an iterator on the objects from the data layer.  The first step in the `Data source view` above. `EvolvingSourceRecordIterator` it a final class whose constructor only needs the configuration and the `NativeSourceData` implementation as described below.

#### protected SourceCommonConfig configure(final Map<String, String> props, final OffsetManager offsetManager)

This method accepts the properties as defined by the user and the OffsetManager implementation and configures the task for execution.


#### protected void closeResources()

This method provides a hook for the task to shut down any connections or other objects that need to be closed.  In general this cleans up any work done in the `configure()` method noted above.


#### protected EvolvingSourceRecord lastEvolution(final EvolvingSourceRecord evolvingSourceRecord)

The `EvolvingSourceRecord` is created early in the process and as processing applied its content evolves.  Eventually it becomes a Kafka SourceRecord.  The `lastEvolution` method is a final point at which a source implementation may make a change to the record before the SourceRecord is produced. The default implementation makes no changes. Common modifications at this point are:

* Key schema and/or value
* Value schema and/or value
* Offset manager entry

### AbstractSourceNativeInfo

AbstractSourceNativeInfo interrogates the native object to extract information needed to construct the source record.  It has three methods that must be implemented:

#### public Context getContext();

This method builds the initial Context from the native information.  The Context is developed as the source record data is discovered.  At a minimum the Context must contain the native key for the native Object.  If other information such as the Kafka topic or Kafka partition can be determined from the native object or its key they should be extracted and set in the Context by this method.

If the Context partition or topic values are set they will be used to initialize the partition and topic for the source record.  Later processing may override those values.

#### protected InputStream getInputStream() throws IOException, UnsupportedOperationException

In many data sources the source is actually some sort of data stream, for example a file from a file system, a byte array, a collection of CSV records, an Avro structure, etc.  This method gets that data as an input stream.  

In some cases the data can not be thought of as a stream.  In these cases this method must throw an UnsupportedOperationException.

#### public long estimateInputStreamLength() throws UnsupportedOperationException

This method works with the `getInputStream()` method above.  If that method throws UnsupportedOperationException then this method must do so also.  In all other cases this method returns an estimated length of the input stream.  If the lengths is unknown the value `AbstractSourceNativeInfo.UNKNOWN_STREAM_LENGTH` must be returned.

### NativeSourceData

Native source data defines data access methods on the native object and native key. It also creates the `OffsetManagerEntry` and gives the source its name.

#### public String getSourceName()

The common name for the data source. For example "AWS S3 Storage" or "AMQP Queue".

#### protected Iterator&lt;? extends AbstractSourceNativeInfo&lt;K, ?&gt;&gt; getNativeItemIterator(final K startFrom)

Creates an iterator of Native objects from the underlying storage layer. The implementation should return the native objects in a repeatable order based on the key. In addition, the underlying storage should be able to start from a specific previously returned key. Systems that can not meet the repeatable order or starting offset requirements may produce duplicate entries or may skip some entries.

####   public OffsetManager.OffsetManagerEntry createOffsetManagerEntry(final Map&lt;String, Object&gt; data);

Creates an offset manager entry using the data in the map.  The map the data extracted from a previous OffsetManagerEntry.  This method may return `null`, doing so will cause any partially processed native object to be reprocessed from the start.

**Note:** If the source object contains multiple records and the order of those records are not consistent across multiple retrievals of the object, this method should probably return  `null`.

#### protected OffsetManager.OffsetManagerEntry createOffsetManagerEntry(final Context context);

Creates an offset manager entry from data in the Context.

#### protected Optional&lt;KeySerde&lt;K&gt;&gt; getNativeKeySerde()

Returns a KeySerde for the native keys. If native key serialization to String is not supported this method must return an empty Optional.

#### protected OffsetManager.OffsetManagerKey getOffsetManagerKey(final K nativeKey)

Creates an offset manager key for the native key. The OffsetManagerKey implementation must meet the contract:
```
 K key = ...
 OffsetManagerKey k = getOffsetManagerKey(key);
 OffsetManagerKey k2 = getOffsetManagerKey(key);
 k2.partitionMap() is element for element equal to k.partitionMap()
```


### OffsetManagerEntry

Kafka provides a system to track what records have been processed from a source connector.  The `OffsetManagerEntry` defines the information that is stored in that system. The `OffsetManagerEntry` tracks the native key that the Kafka record came from so that on a restart previously processed records are not reprocessed. In addition each native object may contain zero or more Kafka records, so the `OffsetManagerEntry` associates a record number with each Kafka record generated.

The `OffsetManagerEntry` comprises two parts: a data map, and a key.

An `OffsetManagerEntry` must meet the contract: 
```
 K key = ...
 OffsetManagerEntry entry = createOffsetManagerEntry(context);
 OffsetManagerEntry entry2 = createOffsetManagerEntry(entry1.getProperties());
 entry2.getProperties() is element for element equal to entry1.getProperties()
```
 also
```
 OffsetManagerKey k = entry.getManagerKey()
 OffsetManagerKey k2 = entry2.getManagerKey()
 k2.partitionMap() is element for element equal to k.partitionMap()
```

#### OffsetManagerEntry fromProperties(Map<String, Object> properties)
    
Creates a new OffsetManagerEntry by wrapping the properties with the current implementation.  This method may throw a RuntimeException if required properties are not defined in the map.
    
#### Map&lt;String, Object&gt; getProperties()

Extracts the data from the entry in the correct format to return to Kafka. This method should make a copy of the internal data and return that to prevent any accidental updates to the internal data.

The `correct format to return to Kafka` means that the objects in the map must be natively serializable by Kafka.  These are limited to Strings, bytes, Numbers (Integer, Long, etc), arrays of the values just listed, and maps where the keys and values are any of the types listed here.  

#### Object getProperty(String key);

Gets the value of the named property. The value returned from a `null` key is implementation dependant but must throw a `NullPointerException` if a `null` key is not supported. A `null` value may be returned if the property is not set.

#### void setProperty(String key, Object value)

Sets the property. Will overwrite any existing value. Implementations of
OffsetManagerEntry may declare specific keys as restricted. These are generally keys that are managed internally by the OffsetManagerEntry and may not be set except through provided setter methods or the constructor.

#### OffsetManagerKey getManagerKey();

 Gets the `OffsetManagerKey` for this entry. The returned value should be a copy of the internal structure or constructed in such a way that modification to the key values is not reflected in the OffsetManagerEntry.

#### void incrementRecordCount()

Increments the record count for the offset manager.

#### long getRecordCount()

Gets the record count from the offset manager.
    
### OffsetManagerEntry.OffsetManagerKey

The `OffsetManagerKey` uniquely identifies the native object data in the Kafka topic, therefore the `OffsetManagerKey` must be constructable from the native key.  The simplest way to do this is to create a KeySerde to serialize the key to a string and deserialize the string back to the native key object, though other options are available.

The `OffsetManagerKey` implementation must
 * not include the record count.
 * override hashCode() and equals().

#### Map&lt;String, Object&gt; getPartitionMap()

Gets the partition map used by Kafka to identify this Offset entry. This is analogous to the `sourcePartition` in the Kafka SourceRecord it represents the native key associated with the native object that the record came from (e.g. a filename, table name, or topic-partition). In most cases this should be a map representation of the NativeKey.

Kafka stores all numbers as longs and so all keys based off integers should be created as longs in the manager key. 

This method should make a copy of the internal data and return that to prevent any accidental updates to the internal data.


Framework Configuration Options
---------------

There are a number of configuration options that are introduced by the Framework.  While all of them may be configured by users, several may be restricted by the source connector implementation. 

### The Extractor

The structure data returned from the data source may be an object or an input stream.  For example a cloud storage connector might read files from the storage and return the contents as an array of bytes or an input stream.  A connector that reads from a queueing system may read a queueing system specific object from the queue.  A connector that reads from a database may return a SQL ResultSet.  The Extractor handles converting those objects into data that can be passed to Kafka. 

The Framework provides implementations for some common data serialization formats.  These extractors require that the data source be provided as an InputStream.  The common formats are:
 * `Avro`: reads Avro generic records from the input stream, and returns each one as a Kafka SourceRecord.
 * `ByteArray`: Reads the input stream and returns byte[] arrays.  If the number of bytes in the input stream exceeds the maximum buffer size for the Extractor the input is broken into multiple Kafka SourceRecords.
 * `CSV`: reads CSV structured rows from the input stream.  Each CSV row is returned as a Kafka SourceRecord.
 * `JSONL`: reads JSON lines from the input stream.  Each JSON object (line) is returned as a Kafka SourceRecord. 
 * `Parquet`: reads the input stream as a Parquet file.  Like the Avro Extractor, each generic record becomes a Kafka SourceRecord.

If the `AbstractSourceNativeInfo` implementation implements the `getInputStream()` method any of these Extractors would be able to process the data. 

If there is a common format that it not supported the `InputStreamExtractor` may be extended to parse the input stream as appropriate.  We would be interested in adding additional extractors to our set of common extractors.

In some cases it the connector implementation may restrict the Extractor to a single type.  For example if a data source only returns CSV structures then there is no need for the user to specify an Extractor.  In fact, it would be a point of failure or them to do so.

For cases where the data source provides an object it is necessary to define a custom Extractor by extending the `Extractor` abstract class.  The extractor has a public method `Stream&lt;SchemaAndValue&gt; generateRecords(EvolvingSourceRecord sourceRecord)` that reads the data from the data source and returns a stream of `SchemaAndValue` objects.  The developer needs to decide how to extract the data and how to represent it to the Kafka environment.

### The circular buffer

When reading data from the source it is often necessary to skip previously seen records.  The framework does this in two ways.  First, it uses the Kafka offset manager framework to retrieve information about records that it has seen previously.  This structure works by looking for the native key in the offset manager data and determining if the data has been read completely.  The process of looking up the Kafka data involves some overhead.  In order to avoid this, the framework introduces a circular buffer that tracks the most recently completed entries.  The circular buffer only tracks native keys that the task has seen, and only since the task was started.  The circular buffer is not shared across tasks.

The size of the circular buffer is configurable.  If set to zero the buffer is disabled.

For some data sources a list of native keys may be requested, and those keys may include the creation time so that newer keys can be retrieved in chronological order.  In some of those cases it is possible that a key is not included in the first request but may be included in a later request because the data was being written while the first query was being executed.  In these cases simply processing from the last record seen will miss the new records.  The circular buffer provides a way to pick up those older, but recent, records.

The framework uses the circular buffer to retrieve a recent native key and passes that to the `NativeSourceData.getNativeIterator()` as the `startFrom` value.  If there is no circular buffer or the buffer is empty `null` is passed.  The native key returned from in the `AbstractSourceNativeInfo` record is checked against the Kafka offset manager to determine how many Kafka SourceRecords have been sent to Kafka from this native record.  Any Kafka SourceRecords that have not been previously sent are sent when the native data is processed.

