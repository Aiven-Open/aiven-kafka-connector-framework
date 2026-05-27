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
# Howto Build A Source Connector

This document describes how to build a source connector using this framework.  There is a complete simple example in the [source integration tests testing code](https://github.com/Aiven-Open/aiven-kafka-connector-framework/tree/main/source-integration-tests/src/test/java/io/aiven/commons/kafka/connector/source).

## High level overview

The source connector sits between the Kafka environment on one side and the data storage on the other.  It must deal with the potential impedance mismatch between the two. To do this the source connector creates a queue of records that are available to send to Kafka and sends them when Kafka polls. The storage layer simply provides an iterator over the currently available data keys and the framework works through the iterator to retrieve the data and create Kafka records that it will send when Kafka polls. If the backend has more data than Kafka is ready for the framework pauses until there is space in the queue again.  If the backend has no data to send it returns an empty iterator and the framework will delay and request again later.

In addition, Kafka records when it completed the ingestion of the records send by the framework. The framework can then track which records have been send and ensure that it requests records after a specific point. If the connector is stopped and restarted it will start by requesting the data after the last confirmed record.

## Thinking about the solution

The connector implementation should concentrate on extracting data from the data source and creating a format for Kafka.

### Extracting data from the data source.

The framework expects that the data source will provide a key, called the `native key`, that will retrieve data from the source.  There are cases where this is not the case and workarounds are discussed in special cases section below.  The framework also expects that the data source will define a `native object` which is the object returned when the `native key` is requested from the data source.

The framework expects that:
 * the `native key` implements Comparable.
 * the data source will provide a method to list `native key`s. 
 * the data source provides a method that will return a `native object` when presented with a `native key`.
 * the `native obejct` is immutable with respect to the `native key`.  This means that subsequent calls with the same `native key` will yield the same data.  Not necessarily the same `native object` but objects containing the same data.


The first step is to implement a `AbstractSourceNativeInfo`.  There is a Java record defined in the common module called `NativeInfo`, this record links the `native key` and `native object` together in one object while providing an implementation of comparable that delegates to the `native key`.  The `AbstractSourceNativeInfo` uses the `NativeInfo` and provides methods to convert the `native key` to a string, get the `native object` data as an InputStream and estimate the size of that input stream.  In some cases the `native object` may not have the concept of an input stream. In this case see the special case below.

The second step is to design the `OffsetManagerData` and `OffsetManagerKey`, these interfaces are defined in the `OffsetManager` class.  The `OffsetManager` tracks what `native object`s the connector has already processed.  It does this by using a Kafka provides data stream.  Each `native object` will produce at least one Kafka SourceRecord, the OffsetManager will track the SourceRecord using the OffsetManagerKey.  In most cases the OffsetManagerKey can simply be the string representation of the `native key`.  The `OffsetManagerData` should contain other information necessary to retrieve the `native object`. 

 - The `OffsetManagerKey` must uniquely identify the `native key`.  It is called the `partitionMap` in the Kafka documentation.  Its data is stored in a Map with String keys and Object values.  The Objects are limited to the Objects that are natively supported by Kafka: String, Integer, Long.
 - The `OffsetManagerData` provides additional data associated with the `OffsetManagerKey`.  This can be any data that may be necessary to reset the data source to a state where the `native key` can be retrieved.

The third step is to implement the `NativeSourceData` abstract class.  The `NativeSourceData` requires implementation of:
 - A name.  This is a common name for the source, for example, "My SQL Data source", or "S3 data source".  It is used in logging to assist in determining where data is from and where issues have arisen.
 - A native item iterator.  This is actually an iterator of `AbstractSourceNativeInfo` objects.  The method that creates the iterator will pass in a `native key` that is the last key that was processed.  The implementation should attempt to return that key and any keys that come after it in the iterator.
 - A method to create an `OffsetManagerEntry` from a previously stored `OffsetManagerEntry` property map, for example, from `OffsetManagerEntry.getProperties()`.
 - A method to create an `OffsetManagerEntry` from a `Context` instance.
 - A getter for the NativeKey Serializer / Deserializer if available, otherwise return empty Optional.
 - A method to get the `OffsetManagerKey` from the `native key`.

The forth step is to select or implement an `Extractor`.  In many cases the `native object` contains multiple Kafka records, for example, a `native object` might contain a CSV or JSONL structure that should be parsed to return one Kafka record for each entry.  The Extractor does this.  In most cases the Extractor uses the `AbstractSourceNativeInfo` to retrieve an input stream and process it into multiple records.  In some cases the `native object` value should be placed on Kafka as a byte array.  In this case the `ByteArrayExtractor` can be used.  If the `native object` does produce an input stream, see the special case listed below.

Finally implement the `AbstractSourceTask` this requires:
 - Implementation of a `getIterator()` method that returns an `EvolvingSourceRecordIterator`.
 - Implementation of a `configuration()` method.
 - Implementation of a `closeResources()` method to shut down cleanly.

and implement a Kafka SourceConnector that configures the SourceTask.

### What is the `EvolvingSourceRecord`

The evolving source record is a source record that is developed as data are retrieved and processed. It eventually becomes the Kafka SourceRecord. Initially the evolving source record contains the `Context` returned from the AbstractSourceNativeInfo.  The Context contains the `native key` and may contain the topic and/or partition that the data may be written to. The EvolvingSourceRecord also contains the `AbstractSourceNativeInfo` for a singe `native object`.

### What is the `EvolvingSourceRecordIterator`

The evolving source record is, as it says, an iterator of `EvolvingSourceRecord`s.  It is produced by the `AbstractSourceTask` from the configuration and the `NativeSourceData`
implementation.  This iterator makes extensive use of the [Aiven Common Utils ExtendedIterator](https://aiven-open.github.io/common-util/apidocs/io.aiven.commons.util/io/aiven/commons/util/collections/ExtendedIterator.html) class.

Internally it uses two iterators.  The `inner iterator` is the iterator returned from the NativeSourceData.  This iterator is wrapped by an `outer iterator` that allows for the extraction of multiple records from the `native object`.  The iterators are built from simple iterators and applying filters and mapping to them much like a Stream.

#### inner iterator

The inner iterator is created by calling the `NativeSourceData.getNativeItemIterator()`.
The `native item` returned from the iterator is mapped to an `EvolvingSourceRecord`, during which any `native key` that is in a `look back` buffer is skipped.  This buffer is configurable in the configuration.  The `overrideContextTopic` is applied to the `Context` from the `AbstractSourceNativeInfo` to create the `Context` for the `EvolvingSourceRecord`.  An `OffsetManagerEntry` is created from the new `Context`. The `OffsetManagerEntry` may be modified by the `OffsetManager` if there record was previously processed.  This allows for recovery from partial processing of `native object`s that contain multiple Kafka SourceRecords.  The `NativeSourceData`, `Context` and `OffsetManagerEntry` are then used to create an `EvolvingSourceRecord`.

The iterator is then filtered to ensure that only records that this task are supposed to process are returned.

Some internal bookkeeping is applied on each retrieval, and the final iterator returned.
The net result is that the `inner iterator` returns EvolvingSourceRecords that wrap `native objects` that are to be processed by this task.

#### outer iterator

The `outer iterator` handles the case where the `native object` contains multiple Kafka SourceRecords.  The outer iterator takes is constructed by passing `EvolvingSourceRecord` returned by the `inner iterator` through the specified `Extractor` which may return 0 or more new `EvolvingSourceRecords` based on the one returned by the `inner iterator`.  The new `EvolvingSourceRecords` are returned in an iterator.

The net result is that the `outer iterator` returns one `EvolvingSourceRecord` for each Kafka SourceRecord that should be generated.  

#### flow

The `EvolvingSourceReocrdIterator` determines if it has data by :
1. Checking the outer iterator.  If the `outer iterator` has data the `EvolvingSourceRecordIterator` has data.  
2. If the `outer iterator` does not have data then the `inner iterator` is checked.  If the `inner iterator` has data then an `EvolvingSourceRecord` is retrieved and the `outer iterator` refreshed.  The `EvolvingSourceRecordIterator` then returns the result of the `outer iterator` has next call.
3. if the `inner iterator` does not have data then the `inner iterator` is refreshed via a call to the `NativeSourceData.getNativeItemIterator()`.  The `outer iterator` is refreshed adn the result of the `outer iteator` has next call is returned.


### How the pieces fit together.

The `AbstractSourceTask` handles requests from Kafka for data.  It uses an internal queue and an Iterator of Kafka SourceRecords to do this.

The Iterator of Kafka SourceRecords is built by retrieving the EvolvingSourceIterator from the `AbstractSourceTask` and modifying it as an [ExtendedIterator]([Aiven Common Utils ExtendedIterator](https://aiven-open.github.io/common-util/apidocs/io.aiven.commons.util/io/aiven/commons/util/collections/ExtendedIterator.html) ) by:
1. Applying the `lastEvolution()` transformation to it.  This allows the SourceTask to make any last minute modifications to or data extractions from the final record.  
2. Mapping the `EvolvingSourceRecord` to a Kafka SourceRecord by calling `EvolvingSourceRecord.getSourceRecord()`.

The queue is populated by a polling thread that continually calling iterator of Kafka SourceRecords `hasNext()` method, if there is a next record it is retrieved and added to the queue.  If not, the thread delays for awhile and checks again.  The delay periods are configurable.

## Special cases

### Data source does not have a `native key`.

In some cases the data source does not have a `native key`.  An example of this is reading from an AMQP style queue where listeners given a message but that message is not replayable nor is it guaranteed to have a unique identifier.  In these cases there are several things to consider.

1. A key needs to be generated and that key should be lexically sortable with respect to time so that earlier results have a lower key than later results.  The simplest solution is to use a [ULID](https://github.com/ulid/spec) to identify the records as they arrive from the data source.  Using the ULID.Value as the key will work well.
2. The lookback buffer should be set to 0 since earlier ULIDs will not be repeated.
3. The `NativeSourceData.getNativeItemIterator()` implementation should ignore the startFrom parameter and simply return the data objects from the data source.


### Data source does not guarantee immutable `native object`.

Some data sources will return a native object for which the contents may change.  An example of this is the Salesforce&trade; CSV results.  The issue is how to detect that the data has changed and how to record that in the `OffsetManagerKey` and `OffsetManagerEntry`.

Kafka tracks the records that have been extracted from the data source via the `OffsetManager`.  The `OffsetManagerKey` should identify the `native object` regardless of whether it has changed or not.  For systems like Salesforce where the key is a query it may make sense to use the string representation of the  query as the `OffsetManagerKey` or it may make sense to decompose the query into constituent parts for the `OffsetManagerKey`.  But in any case if a record is retrieved from the data source it must be possible to create the `OffsetManagerKey` and the key must uniquely identify the `native object`.  

When the `OffsetManagerEntry` is retrieved from the `OffsetManager` using the `OffsetManagerKey` there must be sufficient data to determine if the `naative object` has been processed.  Many of the decisions here will determine how often duplicate data will be returned.  The `OffsetManagerEntry`, by default, keeps track of the number of Kafka SourceRecords the `native object` produced.  In the case we are concerned about having a last modified date in the entry means that we can ask for data that has been modified since the last modified date.  In the end, the `NativeSourceData` handles the filtering in the `getNativeItemIterator()`.  

To assist in the processing a class extending `Context` may be implemented to track the additional data if needed.

It is important to document the cases where the connector will generate duplicate data.


### `native object` does not have the concept of an input stream.

If the `native object` does not have the concept of an input stream then the mapping of the `native object` properties onto a ValueAndSchema for the SourceRecord value and key must be done by a custom `Extractor`.  The `Extractor` had two methods that must be implemented.

 - `generateRecords()` which returns a stream of SchemaAndValue records for the values of the `EvolvingSourceRecord`.  This is the value that is sent to Kafka.
 - `generateKeyData()` which returns a single SchemaAndValue record for teh key of the `EvolvingSourceRecord`.  This is the key that is sent to Kafka.

In addition, the `AbstractSourceTask.lastEvolution()` method can be used to modify the key or value SchemaAndValue record. 