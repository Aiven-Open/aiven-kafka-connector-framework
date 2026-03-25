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
# Architecture Overview

The kacka-common-connector-framework is intended to support Kafka connectors in both the sink and source roles.  All connectors should follow the basic structure introduced here.


## The modules

The framework is broken up into 3 modules under a common parent.  The modules are: common, sink, and source.

### Common module

The common module contains the code and documentation that is common to both the sink and source modules.  This includes common configuration options, helper libraries, and common test code.

In concrete implementations that use this framework, the commons module should include code to connect to the storage as well as test code to simulate that backend.

### Sink module

Contains the common code to implement our standard architecture.  The standard architecture accepts Kafka messages, groups them by various characteristics as directed by the configuration, and then writes the grouped records to the storage.

The framework establishes most of this while the concrete implementation provides the implementation to store the records.

### Source module

The source module contains the common code to pull data from storage and place it on the Kafka queue.  The framework contains the code to handle the Kafka polling as well as placing the data from storage into queues to be passed to Kafka.  

The concrete implementation provides an Iterator over the data keys in the storage layer as well as some tracking information to detect when specific keys have been written to Kafka.  The iterator design allows the backend to gather and filter keys before reporting them as available to be processed by Kafka.  In addition, the backend can continue to scan, returning empty iterators and then iterators that have data as new data arrives in the backend.  The concrete implementation need only focus on what data is available to send and being able to extract that data in a format that is readable by the framework.

## Source Architecture

The source connector sits between the Kafka environment on one side and the data storage on the other.  It must deal with the potential impedance mismatch between the two.  To do this the source connector creates a queue of records that are available to send to Kafka and sends them when Kafka polls.  The storage layer simply provides an iterator over the currently available data keys and the framework works through the iterator to retrieve the data and create Kafka recrods that it will send when Kafka polls.  if the backend has more data than Kafka is ready for the framework pauses until there is space in the queue again.  If the backend has no data to send it returns an empty iterator and the framework will delay and request again later.

In addition, Kafka records when it completed the ingestion of the records send by the framework,  The framework can then track which records have been send and ensure that it requests records after a specific point.  If the connector is stopped and restarted it will start by requesting the data after the last confirmed record.

## Sink Architecture

The sink architecture accepts records from Kafka, potentially splits them into separate groups based upon some data characteristics and then writes those grouped records to the storage engine to write.  The framework is responsible for detecting when the storage engine is overwhelmed and applying backpressure to Kafka to slow data transmission.

## Build Environment

This project will use the Maven framework.  The connector framework will comprise 4 Maven pom files:

 1. The "parent" pom.  This pom contains all the standard versioning and common plugins for the child poms.  All child poms will derive from this pom.  All children share the same version number as this pom.
 2. The "common" pom.  This pom is a maven module of the parent.  It is generally expected to produce a jar, but may itself be a "pom" type that produces multiple jars.
 3. The "source" pom. This pom is a maven module of the parent.  It is generally expected to produce a jar, but may itself be a "pom" type that produces multiple jars.  In general this pom produces the source and jar packaging for the source connector.  The versioning of the connector is controlled by the parent.
4. The "sink" pom.  This pom is a maven module of the parent.  It is generally expected to produce a jar, but may itself be a "pom" type that produces multiple jars.  In general this pom produces the source and jar packaging for the source connector.  The versioning of the connector is controlled by the parent.

Connectors that implement the connector framework are expected to use the same structure with commons providing common code for the implementation, like connecting to the storage engine, while the sink and source provide the concrete implementations to read and write the data. 

### Github actions

The connector framework and the connectors will be released to the master maven repository via github builds.  When a merge is accepted into the main branch on github a github action will commence a build with a SNAPSHOT release.  In this way chagnes to frameworks become available immediately.

During a version release a a github action to perform a release from a newly created tag.  This release will not be a snapshot and will be staged on the Sonatype servers until released by a developer.


