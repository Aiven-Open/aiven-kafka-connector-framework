## v0.4.0
### What is changed
 
 - Merge pull request #93 from Aiven-Open/add-spotless
 - Merge pull request #92 from Aiven-Open/Update-to-commons-v5
 - add spotless plugin
 - cleaned up versions
 - bumped testkit to 0.2.0
 - changed to temurin from adopt java
 - updated versions
 - Merge pull request #91 from Aiven-Open/update-documentation
 - update documentation and fix typos
 - Merge pull request #90 from Aiven-Open/update-context-documentation
 - Updated javadoc and site documents
 - Merge pull request #89 from Aiven-Open/remove-unwanted-log-entry
 - Do not emit log entry when native start key is not set.  Fix #88
 - Merge pull request #87 from Aiven-Open/fix-AMQP-requirements
 - Update source/src/main/java/io/aiven/commons/kafka/connector/source/config/SourceConfigFragment.java
 - Update source/src/main/java/io/aiven/commons/kafka/connector/source/config/SourceConfigFragment.java
 - Potential fix for pull request finding 'Missing Override annotation'
 - Changes in support of Context builder
 - Made Context immutable -- BREAKING CHANGE
 - made Extractor optional
 - Add NativeSourceData.initializeRecordFunction()
 - Merge pull request #86 from Aiven-Open/fix-offset-manager-key
 - fixed test issues
 - Update source/src/main/java/io/aiven/commons/kafka/connector/source/OffsetManager.java
 - Update source/src/main/java/io/aiven/commons/kafka/connector/source/OffsetManager.java
 - Converted OffsetManagerKey from Interface to Class -- this is a BREAKING change. The new	implementation uses a SortedMap	so that	all keys are listed in lexical order.
 - Merge pull request #83 from Aiven-Open/add-headers-to-evolving-record
 - fixed null pointer issue
 - Merge pull request #84 from Aiven-Open/modify-template-framework
 - cleaned up testing
 - applied spotless
 - Added timstamp and headers to EvolvingSourceRecord
 - updated template processing and testing
 - Merge pull request #81 from Aiven-Open/migrate-to-aiven-commons-5
 - updated to aiven commons v5-SNAPSHOT
 - Merge pull request #78 from Aiven-Open/claudenw-fix-release-issue
 - Update create_release.yml
 - Merge pull request #77 from Aiven-Open/release-0.3.0
 - Bump version to 0.4.0-SNAPSHOT
 - Release version 0.3.0
 - Merge pull request #76 from Aiven-Open/Update-test-javadoc-for-release
 - Update test javadoc
 - Merge pull request #75 from Aiven-Open/revert-74-release-0.3.0
 - Revert "Release version 0.3.0"
 - Merge pull request #74 from Aiven-Open/release-0.3.0
 - Bump version to 0.4.0-SNAPSHOT
 
 
### Co-authored by
 
 - Aindriú Lavelle
 - Claude Warren
 - github-actions[bot]
 
 
### Full Changelog
https://github.com/Aiven-Open/aiven-kafka-connector-framework/compare/v0.3.0...v0.4.0
 
## v0.3.0
### What is changed
 
 - Partial fix for #70
 - Merge pull request #68 from Aiven-Open/add-source-connector-integration-test
 - Merge pull request #69 from Aiven-Open/aindriu-aiven/csv-extractor-schema-fix
 - Bump kafka-config from 0.1.0 to 0.2.0
 - Update kafka-testkit to 0.1.0
 - Update documentation
 - Bump aiven-commons from 3 to 4
 - initial integration tests
 - Merge pull request #66 from Aiven-Open/Add-source-documentation
 - fixed misnamed method in text
 - fixed typography issues and add Extractor and circular buffer info 

 
### Co-authored by
 
 - Aindriu Lavelle
 - Claude Warren
 
 
### Full Changelog
https://github.com/Aiven-Open/aiven-kafka-connector-framework/compare/v0.2.0...v0.3.0
 
## v0.2.0
### What is changed
 
 - Added hideNativeStartKey Added hideRingBufferSize
 - added DistributionType documentation
 - Ensure that resources are closed on stop() (#62)
 - changed since version numbers to 0.1.0
 - Allows distribution types that accept all work.
 - allow lookback size of 0
 - 
 
### Co-authored by
 
 - Aindriú Lavelle
 - Claude Warren
 
 
### Full Changelog
https://github.com/Aiven-Open/aiven-kafka-connector-framework/compare/v0.1.0...v0.2.0
 
## v0.1.0
### What is changed

 - Update the headers behaviour in the CSVExtractor (#47)
 - Add option to hide some config from documentation (#45)
 - changed NativeSourceData.getNativeItemStream() to NativeSourceData.getNativeItemIterator()
 - renamed Transformer to Extractor
 - changes to take advantage of RingBuffer comparator usage
 - ensured that Key is defined as comparable when returned
 - Added decompression to transformers
 - Add transformer validator and descriptions (#32)
 - Add a lastEvolution method to the AbstractSourceTask
 - added TemplateValidator
 - Add TransformerRegistry (#27)
 - added Lookback and updated processing
 - Update CsvTransformer.java
 - Update ConnectorCommonConfigFragment.java
 - Aindriu aiven/update csv transformer (#18)
 - added RELAX_SCHEMES to add http as a valid URL schem for schema registries
 - extended comparable to AbstractSourceNativeInfo
 - Made NativeInfo implement Comparable
 - fixed Csv data generation issues and cleaned up tests
 - created AbstractSourceTask and test
 - reworked Transformer to support non InputStream transforms
 - Added SinceInfo implementation
 - Initial architecture documentation
 
 
### Co-authored by
 
 - Aindriu Lavelle
 - Claude Warren
  
### Full Changelog
https://github.com/Aiven-Open/aiven-kafka-connector-framework/compare/1db3bd9ee7eb156cdf39af703c0235a4268afdd7...v0.1.0
 
