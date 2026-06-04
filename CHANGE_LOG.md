## v0.3.0
### What is changed
 
 - Partial fix for #70
 - Merge pull request #68 from Aiven-Open/add-source-connector-integration-test
 - fix typos
 - fixed copyright date
 - fixed URLs
 - add implementations list
 - add implementations list
 - updated example as per review comment
 - updated example as per review comment
 - formatting and license corrections
 - changes as per review
 - Apply suggestions from code review
 - Merge pull request #69 from Aiven-Open/aindriu-aiven/csv-extractor-schema-fix
 - update documentation
 - updated to kafka-config version 0.2.0
 - remove dead code
 - fix conflicts
 - Update for upstream changes
 - Merge remote-tracking branch 'origin/aindriu-aiven/csv-extractor-schema-fix' into aindriu-aiven/csv-extractor-schema-fix
 - Remove the unused code in BeforeAll
 - Merge branch 'add-source-connector-integration-test' into aindriu-aiven/csv-extractor-schema-fix
 - Update as per review
 - Bump kafka-config from 0.1.0 to 0.2.0
 - Update kafka-testkit to 0.1.0
 - merged main
 - Update documentation
 - Bump aiven-commons from 3 to 4
 - working implementation
 - ionitial integration tests
 - Update from PR comments to remove dead code and irrelevant test code
 - Updates based on PR review
 - Add a test to deserialize the data using a JsonConverter
 - Allow users to specify the converter type they wish to use
 - Fix the schema that is sent with the data sothe jsonConverter can be used with the data
 - Merge pull request #66 from Aiven-Open/Add-source-documentation
 - fixed misnamed method in text
 - fixed typography issues and add Extractor and circular buffer info
 - updated documentation as per review
 - Apply suggestions from code review
 - removed unchanged files
 - updated documentation
 - Merge pull request #65 from Aiven-Open/release-0.2.0
 - Bump version to 0.3.0-SNAPSHOT
 
 
### Co-authored by
 
 - Aindriu Lavelle
 - Aindriú Lavelle
 - Claude Warren
 - github-actions[bot]
 
 
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
 
