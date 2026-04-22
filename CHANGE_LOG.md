## v0.2.0
### What is changed
 
 - Merge pull request #63 from Aiven-Open/Aadjust-documentation
 - removed accidental paste
 - Update DistributionType.java
 - Potential fix for pull request finding 'Missing space in string literal'
 - Added hideNativeStartKey Added hideRingBufferSize
 - added DistributionType documentation
 - Ensure that resources are closed on stop() (#62)
 - Merge pull request #60 from Aiven-Open/Add-ALL-distributionType
 - Fixed as per review + added tests
 - Merge pull request #61 from Aiven-Open/fix-version-numbers
 - Merge branch 'main' into fix-version-numbers
 - Merge pull request #59 from Aiven-Open/fix_lookback_config
 - changed since version numbers to 0.1.0
 - Allows distribution types that accept all work.
 - allow lookback size of 0
 - Merge pull request #57 from Aiven-Open/release-0.1.0
 - Bump version to 0.2.0-SNAPSHOT
 - Release version 0.1.0
 - Update create_release.yml
 - Revert "Release version 0.1.0" (#56)
 - Update create_release.yml
 - Update create_release.yml
 - Update create_release.yml
 - Update create_release.yml
 - Merge pull request #55 from Aiven-Open/release-0.1.0
 - Bump version to 0.2.0-SNAPSHOT
 
 
### Co-authored by
 
 - Aindriú Lavelle
 - Claude Warren
 - github-actions[bot]
 
 
### Full Changelog
https://github.com/Aiven-Open/aiven-commons/compare/v0.1.0...v0.2.0
 
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
https://github.com/Aiven-Open/aiven-commons/compare/1db3bd9ee7eb156cdf39af703c0235a4268afdd7...v0.1.0
 
