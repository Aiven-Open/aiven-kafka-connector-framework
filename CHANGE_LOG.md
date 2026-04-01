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
 
