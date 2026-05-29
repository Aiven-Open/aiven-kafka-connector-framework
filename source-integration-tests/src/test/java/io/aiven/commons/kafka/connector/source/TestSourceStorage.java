package io.aiven.commons.kafka.connector.source;

import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.kafka.connector.common.NativeInfo;
import io.aiven.commons.kafka.connector.source.extractor.ExtractorRegistry;
import io.aiven.commons.kafka.connector.source.impl.ExampleOffsetManagerEntry;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.commons.io.function.IOSupplier;
import org.apache.kafka.connect.connector.Connector;
import org.codehaus.plexus.util.FileUtils;
import org.codehaus.plexus.util.IOUtil;

/**
 * The implementation of SourceStorage for this set of tests. This example reads and writes to a
 * test directory, in a real case this class would read/write to the storage under test.
 */
public class TestSourceStorage implements SourceStorage<String, ByteBuffer> {
  private final ULID ulid = new ULID();
  // the root directory to write test data to.
  private final Path directory;

  private String workingTopic;

  /**
   * Constructor.
   *
   * @param directory the directory to write test data to.
   */
  TestSourceStorage(Path directory) {
    this.directory = directory;
  }

  /**
   * Gets the path to the test directory. This is specific to this implementation.
   *
   * @return the path to the test directory.
   */
  Path getTestDir() {
    return directory;
  }

  @Override
  public ExtractorRegistry supportedExtractors() {
    return ExtractorRegistry.STANDARD;
  }

  @Override
  public String createKey(String topic, int partition) {
    return String.format("%s/%s/%s", topic, partition, ulid.nextULID());
  }

  public OffsetManager.OffsetManagerKey createKey(final String nativeKey) {
    return () -> {
      String[] parts = nativeKey.split("/");
      return Map.of("topic", parts[0], "partition", parts[1], "ulid", parts[2]);
    };
  }

  @Override
  public WriteResult writeWithKey(String nativeKey, byte[] testDataBytes) {
    Path path = directory.resolve(nativeKey);
    File parent = path.toFile().getParentFile();
    if (!parent.exists()) {
      parent.mkdirs();
    }
    try (OutputStream out = Files.newOutputStream(path)) {
      if (testDataBytes != null) {
        out.write(testDataBytes);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new WriteResult(createKey(nativeKey), nativeKey);
  }

  @Override
  public Map<String, String> createConnectorConfig() {
    Map<String, String> result = new HashMap<>();
    result.put("example.dir", directory.toString());
    return result;
  }

  @Override
  public BiFunction<Map<String, Object>, Map<String, Object>, OffsetManager.OffsetManagerEntry>
      offsetManagerEntryFactory() {
    return (key, properties) -> {
      HashMap<String, Object> map = new HashMap<>();
      map.putAll(key);
      map.putAll(properties);
      return new ExampleOffsetManagerEntry(map);
    };
  }

  @Override
  public Class<? extends Connector> getConnectorClass() {
    return ExampleSourceConnector.class;
  }

  @Override
  public void createStorage(String topic) {
    workingTopic = topic;
    File topicDir = directory.resolve(topic).toFile();
    if (!topicDir.exists()) {
      topicDir.mkdirs();
    }
  }

  @Override
  public void removeStorage() {
    File topicDir = directory.resolve(workingTopic).toFile();
    if (topicDir.exists()) {
      try {
        FileUtils.cleanDirectory(topicDir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    workingTopic = null;
  }

  @Override
  public List<? extends NativeInfo<String, ByteBuffer>> getNativeInfo() {
    return getNativeInfo(null);
  }

  public List<? extends NativeInfo<String, ByteBuffer>> getNativeInfo(String startFrom) {
    String[] parts = startFrom == null ? new String[] {"", "", ""} : startFrom.split("/");
    List<NativeInfo<String, ByteBuffer>> result = new ArrayList<>();
    try {
      List<File> topics;
      try (Stream<Path> paths = Files.list(directory)) {
        topics =
            paths
                .map(Path::toFile)
                .filter(
                    file ->
                        file.isDirectory()
                            && !file.getName().startsWith(".")
                            && file.getName().compareTo(parts[0]) >= 0)
                .toList();

        for (File topic : topics) {
          Path topicPath = directory.resolve(topic.getName());
          List<File> partitions;
          try (Stream<Path> partitionPaths = Files.list(topicPath)) {
            partitions =
                partitionPaths
                    .map(Path::toFile)
                    .filter(
                        file ->
                            file.isDirectory()
                                && !file.getName().startsWith(".")
                                && file.getName().compareTo(parts[1]) >= 0)
                    .toList();
          }
          for (File partition : partitions) {
            Path partitionPath = topicPath.resolve(partition.getName());
            try (Stream<Path> filePaths = Files.list(partitionPath)) {
              filePaths
                  .map(Path::toFile)
                  .filter(file -> file.isFile() && file.getName().compareTo(parts[2]) >= 0)
                  .forEach(
                      ulid -> {
                        try (FileReader reader = new FileReader(ulid)) {
                          String key =
                              String.format(
                                  "%s/%s/%s", topic.getName(), partition.getName(), ulid.getName());
                          result.add(
                              new NativeInfo<>(key, ByteBuffer.wrap(IOUtil.toByteArray(reader))));
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      });
            }
          }
        }
      }
      result.forEach(System.out::println);
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public IOSupplier<InputStream> getInputStream(String nativeKey) {
    return () -> new FileInputStream(directory.resolve(nativeKey).toFile());
  }

  @Override
  public String defaultPrefix() {
    return "";
  }
}
