// package io.aiven.commons.kafka.connector.source;
//
// import io.aiven.commons.kafka.connector.common.NativeInfo;
// import io.aiven.commons.kafka.connector.source.integration.AbstractSourceIntegrationBase;
// import io.aiven.commons.kafka.connector.source.integration.SourceStorage;
// import io.aiven.commons.kafka.connector.source.task.Context;
// import org.junit.jupiter.api.AfterEach;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
//
// import java.nio.charset.StandardCharsets;
// import java.time.Duration;
// import java.util.ArrayList;
// import java.util.Iterator;
// import java.util.List;
// import java.util.Optional;
// import java.util.SortedSet;
// import java.util.TreeSet;
// import java.util.function.Function;
//
// import static org.assertj.core.api.Assertions.assertThat;
//
// public abstract class AbstractNativeSourceDataIntegrationTest<K extends Comparable<K>, N>
//		extends
//			AbstractSourceIntegrationBase<K, N> {
//	private NativeSourceData<K> underTest;
//	private static final Duration writeTimeout = Duration.ofSeconds(5);
//
//	protected AbstractNativeSourceDataIntegrationTest() {
//		super();
//	}
//
//	/**
//	 * Gets the NativeSourceData under test.
//	 *
//	 * @return an instance of the NativeSourceData.
//	 */
//	protected abstract NativeSourceData<K> getNativeSourceData();
//
//	@BeforeEach
//	void createStorage() {
//		getSourceStorage().createStorage();
//		underTest = getNativeSourceData();
//	}
//
//	@AfterEach
//	void removeStorage() {
//		getSourceStorage().removeStorage();
//	}
//
//	@Test
//	void getSourceName() {
//		assertThat(underTest.getSourceName()).isNotEmpty();
//	}
//
//	@Test
//	void getNativeItemStream() {
//		final String topic = getTopic();
//
//		final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
//		final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
//
//		final List<SourceStorage.WriteResult<K>> writeResults = new ArrayList<>();
//		// write 4 objects
//		writeResults.add(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0));
//		writeResults.add(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0));
//		writeResults.add(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1));
//		writeResults.add(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1));
//
//		// verify the records were written to storage.
//		waitForStorage(writeTimeout, () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
//				writeResults.stream().map(SourceStorage.WriteResult::getNativeKey).toList());
//
//		final TreeSet<K> expected = new TreeSet<>(
//				writeResults.stream().map(SourceStorage.WriteResult::getNativeKey).toList());
//		List<K> actual =
// underTest.getNativeItemStream(null).map(AbstractSourceNativeInfo::nativeKey).toList();
//		assertThat(actual).containsExactlyElementsOf(expected);
//
//		// get an offset that is not the beginning.
//		K startKey = expected.iterator().next();
//		actual =
// underTest.getNativeItemStream(startKey).map(AbstractSourceNativeInfo::nativeKey).toList();
//		// some clients will return the key others will return the first value larger
//		// than the key.
//		boolean inclusive = actual.size() == expected.size();
//		final SortedSet<K> newExpected = expected.tailSet(startKey, inclusive);
//		assertThat(actual).containsExactlyElementsOf(newExpected);
//	}
//
//	@Test
//	void offsetManagerEntry() {
//		final String topic = getTopic();
//		K nativeKey = createKey(topic, 1);
//		Context context = new Context(nativeKey);
//
//		OffsetManager.OffsetManagerEntry entry1 = underTest.createOffsetManagerEntry(context);
//		OffsetManager.OffsetManagerEntry entry2 =
// underTest.createOffsetManagerEntry(entry1.getProperties());
//		assertThat(entry1.getProperties()).containsExactlyInAnyOrderEntriesOf(entry2.getProperties());
//
//		OffsetManager.OffsetManagerKey key1 = entry1.getManagerKey();
//		OffsetManager.OffsetManagerKey key2 = entry2.getManagerKey();
//		assertThat(key1.getPartitionMap()).containsExactlyEntriesOf(key2.getPartitionMap());
//	}
//
//	@Test
//	void getIterator() {
//		final String topic = getTopic();
//		final String testData1 = "Hello, Kafka Connect S3 Source! object 1";
//		final String testData2 = "Hello, Kafka Connect S3 Source! object 2";
//		final Function<Iterator<EvolvingSourceRecord>, List<K>> readIterator = iter -> {
//			List<K> actual = new ArrayList<>();
//			while (iter.hasNext()) {
//				K key = (K) iter.next().getNativeKey();
//				actual.add(key);
//				underTest.recordNativeKeyFinished();
//			}
//			return actual;
//		};
//
//		final List<SourceStorage.WriteResult<K>> writeResults = new ArrayList<>();
//		// write 2 objects
//		writeResults.add(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 0));
//		writeResults.add(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 0));
//		// verify the records were written to storage.
//		waitForStorage(writeTimeout, () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
//				writeResults.stream().map(SourceStorage.WriteResult::getNativeKey).toList());
//
//		List<K> expected = writeResults.stream().map(SourceStorage.WriteResult::getNativeKey).toList();
//		Iterator<EvolvingSourceRecord> iter = underTest.getIterator(context -> true);// accept all
// records
//		List<K> actual = readIterator.apply(iter);
//		assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
//
//		K startKey = actual.get(actual.size() - 1);
//
//		// add more records and verify the iterator picks them up.
//		writeResults.clear();
//		writeResults.add(write(topic, testData1.getBytes(StandardCharsets.UTF_8), 1));
//		writeResults.add(write(topic, testData2.getBytes(StandardCharsets.UTF_8), 1));
//		waitForStorage(writeTimeout, () -> getNativeInfo().stream().map(NativeInfo::nativeKey).toList(),
//				writeResults.stream().map(SourceStorage.WriteResult::getNativeKey).toList());
//		expected = writeResults.stream().map(SourceStorage.WriteResult::getNativeKey).toList();
//
//		// calling getIterator again should only read the latest data and perhaps the
//		// last key.
//		iter = underTest.getIterator(context -> true);
//		actual = readIterator.apply(iter);
//		if (actual.size() != 2) {
//			// the client returns the key.
//			expected = new ArrayList<>(expected);
//			expected.add(startKey);
//		}
//		assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
//	}
//
//	@Test
//	void nativeKeyRoundtrip() {
//		final Optional<NativeSourceData.KeySerde<K>> keySerde = underTest.getNativeKeySerde();
//		keySerde.ifPresent(serde -> {
//			final String topic = getTopic();
//			final K nativeKey = createKey(topic, 1);
//			final String keyString = serde.toString(nativeKey);
//			final K unparsedKey = serde.fromString(keyString);
//			assertThat(unparsedKey.compareTo(nativeKey)).isEqualTo(0);
//		});
//	}
//
//	@Test
//	void offsetManagerKey() {
//		final String topic = getTopic();
//		final K nativeKey = createKey(topic, 1);
//		OffsetManager.OffsetManagerKey offsetManagerKey1 = underTest.getOffsetManagerKey(nativeKey);
//		OffsetManager.OffsetManagerKey offsetManagerKey2 = underTest.getOffsetManagerKey(nativeKey);
//
//	assertThat(offsetManagerKey1.getPartitionMap()).containsExactlyEntriesOf(offsetManagerKey2.getPartitionMap());
//	}
//
// }
