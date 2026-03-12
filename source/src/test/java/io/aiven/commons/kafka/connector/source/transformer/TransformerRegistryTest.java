/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.commons.kafka.connector.source.transformer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TransformerRegistryTest {
	private static int FEATURE1 = 1;
	private static int FEATURE2 = 1 << 1;
	private static int PRIVATE_FEATURE1 = 1 << (TransformerInfo.PRIVATE_FEATURE_SHIFT + 0);

	private TransformerInfo[] infos = {
			new TransformerInfo("no features", Transformer.class, TransformerInfo.FEATURE_NONE),
			new TransformerInfo("feat1", Transformer.class, FEATURE1),
			new TransformerInfo("feat2", Transformer.class, FEATURE2),
			new TransformerInfo("feat3", Transformer.class, FEATURE1 | FEATURE2),
			new TransformerInfo("feat4", Transformer.class, 4),
			new TransformerInfo("private", Transformer.class, PRIVATE_FEATURE1),
			new TransformerInfo("private + 1", Transformer.class, PRIVATE_FEATURE1 | FEATURE1),
			new TransformerInfo("private + 2", Transformer.class, PRIVATE_FEATURE1 | FEATURE2),
			new TransformerInfo("private + 3", Transformer.class, PRIVATE_FEATURE1 | FEATURE1 | FEATURE2),
			new TransformerInfo("private + 4", Transformer.class, PRIVATE_FEATURE1 | 4)};

	private TransformerRegistry underTest = TransformerRegistry.builder().add(infos).build();

	@Test
	void get() {
		assertThat(underTest.get("no features")).isNotNull();
		assertThat(underTest.get("missing")).isNull();
		assertThat(underTest.get("No features")).isNull();
	}

	@Test
	void getIgnoreCase() {
		assertThat(underTest.getIgnoreCase("no features")).isNotNull();
		assertThat(underTest.getIgnoreCase("missing")).isNull();
		assertThat(underTest.getIgnoreCase("No features")).isNotNull();

		TransformerRegistry registry2 = TransformerRegistry.builder().add(underTest)
				.add(new TransformerInfo("no features", Transformer.class, TransformerInfo.FEATURE_NONE)).build();
		assertThat(registry2.getIgnoreCase("no features")).isNotNull();
		assertThat(registry2.getIgnoreCase("missing")).isNull();
		assertThat(registry2.getIgnoreCase("No features")).isNotNull();

		assertThat(registry2.getIgnoreCase("no features").commonName()).isEqualTo("no features");
		assertThat(registry2.getIgnoreCase("No features").commonName()).isEqualTo("no features");

	}

	@Test
	void list() {
		assertThat(underTest.list()).containsExactlyInAnyOrder(infos);
	}

	@Test
	void any() {
		assertThat(underTest.any()).isNotNull();
		assertThat(TransformerRegistry.builder().build().any()).isNull();
	}

	@Test
	void anyFeature() {
		assertThat(underTest.anyFeature(FEATURE1).stream().map(TransformerInfo::commonName).toList())
				.containsExactlyInAnyOrder("feat1", "feat3", "private + 1", "private + 3");
		assertThat(underTest.anyFeature(FEATURE2).stream().map(TransformerInfo::commonName).toList())
				.containsExactlyInAnyOrder("feat2", "feat3", "private + 2", "private + 3");
		assertThat(underTest.anyFeature(PRIVATE_FEATURE1).stream().map(TransformerInfo::commonName).toList())
				.containsExactlyInAnyOrder("private", "private + 1", "private + 2", "private + 3", "private + 4");
		assertThat(underTest.anyFeature(FEATURE1 | FEATURE2).stream().map(TransformerInfo::commonName).toList())
				.containsExactlyInAnyOrder("feat1", "feat2", "feat3", "private + 1", "private + 2", "private + 3");
	}

	@Test
	void allFeatures() {
		assertThat(underTest.allFeatures(FEATURE1).stream().map(TransformerInfo::commonName).toList())
				.containsExactlyInAnyOrder("feat1", "feat3", "private + 1", "private + 3");
		assertThat(underTest.allFeatures(FEATURE2).stream().map(TransformerInfo::commonName).toList())
				.containsExactlyInAnyOrder("feat2", "feat3", "private + 2", "private + 3");
		assertThat(underTest.allFeatures(PRIVATE_FEATURE1).stream().map(TransformerInfo::commonName).toList())
				.containsExactlyInAnyOrder("private", "private + 1", "private + 2", "private + 3", "private + 4");
		assertThat(underTest.allFeatures(FEATURE1 | FEATURE2).stream().map(TransformerInfo::commonName).toList())
				.containsExactlyInAnyOrder("feat3", "private + 3");
	}

}
