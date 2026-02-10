package io.aiven.commons.kafka.connector.source.impl;

import io.aiven.commons.kafka.connector.source.AbstractSourceRecordIterator;
import io.aiven.commons.kafka.connector.source.AbstractSourceTask;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.testFixture.format.JsonTestDataFixture;
import io.aiven.commons.kafka.connector.source.transformer.ByteArrayTransformer;
import io.aiven.commons.timing.BackoffConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ExampleSourceTask extends AbstractSourceTask<String, ExampleNativeItem, ExampleOffsetManagerEntry, ExampleSourceRecord> {

    @Override
    protected AbstractSourceRecordIterator<String, ExampleNativeItem, ExampleOffsetManagerEntry, ExampleSourceRecord> getIterator(SourceCommonConfig config) {
        try {
            List<ExampleNativeItem> lst = new ArrayList<>();
            lst.add(new ExampleNativeItem("first 10", JsonTestDataFixture.generateJsonData(10)));
            lst.add(new ExampleNativeItem("second 10", JsonTestDataFixture.generateJsonData(10,10)));
            lst.add(new ExampleNativeItem("third 10", JsonTestDataFixture.generateJsonData(20,10)));
            ExampleNativeSourceData sourceData = new ExampleNativeSourceData() {
                @Override
                public Stream<ExampleNativeItem> getNativeItemStream(String offset) {
                    return lst.stream();
                }
            };

            return new AbstractSourceRecordIterator<>(config, new OffsetManager<>(context), sourceData);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected SourceCommonConfig configure(Map<String, String> props) {
        SourceConfigFragment.Setter setter = SourceConfigFragment.setter(props);
        setter.transformerClass(ByteArrayTransformer.class);
        return new SourceCommonConfig(new SourceCommonConfig.SourceCommonConfigDef(), props);
    }

    @Override
    protected void closeResources() {

    }

    @Override
    public String version() {
        return "";
    }
}
