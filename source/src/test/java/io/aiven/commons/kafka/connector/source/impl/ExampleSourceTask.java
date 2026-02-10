package io.aiven.commons.kafka.connector.source.impl;

import io.aiven.commons.kafka.connector.source.AbstractSourceRecordIterator;
import io.aiven.commons.kafka.connector.source.AbstractSourceTask;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.commons.kafka.connector.source.transformer.ByteArrayTransformer;
import io.aiven.commons.timing.BackoffConfig;

import java.util.Map;

public class ExampleSourceTask extends AbstractSourceTask<String, ExampleNativeItem, ExampleOffsetManagerEntry, ExampleSourceRecord> {

    @Override
    protected AbstractSourceRecordIterator<String, ExampleNativeItem, ExampleOffsetManagerEntry, ExampleSourceRecord> getIterator(SourceCommonConfig config, BackoffConfig backOffConfig) {

        return new AbstractSourceRecordIterator<>(config, new OffsetManager<>(context), new ExampleNativeSourceData());
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
