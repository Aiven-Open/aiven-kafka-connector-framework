/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.commons.kafka.connector.sink.output.csv;

import io.aiven.commons.kafka.connector.sink.output.SinkOutputStreamWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.function.Function;

final class CsvOutputStreamWriter implements SinkOutputStreamWriter {

    private CSVPrinter printer;

    private final List<Function<SinkRecord, Object>> writers;

    CsvOutputStreamWriter(final List<Function<SinkRecord, Object>> writers) {
        this.writers = writers;
    }

    @Override
    public void startWriting(OutputStream outputStream) throws IOException {
        // TODO select CSV format and potentially print headers.
        printer = new CSVPrinter(new OutputStreamWriter(outputStream), CSVFormat.RFC4180);
    }

    @Override
    public void writeOneRecord(final OutputStream outputStream, final SinkRecord record) throws IOException {
        List<Object> columns = writers.stream().map(writer -> writer.apply(record)).toList();
        printer.printRecord(columns);
    }

    @Override
    public void stopWriting(OutputStream outputStream) throws IOException {
        printer.close(true);
    }
}
