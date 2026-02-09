package io.aiven.commons.kafka.connector.source.testFixture.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.aiven.commons.kafka.connector.source.testFixture.format.AvroTestDataFixture.generateAvroData;

public class ByteArrayDataFixture {

    /**
     * Generates a byte array containing the specified number of records.
     *
     * @param numBytes
     *            the numer of records to generate
     * @return A byte array containing the specified number of records.
     * @throws IOException
     *             if the Avro records can not be serialized.
     */
    public static byte[] generateByteData(final int numBytes) {
        return generateByteData(numBytes, 0);
    }

    public static byte[] generateByteData(final int numBytes, int offset) {
        byte[] result = new byte[numBytes];
        for (int i=offset; i < numBytes + offset; i++) {
            result[i] = (byte) (i % Byte.MAX_VALUE);
        }
        return result;
    }
}
