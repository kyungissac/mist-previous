package edu.snu.mist.utils;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public final class AvroSerializer {

  public static <T extends SpecificRecord> String avroToString(final T avroObject, final Class<T> avroObjectClass) {
    final DatumWriter<T> datumWriter = new SpecificDatumWriter<>(avroObjectClass);
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroObject.getSchema(), out);
      datumWriter.write(avroObject, encoder);
      encoder.flush();
      out.close();
      return out.toString();
    } catch (final IOException ex) {
      throw new RuntimeException("Unable to serialize " + avroObjectClass.getName(), ex);
    }
  }

  public static <T extends SpecificRecord> T avroFromString(final String serializedObject,
                                                            final Schema schema,
                                                            final Class<T> avroObjectClass) {
    try {
      final Decoder decoder =
          DecoderFactory.get().jsonDecoder(schema, serializedObject);
      final SpecificDatumReader<T> reader = new SpecificDatumReader<>(avroObjectClass);
      return reader.read(null, decoder);
    } catch (final IOException ex) {
      throw new RuntimeException("Unable to deserialize logical plan", ex);
    }
  }
}
