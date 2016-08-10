package io.confluent.kafkarest.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides conversion of byte[] to JSON.
 */
public class JsonConverter {

  private static final Logger log = LoggerFactory.getLogger(JsonConverter.class);

  private static ObjectMapper objectMapper = new ObjectMapper();

  public static Object deserializeJson(byte[] data) {
    try {
      return data == null ? null : objectMapper.readValue(data, Object.class);
    } catch (Exception e) {
      throw new ConversionException("Failed to convert byte[] to JSON: " + e.getMessage());
    }
  }
}
