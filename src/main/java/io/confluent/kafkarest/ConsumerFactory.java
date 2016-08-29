package io.confluent.kafkarest;


import org.apache.kafka.clients.consumer.Consumer;

import java.util.Properties;

public interface ConsumerFactory<K, V> {

  Consumer<K, V> createConsumer();
}
