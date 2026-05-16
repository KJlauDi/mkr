package com.example.kafkademo;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ConsumerApp {
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:29092");
    private static final String REQUEST_TOPIC = "demo-requests";
    private static final String RESPONSE_TOPIC = "demo-responses";
    private static final String CONSUMER_GROUP = "demo-responder-group";

    public static void main(String[] args) throws Exception {
        createTopics();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties());
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {

            consumer.subscribe(Collections.singletonList(REQUEST_TOPIC));
            System.out.printf("Чекаю запитів у '%s'.%n", REQUEST_TOPIC);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    String requestValue = record.value();
                    String correlationId = getCorrelationId(record);
                    String responseValue;

                    try {
                        int[] range = parseRange(requestValue);
                        int start = range[0];
                        int finish = range[1];

                        System.out.printf("<- Отримано запит: start=%d finish=%d%n", start, finish);
                        responseValue = String.valueOf(averageCollatzSteps(start, finish));
                    } catch (Exception ex) {
                        System.out.printf("Помилка обробки запиту '%s': %s%n", requestValue, ex.getMessage());
                        responseValue = "error: " + ex.getMessage();
                    }

                    ProducerRecord<String, String> response = new ProducerRecord<>(RESPONSE_TOPIC, responseValue);
                    response.headers().add("correlation-id", correlationId.getBytes(StandardCharsets.UTF_8));
                    producer.send(response).get();
                    producer.flush();
                    consumer.commitSync(Collections.singletonMap(
                            new TopicPartition(record.topic(), record.partition()),
                            new org.apache.kafka.clients.consumer.OffsetAndMetadata(record.offset() + 1)
                    ));

                    System.out.printf("-> Надіслано відповідь: avgSteps=%s%n", responseValue);
                }
            }
        }
    }

    private static void createTopics() throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            List<NewTopic> topics = List.of(
                    new NewTopic(REQUEST_TOPIC, 1, (short) 1),
                    new NewTopic(RESPONSE_TOPIC, 1, (short) 1)
            );

            try {
                adminClient.createTopics(topics).all().get();
                System.out.println("Топіки створено.");
            } catch (ExecutionException ex) {
                if (ex.getCause() instanceof TopicExistsException) {
                    System.out.println("Топіки вже існують.");
                } else {
                    throw ex;
                }
            }
        }
    }

    private static Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }

    private static Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static String getCorrelationId(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("correlation-id");
        if (header == null) {
            return "";
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static int[] parseRange(String value) {
        String[] parts = value.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Запит має бути у форматі 'start,finish'.");
        }

        int start = Integer.parseInt(parts[0].trim());
        int finish = Integer.parseInt(parts[1].trim());

        if (start <= 0 || finish <= 0) {
            throw new IllegalArgumentException("start і finish мають бути додатними числами.");
        }

        if (start > finish) {
            throw new IllegalArgumentException("start має бути меншим або рівним finish.");
        }

        return new int[]{start, finish};
    }

    private static int collatzSteps(long number) {
        int steps = 0;

        while (number != 1) {
            if (number % 2 == 0) {
                number /= 2;
            } else {
                number = number * 3 + 1;
            }
            steps++;
        }

        return steps;
    }

    private static int averageCollatzSteps(int start, int finish) {
        long totalSteps = 0;
        int count = finish - start + 1;

        for (int number = start; number <= finish; number++) {
            totalSteps += collatzSteps(number);
        }

        return (int) Math.round((double) totalSteps / count);
    }
}
