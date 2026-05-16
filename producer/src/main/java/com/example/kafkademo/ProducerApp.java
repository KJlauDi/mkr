package com.example.kafkademo;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
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
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerApp {
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:29092");
    private static final String REQUEST_TOPIC = "demo-requests";
    private static final String RESPONSE_TOPIC = "demo-responses";
    private static final int START = 10;
    private static final int FINISH = 100;
    private static final long RESPONSE_TIMEOUT_SECONDS = 3000;

    public static void main(String[] args) throws Exception {
        createTopics();

        String correlationId = UUID.randomUUID().toString();

        try (KafkaConsumer<String, String> responseConsumer = new KafkaConsumer<>(consumerProperties(correlationId));
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {

            responseConsumer.subscribe(Collections.singletonList(RESPONSE_TOPIC));
            waitForAssignment(responseConsumer);

            String requestValue = START + "," + FINISH;
            ProducerRecord<String, String> request = new ProducerRecord<>(REQUEST_TOPIC, requestValue);
            request.headers().add("correlation-id", correlationId.getBytes(StandardCharsets.UTF_8));
            producer.send(request).get();
            producer.flush();

            System.out.printf("-> Запит надіслано: start=%d finish=%d (id=%s)%n", START, FINISH, correlationId);

            String avgSteps = waitForResponse(responseConsumer, correlationId);
            System.out.printf("<- Отримано відповідь: avgSteps=%s%n", avgSteps);
            System.out.println("Готово. Контейнер живе.");

            while (true) {
                Thread.sleep(60_000);
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

    private static Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static Properties consumerProperties(String correlationId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-producer-" + correlationId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }

    private static void waitForAssignment(KafkaConsumer<String, String> consumer) {
        long deadline = System.currentTimeMillis() + 30_000;

        while (consumer.assignment().isEmpty()) {
            consumer.poll(Duration.ofMillis(200));

            if (System.currentTimeMillis() > deadline) {
                throw new RuntimeException("Не вдалося підписатися на топік відповідей.");
            }
        }
    }

    private static String waitForResponse(KafkaConsumer<String, String> consumer, String correlationId) {
        long deadline = System.currentTimeMillis() + RESPONSE_TIMEOUT_SECONDS * 1000;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                Header header = record.headers().lastHeader("correlation-id");
                if (header == null) {
                    continue;
                }

                String messageCorrelationId = new String(header.value(), StandardCharsets.UTF_8);
                if (correlationId.equals(messageCorrelationId)) {
                    return record.value();
                }
            }
        }

        throw new RuntimeException("Відповідь не отримано за відведений час.");
    }
}
