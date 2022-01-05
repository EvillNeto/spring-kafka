package br.com.services;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaService {

    private Properties sendProperties;

    private Properties readProperties;

    public KafkaService() {
        this.sendProperties = buildSendProperties();
        this.readProperties = buildReadProperties();
    }

    public String send(String message) throws InterruptedException, ExecutionException {

        var producer = new KafkaProducer<String, String>(getSendProperties());
        var record = new ProducerRecord<>("TESTE_MENSAGEM_1", message, message);
        producer.send(record, (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando" + data.topic() + ":::partition" + data.partition() + "/ offset"
                    + data.offset() + "/ timestamp" + data.timestamp());
        }).get();
        producer.close();
        return "success";
    }

    public List<String> read() {
        var consumer = new KafkaConsumer<String, String>(getReadProperties());
        consumer.subscribe(Collections.singletonList("TESTE_MENSAGEM_1"));
        var records = consumer.poll(Duration.ofMillis(100));
        List<String> messages = new ArrayList<>();
        if (!records.isEmpty()) {
            int count = 0;
            System.out.println("########### " + records.count() + " mensagem encontrada ##############");
            for (var record : records) {
                System.out.println("____________Processando mensagem " + count + "_____________");
                messages.add(record.value());
            }
        }
        consumer.close();
        return messages;
    }

    public Properties getSendProperties() {
        return sendProperties;
    }

    public void setSendProperties(Properties sendProperties) {
        this.sendProperties = sendProperties;
    }

    public Properties getReadProperties() {
        return readProperties;
    }

    public void setReadProperties(Properties readProperties) {
        this.readProperties = readProperties;
    }

    private Properties buildReadProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "0");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "0");

        return properties;
    }

    private static Properties buildSendProperties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

}
