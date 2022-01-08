package br.com.evilnetodev.kafka1.services;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);

    private Properties sendProperties = buildSendProperties();

    private String clientId = UUID.randomUUID().toString();

    private static String kafkaAdress = System.getenv("KAFKA_ADRESS");

    private Callback callback = (data, ex) -> {
        if (ex != null) {
            ex.printStackTrace();
            return;
        }
        LOG.info("sucesso enviando" + data.topic() + ":::partition" + data.partition() + "/ offset"
                + data.offset() + "/ timestamp" + data.timestamp());
    };
    
    public Properties getSendProperties() {
        return sendProperties;
    }

    /* Implementação do metodo de envio de mensagen */
    public String send(String message, String topic) throws InterruptedException, ExecutionException {

        var producer = new KafkaProducer<String, String>(getSendProperties());
        var record = new ProducerRecord<>(topic, UUID.randomUUID().toString(), message);
        producer.send(record, callback).get();
        producer.close();
        LOG.info("#######__Message sent__#######");
        return "success";
    }

    /* Implementação do metodo de leitura das mensagens não lidas */
    public List<String> read(String topic, String group) {

        var consumer = new KafkaConsumer<String, String>(readProperties(group));
        consumer.subscribe(Collections.singletonList(topic));
        var records = consumer.poll(Duration.ofMillis(100));
        List<String> messages = new ArrayList<>();
        if (!records.isEmpty()) {
            int count = 0;
            LOG.info("###########__ " + records.count() + " mensagem encontrada __##############");
            for (var record : records) {
                LOG.info("____________Processando mensagem " + count + "_____________");
                messages.add(record.value());
            }
        }
        consumer.close();
        return messages;
    }

    public void readLoop(String topic, String group) {

        var consumer = new KafkaConsumer<String, String>(readProperties(group));
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                int count = 0;
                LOG.info("###########__ " + records.count() + " __mensagem encontrada ##############");
                for (var record : records) {
                    LOG.info("______________Processando mensagem " + count + "_____________");
                    LOG.info("Message topic:     " + record.topic());
                    LOG.info("Message key:       " + record.key());
                    LOG.info("Message value:     " + record.value());
                    LOG.info("Message partition: " + record.partition());
                    LOG.info("Message offset:    " + record.offset());
                    LOG.info("______________Mensagem processada____________________");
                }
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                // ignore
                e.printStackTrace();
            }
        }
    }

    /* Cria as configurações de envio */
    private Properties readProperties(String group) {

        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAdress);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        return properties;
    }

    /* Cria as configurações de leitura */
    private static Properties buildSendProperties() {

        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAdress);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

}
