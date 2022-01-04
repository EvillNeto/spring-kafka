package br.com.services;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

@Service
public class KafkaSender {

    public String send(String message) throws InterruptedException, ExecutionException {

        var producer = new KafkaProducer<String,String>(properties());
        var record = new ProducerRecord<>("TESTE_MENSAGEM_1", message, message);
        producer.send(record, (data, ex)-> {
            if(ex != null){
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando" + data.topic() + ":::partition" + data.partition() + "/ offset" + data.offset() + "/ timestamp" + data.timestamp());
        }).get();
        return "success";
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
