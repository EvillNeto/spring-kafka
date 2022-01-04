package br.com.evilnetodev.kafka1.controllers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.services.KafkaSender;

@RestController
@RequestMapping("/message")
public class MessageController {

    @Autowired
    private KafkaSender kafkaSender;

    @GetMapping("/{message}")
    public String sendMenssage(@PathVariable(name = "message") String message) throws InterruptedException, ExecutionException{
        return kafkaSender.send(message);
    }
}
