package br.com.evilnetodev.kafka1.controllers;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.evilnetodev.kafka1.services.KafkaService;


@RestController
@RequestMapping("/message")
public class MessageController {

    @Autowired
    private KafkaService kafkaService;

    /* 
    Devolve todas as mensagens não lidas ja enviadas
     */
    @GetMapping
    public List<String> readMenssagens(){
        return kafkaService.read();
    }

    /* 
    Envia uma mensagem para o kafka
    */
    @PostMapping("/{message}")
    public String sendMenssage(@PathVariable(name = "message") String message) throws InterruptedException, ExecutionException{
        return kafkaService.send(message);
    }
}
