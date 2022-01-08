package br.com.evilnetodev.kafka1.parallelApplications;

import br.com.evilnetodev.kafka1.services.KafkaService;


/* Classe JAva padrão que escuta o topico informado 
paralelamente com outras instancias do mesmo afim 
de testar o paralelismo fornecido pelo apache-kafka */
public class ParallelConsumer {
    
    public static void main(String[] args) {
        var kafka = new KafkaService();
        kafka.readLoop("TESTE_MENSAGEM_1", "parallel");
        return;
    }
}
