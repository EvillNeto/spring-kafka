package br.com.evilnetodev.kafka1.forms;

public class MessageForm {

    private String message;

    private String topic;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

}
