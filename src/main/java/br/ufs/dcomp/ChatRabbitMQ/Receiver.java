package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Receiver extends Thread {
    private Connection connection;
    private String queueName;

    public Receiver(Connection connection, String queueName) {
        this.connection = connection;
        this.queueName = queueName;
    }

    public Connection getConnection() {
        return connection;
    }

    public String getQueueName() {
        return queueName;
    }

    public void checkQueue(Channel channel) throws Exception {
        Consumer consumer = new DefaultConsumer(channel) {
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                // (21/09/2016 às 20:53) marciocosta diz:

                MensagemProto.Mensagem recMessage = MensagemProto.Mensagem.parseFrom(body);

                String sender = recMessage.getEmissor();
                String date = recMessage.getData();
                String hour = recMessage.getHora();
                String group = recMessage.getGrupo();
                MensagemProto.Conteudo recContent = recMessage.getConteudo();

                String type = recContent.getTipo();
                byte[] content = recContent.getCorpo().toByteArray();
                String filename = recContent.getNome();

                group = (group.length() > 0) ? ("#" + group) : group;
                
                if(type.equals("text/plain")) {
                    String text = new String(content);
                    System.out.printf("\n(%s às %s) %s%s diz: %s%n", date, hour, sender, group, text);
                }
                else {
//                    Path source = Paths.get("/home/ubuntu/chat/downloads/" + filename);
                    Path source = Paths.get("C:\\downloads\\" + filename);
                    Files.write(source, content, StandardOpenOption.CREATE_NEW);

                    System.out.printf("\n(%s às %s) Arquivo \"%s\" recebido de @%s%s%n !\n", date, hour, filename, sender, group);
                }
            }
        };
        //(queue-name, autoAck, consumer);
        channel.basicConsume(this.getQueueName(), true, consumer);
    }

    @Override
    public void run() {
        Channel channel;
        try {
            channel = this.getConnection().createChannel();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            this.checkQueue(channel);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}