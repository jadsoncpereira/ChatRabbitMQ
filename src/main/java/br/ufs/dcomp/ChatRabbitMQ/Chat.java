package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import java.io.IOException;

import com.google.protobuf.ByteString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;


/**
 * compile: mvn clean compile assembly:single
 * execute: java -jar <jarfile> <host> <username> <password>
 **/
public class Chat extends Thread {
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private String queueName = "";

    public Chat(String connHost, String connUser, String connPass) throws Exception {
        this.factory = new ConnectionFactory();
        this.factory.setHost(connHost);
        //this.factory.setPort(80);
        this.factory.setUsername(connUser);
        this.factory.setPassword(connPass);
        this.factory.setVirtualHost("/");

        System.out.println("> connecting...");
        this.connection = this.factory.newConnection();
        System.out.println("> creating channel...");
        this.channel = this.connection.createChannel();
    }

    public Connection getConnection() {
        return connection;
    }

    public Channel getChannel() {
        return channel;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void run() {
        Receiver receiver, fileReceiver;
        Scanner scanner = new Scanner(System.in);
        System.out.print("User: ");
        try {
            String queueName = scanner.nextLine();
            this.setQueueName(queueName);
            //(queue-name, durable, exclusive, auto-delete, params);
            this.getChannel().queueDeclare(this.getQueueName(),false,false,false,null);
            
            Sender sender = new Sender(this.getConnection(), this.getQueueName());
            sender.start();
            
            while(true) {
                Thread.sleep(1000);
                receiver = new Receiver(this.getConnection(), this.getQueueName());
                receiver.start();
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] arg) throws Exception {
        if(arg.length != 3) {
            System.out.println("java -jar <jarfile> <host> <username> <password>");
            return;
        }

        Chat chat = new Chat(arg[0], arg[1], arg[2]);
        chat.run();
    }
}