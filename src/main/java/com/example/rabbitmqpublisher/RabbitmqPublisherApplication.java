package com.example.rabbitmqpublisher;

import com.google.common.primitives.Longs;
import com.rabbitmq.client.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class RabbitmqPublisherApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(RabbitmqPublisherApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String queueName = "delay.q1";
        String exchangeName = "delay.exchange1";

        channel.queueDeclare(queueName, true, false, false, null);

        long x = System.nanoTime();
        for (int i = 0; i < 100_100L; i++) {
            Long message = System.nanoTime() + 10_000_000_000L;
            Map<String, Object> headers = new HashMap<>();
            headers.put("x-delay", 10_000);
            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties().builder().headers(headers).build();
            channel.basicPublish(exchangeName, queueName, basicProperties, Longs.toByteArray(message));
        }

        System.out.println("time = " + NumberFormat.getInstance().format(System.nanoTime() - x));
        channel.close();
        connection.close();
    }
}
