import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;

public class Consumer {

   private static final String EXCHANGE_NAME = "Exchange";

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try (
            Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            Scanner scanner = new Scanner(System.in);
        )    {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            String queueName = channel.queueDeclare().getQueue();
            System.out.println("Enter 'sub_topic TopicName' to subscribe or 'del_topic TopicName' to unsubscribe");
            DeliverCallback deliverCallback = new DeliverCallback() {
                @Override
                public void handle(String s, Delivery delivery) throws IOException {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println("Received a new article on topic "
                            + delivery.getEnvelope().getRoutingKey() + ": "
                            +message);
                }
            };

            while (true) {
                if (scanner.hasNextLine()) {
                    String str = scanner.nextLine().trim().toLowerCase();
                    if (str.equals("exit")) {
                        break;
                    }
                    String[] arrayString = str.split(" ");
                    String topic = arrayString[arrayString.length - 1];
                    String routingKey = topic + ".#";
                    if (str.equals("del_topic " + topic)) {
                        channel.queueUnbind(queueName, EXCHANGE_NAME, routingKey);
                        System.out.println("You are unsubscribe topic: " + topic);
                        System.out.println("Enter 'sub_topic TopicName' to subscribe or 'del_topic TopicName' to unsubscribe");
                    }
                    if (str.equals("sub_topic " + topic)) {
                        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
                        System.out.println("You are subscribe on topic " + topic);
                        CancelCallback cancelCallback = new CancelCallback() {
                            @Override
                            public void handle(String s) throws IOException {

                            }
                        };
                        channel.basicConsume(queueName, true, deliverCallback, cancelCallback);
                        System.out.println("Enter 'sub_topic TopicName' to subscribe or 'del_topic TopicName' to unsubscribe");
                    }
                }
            }
        }
    }
}
