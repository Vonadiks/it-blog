import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class Producer {

    private static final String EXCHANGE_NAME = "Exchange";

    public static void main(String[] args) throws Exception{
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try (
                Connection connection = connectionFactory.newConnection();
                Channel channel = connection.createChannel();
                Scanner scanner = new Scanner(System.in);
        ) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            String topic = "";
            String message = "NewArticle";
            System.out.println("Enter message topic");
            while (true) {
                if (scanner.hasNextLine()){
                    topic = scanner.nextLine();
                    if (topic.trim().toLowerCase().equals("exit")){
                        break;
                    }
                    channel.basicPublish(EXCHANGE_NAME, topic, null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println("New publication on topic " + topic + ": " + message);
                }
                else {
                    System.out.println("Incorrect message");
                }
                System.out.println("Enter message topic: ");
            }
        }
    }
}
