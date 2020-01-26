package Kafka_Producer_Demo.K_P_D;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class KafkaProducerDemo {
	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		ThreadProducer threadProducer = new ThreadProducer(producer);
		ThreadProducer threadProducer2 = new ThreadProducer(producer);
		threadProducer.start();
		threadProducer2.start();



	}
}
