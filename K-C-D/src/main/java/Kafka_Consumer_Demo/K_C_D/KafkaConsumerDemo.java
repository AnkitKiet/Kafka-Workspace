package Kafka_Consumer_Demo.K_C_D;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Hello world!
 *
 */
public class KafkaConsumerDemo {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "1");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");

		ObjectMapper objectMapper = new ObjectMapper();
		KafkaConsumer<String, JsonNode> consumer = new KafkaConsumer<String, JsonNode>(props);
		consumer.subscribe(Arrays.asList("my-topic"));
		while (true) {
			ConsumerRecords<String, JsonNode> records = consumer.poll(1000);
			for (ConsumerRecord<String, JsonNode> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
				try {
					Pojo[] objPojos = objectMapper.treeToValue(record.value(), Pojo[].class);
					for (int i = 0; i < objPojos.length; i++) {
						System.out.println("name : " + objPojos[i].getName());
						System.out.println("age : " + objPojos[i].getAge());
					}
				} catch (JsonProcessingException e) {
					System.out.println("Wrong Json Format");
				}

			}
		}
	}
}
