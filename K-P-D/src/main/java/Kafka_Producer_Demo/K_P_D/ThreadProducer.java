package Kafka_Producer_Demo.K_P_D;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ThreadProducer extends Thread {

	volatile static int count = 0;
	Producer<String, String> producer = null;

	public ThreadProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	@Override
	public void run() {
		for (count = 0; count < 10; count++) {
			producer.send(new ProducerRecord<String, String>("my-topic", count+" "+Thread.currentThread().getId()));
			System.out.println(count);
		}
		producer.close();
	}

}
