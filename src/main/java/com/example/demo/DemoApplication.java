package com.example.demo;

import de.codecentric.boot.admin.server.config.EnableAdminServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@EnableAdminServer
@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) throws Exception {

		SpringApplication.run( DemoApplication.class, args );
//        String topic = "spring-kafka-demo";
//		var producer = new MyProducer( topic );
//
//        ExecutorService fixedPool = Executors.newFixedThreadPool( 100);
//        var launch = new CountDownLatch(10000);
//		fixedPool.submit( new Runnable() {
//                    @Override
//                    public void run() {
//                        for (int i = 0; i < 10000; i++) {
//                        try {
//                            producer.send( Integer.toString( i ) ,"my-topic");
//                            launch.countDown();
//
//                        } catch (ExecutionException e) {
//                            throw new RuntimeException( e );
//                        } catch (InterruptedException e) {
//                            throw new RuntimeException( e );
//                        }
//                        System.out.println(i+" sent");
//                    }
//					}
//        });
//
//		launch.await();
//
//		var consumer = new MyConsumer(topic);
//		consumer.consume(record -> System.out.println( "Got key: " + record.key() + ", value: " + record.value()+ ", partition: " + record.partition() ));
//		TimeUnit.MINUTES.sleep( 10 );
//		producer.close();
//		consumer.close();
	}

}

//class MyConsumer implements AutoCloseable {
//	private String topic;
//	private KafkaConsumer<String, String> consumer = getConsumer();
//
//	MyConsumer (String topic) {
//		this.topic = topic;
//	}
//
//	private KafkaConsumer<String, String> getConsumer() {
//		var props = new Properties();
//		props.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
//		props.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
//
//		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>( props);
//		consumer.subscribe( Collections.singletonList( "spring-kafka-demo" ) );
//		return consumer;
//	}
//
//	void consume (Consumer<ConsumerRecord<String,String>> recordConsumer) {
//		new Thread( () -> {
//			while (true) {
//				var records = consumer.poll( Duration.ofSeconds( 1) );
//				records.forEach( record ->
//						recordConsumer.accept( record ));
//			}
//		}).start();
//	}
//
//	@Override
//	public void close() throws Exception {
//		consumer.close();
//	}
//
//
//}
//
//
//class MyProducer implements AutoCloseable {
//	String topic="spring-kafka-demo";
//	KafkaProducer<String, String> producer = getProducer();
//
//	MyProducer (String topic) {
//		this.topic = "spring-kafka-demo";
//	}
//
//	private KafkaProducer<String, String> getProducer() {
//		var props = new Properties();
//		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//		props.put(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
//		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
//
//		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
//		return producer;
//	}
//
//	void send(String key, String value) throws ExecutionException, InterruptedException {
//		producer.send(new ProducerRecord<String, String>("spring-kafka-demo", key, value)).get();
//		System.out.println(key+"Yegor");
//	}
//
//	@Override
//	public void close() throws Exception {
//		producer.close();
//	}
//}
