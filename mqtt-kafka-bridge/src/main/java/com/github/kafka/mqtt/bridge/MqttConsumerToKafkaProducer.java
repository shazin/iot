package com.github.kafka.mqtt.bridge;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
/**
 * MQTT Kafka Bridge
 * 
 * @author shazin
 *
 */
public class MqttConsumerToKafkaProducer {

	private static final String MQTT_BROKER_TOPICS = "mqttbrokertopics";
	private static final String MQTT_BROKER_PORT = "mqttbrokerport";
	private static final String MQTT_BROKER_HOST = "mqttbrokerhost";
	private static final String SERIALIZER_CLASS = "serializerclass";
	private static final String BROKER_LIST = "brokerlist";

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		
		options.addOption(BROKER_LIST, true, "Kafka Brokers List");
		options.addOption(SERIALIZER_CLASS, true, "Kafka Serializer Class");
		options.addOption(MQTT_BROKER_HOST, true, "MQTT Broker Host");
		options.addOption(MQTT_BROKER_PORT, true, "MQTT Broker Port");
		options.addOption(MQTT_BROKER_TOPICS, true, "MQTT Broker Topics");
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		
		Properties props = new Properties();
		props.put("metadata.broker.list", cmd.getOptionValue(BROKER_LIST, "localhost:9092"));
		props.put("serializer.class", cmd.getOptionValue(SERIALIZER_CLASS, "kafka.serializer.StringEncoder"));
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config); 
        
		MQTT mqtt = new MQTT();
		mqtt.setHost(cmd.getOptionValue(MQTT_BROKER_HOST, "localhost"), Integer.parseInt(cmd.getOptionValue(MQTT_BROKER_PORT, "1883")));

		BlockingConnection connection = mqtt.blockingConnection();
		connection.connect();
		
		String topicsArg = cmd.getOptionValue(MQTT_BROKER_TOPICS, "illumination");
		List<Topic> topicsList = new ArrayList<Topic>();
		String[] topics = topicsArg.split(",");
		for(String topic:topics) {
			topicsList.add(new Topic(topic, QoS.AT_LEAST_ONCE));
		}

		Topic[] mqttTopics = topicsList.toArray(new Topic[]{});
		byte[] qoses = connection.subscribe(mqttTopics);

		
		boolean exit = false;
		while (!exit) {
			Message message = connection.receive();
			byte[] payload = message.getPayload();
			String strPayload = new String(payload);
			// process the message then:
			message.ack();
			KeyedMessage<String, String> kafkaMessage = new KeyedMessage<String, String>(message.getTopic() , strPayload);
			producer.send(kafkaMessage);
		}

		connection.disconnect();
		producer.close();
	}

}
