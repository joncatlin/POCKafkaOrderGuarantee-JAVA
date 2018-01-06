
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class SimpleConsumer {

	static final String ENV_KAFKA_NODES = "KAFKA_NODES";
	static final String ENV_KAFKA_TOPIC = "KAFKA_TOPIC";
	static final String ENV_KAFKA_GROUP_ID = "KAFKA_GROUP_ID";
	
	static Map<String, String> env = System.getenv();
	static List<String> envVariables = new ArrayList<String>();

	public static void main(String[] args) {

		// Initialize the list of Environment Variables that must exist
		envVariables.add(ENV_KAFKA_NODES);
		envVariables.add(ENV_KAFKA_TOPIC);
		envVariables.add(ENV_KAFKA_GROUP_ID);
		
		// Initialize the environment
		checkEnvVariables();

		// Read in the file and send to a kafka topic
		consume();
	}

	
	private static void consume() {

		Gson gson = new Gson();
        Type type = new TypeToken<SimpleClass>() {}.getType();

		ConsumerRecords<String, String> records = null;
		KafkaConsumer<String, String> consumer = null;

		consumer = getKafkaConnection();

		// Subscribe to the list of topics
		consumer.subscribe(Arrays.asList(env.get(ENV_KAFKA_TOPIC)));

		//Create structure to keep the indexes for each key in
		final int MAX_KEYS = 5;
		int[] indexes = new int[MAX_KEYS];
		
		// Process any records received from the topic and check to ensure they are in sequence
		while (true) {

			// Get some records from Kafka
			records = consumer.poll(100);

			for (ConsumerRecord<String, String> record : records) {
				int key = Integer.valueOf(record.key());
//				long offset = record.offset();
				
				SimpleClass msgReceived = gson.fromJson(record.value(), type);
				if (msgReceived.counter != indexes[key]+1) {
					System.out.println("Key: " + key + ", Expected counter: " + indexes[key]+1 + ", Received counter: " + msgReceived.counter);
				}
				else if (msgReceived.counter == indexes[key]) {
					System.out.println("Key: " + key + ", Received duplicate counter: " + msgReceived.counter);
				} else {
					indexes[key]++;
				}
			}
			
            consumer.commitAsync();
		}
	}

	
	private static void checkEnvVariables() {

		// Check all environment variables exist and if not exit
		for (String envVar : envVariables) {
			if (!env.containsKey(envVar)) {
				System.out.println("Missing environment variable: " + envVar);
				System.exit(0);
			} else {
				System.out.println(envVar + " = " + env.get(envVar));
			}
		}
	}
	
	
	private static KafkaConsumer<String, String> getKafkaConnection() {

		KafkaConsumer<String, String> consumer = null;
		
		// Create the connection to the Kafka cluster
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.get(ENV_KAFKA_NODES));
		props.put(ConsumerConfig.GROUP_ID_CONFIG, env.get(ENV_KAFKA_GROUP_ID));
//		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		boolean waitingForKafkaServer = true;
		
		do {
			try {
				consumer = new KafkaConsumer<String, String>(props);

				// Try the connection to see if it is operational by getting a list of the topics
				Map<String, List<PartitionInfo> > topics = consumer.listTopics();
				for (String topicName : topics.keySet()) {
					System.out.println("Topic found: " + topicName );
				}
				
				waitingForKafkaServer = false;
			} catch (Exception e) {
				System.out.println("Kafka server unavailable, waiting for connection before continuing");
				try {Thread.sleep(10000L);} catch (InterruptedException ex) { System.exit(0); }
			}
		} while (waitingForKafkaServer);		
		
		System.out.println("Kafka server available, continuing");
		return consumer;
	}
}
