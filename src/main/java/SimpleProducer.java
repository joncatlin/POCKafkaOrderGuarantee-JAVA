import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class SimpleProducer {
	
	static final String ENV_KAFKA_NODES = "KAFKA_NODES";
	static final String ENV_KAFKA_TOPIC = "KAFKA_TOPIC";
	static final String ENV_KAFKA_GROUP_ID = "KAFKA_GROUP_ID";
	
	static Map<String, String> env = System.getenv();
	static List<String> envVariables = new ArrayList<String>();

	static long startTime = 0;
	
	public static void main(String[] args) {

		startTime = System.nanoTime();    

		// Initialize the list of Environment Variables that must exist
		envVariables.add(ENV_KAFKA_NODES);
		envVariables.add(ENV_KAFKA_TOPIC);
		envVariables.add(ENV_KAFKA_GROUP_ID);
		
		// Initialize the environment
		checkEnvVariables();
		
		// Read in the file and send to a kafka topic
		produce();
	}

	
	private static void produce() {
		
		Gson gson = new Gson();
        Type type = new TypeToken<SimpleClass>() {}.getType();
		
		try {
			// Get a connection to the kafka cluster
			Producer<String, String> producer = getKafkaConnection();

			//Create structure to keep the indexes for each key in
			final int MAX_KEYS = 5;
			int[] indexes = new int[MAX_KEYS];
			
			// Produce msgs
			for (int index=0; index<1000000; index++) {
//              String msgKey = "" + index % MAX_KEYS;
              String msgKey = "" + index;
                SimpleClass msgToSend = new SimpleClass(msgKey, indexes[index % MAX_KEYS]++, 10);
                String json = gson.toJson(msgToSend, type);
                
                final ProducerRecord<String, String> record =
                        new ProducerRecord<>(env.get(ENV_KAFKA_TOPIC), String.valueOf(index), json);
                producer.send(record, (metadata, exception) -> 
                {
                    if (metadata != null) {
                    	System.out.println("Sent to " + metadata.toString());
                    } else {
                        exception.printStackTrace();
                    }
                });
     		}
			
	        producer.flush();
	        
	        producer.close();

		} catch (Exception e) {
			e.printStackTrace();
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
	
	
	private static Producer<String, String> getKafkaConnection() {

		final int C10MB = 10000000;
		
		// Refer to the following documentation - http://kafka.apache.org/documentation.html#producerconfigs
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.get(ENV_KAFKA_NODES));
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 10 * C10MB); 	// 100MB
		props.put(ProducerConfig.RETRIES_CONFIG, 10);		// Kafka Definitive Guide page 124, must retry.
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, C10MB);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 600000);
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Kafka Def Guide page 51
		
		//max.request.size
		//enable.idempotence

//		props.put("max.request.size", C10MB);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// Connect to the kafka cluster
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		return producer;
	}
}

