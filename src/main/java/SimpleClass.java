import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class SimpleClass
    {
        public String key;
        public int counter;
        public HashMap<String, String> msgs;
        public int[] intArray;

        public SimpleClass(String key, int counter, int numMsgs)
        {
            this.key = key;
            this.counter = counter;
            this.msgs = new HashMap<String, String>(10);
            for (int i=0; i < numMsgs; i++)
            {
            	String textGUID = UUID.randomUUID().toString();
                this.msgs.put(textGUID, "GUID = " + textGUID);
            }
            int size = ThreadLocalRandom.current().nextInt(1, 51);
            this.intArray = new int[size];
            for (int j=0; j<size; j++)
            {
                this.intArray[j] = ThreadLocalRandom.current().nextInt(1, 51);
            }
        }
    }