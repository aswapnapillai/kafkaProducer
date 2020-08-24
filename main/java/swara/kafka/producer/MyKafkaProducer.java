package swara.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class MyKafkaProducer {
    static Properties props = new Properties();

    static {
        props.put("bootstrap.servers","192.168.0.191:2181");

        props.put("acks","all");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

    }

    public   KafkaProducer<String,String>  connectToKafka(){
        KafkaProducer<String,String> producer = new KafkaProducer(props);

        System.out.println("Connected");
        return producer;
    }

    public   void sendMessageSyn(String message){
        String key="myKey";
        String value=message;
        ProducerRecord data = new ProducerRecord("",key,value);
        connectToKafka().send(data, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e!=null){
                    e.printStackTrace();
                    System.out.println("Offset of sent recored is " + recordMetadata.offset());
                }
                System.out.println("Offset of sent recored is " + recordMetadata.offset());
            }
        });
    }

    public static void main(String[] args) {
        MyKafkaProducer myProducer = new MyKafkaProducer();
        myProducer.sendMessageSyn("This Is my Message");


    }
}
