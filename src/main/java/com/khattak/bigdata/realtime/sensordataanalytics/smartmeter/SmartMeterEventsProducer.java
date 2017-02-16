/*
 * Creates dummy smart meter readings every 15 seconds for 20 different meters
 */
package com.khattak.bigdata.realtime.sensordataanalytics.smartmeter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;


public class SmartMeterEventsProducer {

    private static final Logger LOG = Logger.getLogger(SmartMeterEventsProducer.class);
    private static List<String> METER_IDS = null;  
    private static final String[] SUBSTATION_IDS = {"d3b85a1b-c140-4190-afe8-9ef3f893392d", "213d79b9-ea33-418d-9a56-5d53b0187c78", "a55c3494-6edd-450c-abab-42b8bc3c50ca", "6a8973ce-b704-4f58-821b-661c52bb9aaf", "5fe288d5-5800-4791-80a3-6843a6b88bb5"};
    private static final String[] CITIES = {"London","Birmingham","Manchester","Glasgow"};
    private static String TOPIC = null;//"smartmeter-readings"
    private static int TOPIC_PARTITION = 0;//0 or 1 or 2 ......

    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length < 3 && args.length > 4) {
            
            System.out.println("Usage: SmartMeterEventsProducer <broker list> <topic> OR SmartMeterEventsProducer <broker list> <topic> <topic-partition number>");
            System.exit(-1);
        }
        
        TOPIC = args[1]; 
       
        if (args.length == 2){
        	LOG.debug("Using broker list:" + args[0] +", topic:" + args[1]);
        }
        else{
        	TOPIC_PARTITION = Integer.parseInt(args[2]); 
        	LOG.debug("Using broker list:" + args[0] + ", topic:" + args[1] + ", topic-partition number:" + args[2]);
        }

        // Initialize the METER_IDS array
        generateMeterIds();
    	
        // Now kick off n number of threads (20 at the moment) to generate readings & write to Kafka 
        for (int i = 0;i < METER_IDS.size();i++){
        	GenerateAndWriteMeterData thread = new GenerateAndWriteMeterData(i,args); 
        	new Thread(thread).start();
        }
        
    }
    
    /*
     * Generates 15 second consumption reading based on seasonal & time of day factors
     */
    private static String generate15SecondUsageKwh(Date dt) {
    	double seasonalEffect = 0.0d;
    	double timeOfDayEffect = 0.0d;
    	double totalEffect = 0.0d;
    	
    	
    	Calendar calendar = Calendar.getInstance();
    	calendar.setTime(dt);
    	
    	// heavy usage due to summer/winter
    	if ((calendar.get(Calendar.MONTH) > 3) && (calendar.get(Calendar.DAY_OF_MONTH) < 8) && (calendar.get(Calendar.DAY_OF_MONTH) > 10) && (calendar.get(Calendar.DAY_OF_MONTH) < 2)) {
    		seasonalEffect = 0.5;
    	}
    	//heavy usage during evening & morning 
    	if ((calendar.get(Calendar.HOUR_OF_DAY) > 5) && (calendar.get(Calendar.HOUR_OF_DAY) < 9) && (calendar.get(Calendar.HOUR_OF_DAY) > 17) && (calendar.get(Calendar.HOUR_OF_DAY) < 22)) {
    		timeOfDayEffect = 0.35;
    	}
    	
    	totalEffect = 1 + seasonalEffect + timeOfDayEffect;
    	
    	//average UK electricity consumption per 15 seconds = 0.0022 kwh
    	
    	DecimalFormat df = new DecimalFormat("0.####");
    	return  df.format((ThreadLocalRandom.current().nextDouble(0.0005,0.0023) * totalEffect));
	}

    /*
     * Fills up the METER_IDS array with 20 unique meter Ids: 10 domestic & 10 commercial
     * Meter Id is UUID based 
     */
	private static void generateMeterIds(){
    	METER_IDS = new ArrayList<String>();
    	
    	//domestic
    	for (int i = 0; i < 10; i++){
    		METER_IDS.add("DOM$" + UUID.randomUUID().toString());
    	}
    	
    	//commercial
    	for (int i = 0; i < 10; i++){
    		METER_IDS.add("COM$" + UUID.randomUUID().toString());
    	}
    }
	
	/*
	 * Thread class that generates & writes readings to a Kafka topic
	 */
	private static class GenerateAndWriteMeterData implements Runnable {
		static volatile int threadCount = 0;
		int meterIndexInList = 0;
		String[] args = null;

		public GenerateAndWriteMeterData(int meterIndexInList, String[] args) {
			this.meterIndexInList = meterIndexInList; 
			this.args = args;
		}
		
		@Override
		public void run() {
			GenerateAndWriteMeterData.threadCount++;
			LOG.info("Total producer threads:" + GenerateAndWriteMeterData.threadCount);
			
			String meterId = METER_IDS.get(this.meterIndexInList);
			             
            Properties props = new Properties();
            props.put("bootstrap.servers", args[0]); // localhost:9092 OR 192.168.70.136:9092
            props.put("client.id", meterId); // An id string to pass to the server when making requests
            props.put("acks", "all");
            //props.put("retries", 0);
            //props.put("batch.size", 0); //disabled
            //props.put("linger.ms", 0); // no delay
            //props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            
            Producer<String, String> producer = new KafkaProducer<>(props);
            ProducerRecord<String, String> data = null;
            
            Date rightNow = null;
			String fifteenSecondUsage = null; 
			Timestamp ts = null;
			String substationId = "na";
            String city = "na";
            String key = null;
            String message = null;
            
            // run for a very long time
			for (long i = 0;i < 1000000000;i++){
	            try {
	            	//create message
					rightNow = new Date();
					// The consumption figures
					fifteenSecondUsage = generate15SecondUsageKwh(rightNow); 
					ts = new Timestamp(new Date().getTime());
					
					// Assign each meter to the same substation for meaningful reporting 
					// Could have used a case statement here
				    if (this.meterIndexInList < 4) substationId = SUBSTATION_IDS[0];
					else if (this.meterIndexInList > 3 && this.meterIndexInList < 8) substationId = SUBSTATION_IDS[1];
					else if (this.meterIndexInList > 7 && this.meterIndexInList < 12) substationId = SUBSTATION_IDS[2];
					else if (this.meterIndexInList > 11 && this.meterIndexInList < 16) substationId = SUBSTATION_IDS[3];
					else substationId = SUBSTATION_IDS[4];
					
				    // Assign each meter to the same city for meaningful reporting 
				    if (this.meterIndexInList < 5) city = CITIES[0];
					else if (this.meterIndexInList > 4 && this.meterIndexInList < 10) city = CITIES[1];
					else if (this.meterIndexInList > 9 && this.meterIndexInList < 15) city = CITIES[2];
					else city = CITIES[3];
					
				    // Now setup the message that needs to be written to Kafka
				    // Message key is the meter id
					key = meterId;
					message = ts + "|" + meterId + "|" + fifteenSecondUsage //+ "|" + events[random.nextInt(evtCnt)] 
						        + "|" + substationId + "|" + city
						        + "|" + Math.round(ThreadLocalRandom.current().nextDouble(240.01,245)*100.00d)/100.00d// max voltage 
								+ "|" + Math.round(ThreadLocalRandom.current().nextDouble(235,240)*100.00d)/100.00d// min voltage
					  			+ "|" + ((i%37==0)?(Math.round(ThreadLocalRandom.current().nextInt(5,16)*100.00d)/100.00d):0);// power cut duration in seconds, generated only few times
	            	 
					if (args.length == 3) data = new ProducerRecord<String, String>(TOPIC, key, message);
					else data = new ProducerRecord<String, String>(TOPIC, TOPIC_PARTITION, key, message);
					LOG.info("key: " + key +", msg:" + message);
					producer.send(data);
				    Thread.sleep(15000);// 15 seconds
				} catch (Exception e) {
				    e.printStackTrace();
				    LOG.error(e.getMessage());
				}
			}
			 producer.close();
		}
	}
	
}
