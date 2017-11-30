# Library: Kafka Producer
## Overview   
  
A Java library that creates dummy smart meter readings every 15 seconds for 20 different meters.

## Requirements
See [POM file](./pom.xml)


## Usage Example
```java
java -cp smartmeter-kafka-producer-0.0.1.jar com.khattak.bigdata.realtime.sensordataanalytics.smartmeter.SmartMeterEventsProducer 192.168.70.136:9092 smartmeter-readings
```

## License
The content of this project is licensed under the [Creative Commons Attribution 3.0 license](https://creativecommons.org/licenses/by/3.0/us/deed.en_US).