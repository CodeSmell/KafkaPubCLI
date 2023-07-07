# Kafka Util

This utility will monitor a directory. It will publish each file in the specified directory to a Kafka topic.
This CLI utility was built to enable easy testing of products that consume from Kafka topics. 


## Format for the Files
The utility does not expect any format when publishing only a body on a Kafka message. 
The entire contents of the file will be published as the body of the message
It will not treat the EOL in any special way so this allows the content (like JSON) to be pretty printed and still be sent in its entirety. 

For example a JSON that is pretty printed. 

```
{
	"foo": "bar",
	"code": "smell"
}
```

This allows easy scanning and editing the file when testing.

### Adding Headers to Kafka message

However, if headers are needed in the Kafka message the following format can be used:

At the top of the file add the following  `--header` delimiter
Every line above that delimiter will be expected to be a key value pair separated by a colon (:)
All of the content below that delimiter will be considered the body of the Kafka message.

```
hello:world
--header
foobar
```

The contents above will result in a single Kafka message.
The body will be `foobar` and there will be one header. 
The key for the header will be `hello` and the value will be `world`. 


## Build the Uber JAR

```
mvn clean install spring-boot:repackage
```

### Running the Util
The default mode is to continually poll the directory (`messageLocation`) for files that should be published to Kafka. Once a file is published to the Kafka topic it will be deleted. 

``` 
java -jar target/kafka-utils-0.0.1-SNAPSHOT.jar \
                            -topic myTopic \
                            -bootstrap-server localhost:9092 \
                            -acks 1 \
                            -messageLocation /dev/myKafkaFiles
```

if the user only wants to run the utility against the directory once then add the parameter (`runOnce`). 
if the user doesn't want to remove the file then add the parameter (`noDeleteFiles`).

``` 
java -jar target/kafka-utils-0.0.1-SNAPSHOT.jar \
                            -topic myTopic \
                            -bootstrap-server localhost:9092 \
                            -acks 1 \
                            -messageLocation /dev/myKafkaFiles \
                            -runOnce \
                            -noDeleteFiles
```


### Running the Util in secure mode

``` 
java -jar target/kafka-utils-0.0.1-SNAPSHOT.jar \
                            -topic myTopic \
                            -bootstrap-server localhost:443 \
                            -acks 1 \
                            -messageLocation /dev/myKafkaFiles \
                            -isSecure \
                            -trustStoreType JKS \
                            -trustStoreLocation /dev/truststore.jks \
                            -trustStorePassword password \                            
                            -securityProtocol SASL_SSL \
                            -saslMechanism SCRAM-SHA-512 \
                            -saslJaasConfig 'org.apache.kafka.common.security.scram.ScramLoginModule required \
                            	username="user" \
                            	password="password";'
```
