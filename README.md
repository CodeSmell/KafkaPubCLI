# Kafka Util

This utility will monitor a directory. It will publish each file in the specified directory to a Kafka topic.
This CLI utility was built to enable easy testing of products that consume from Kafka topics. 

## Architecture Overview

```
                        KafkaMain
                     (Entry Point)
                 - Parses CLI arguments
                 - Manages dependencies
                          |
                          | uses
                          v
              DefaultKafkaProducerUtil
                    (Coordinator)
                          |
          +------------------+-------------------+
          |                  |                   |
      uses for            uses to             uses to
    file polling        parse file              send        
          |               content             to Kafka
          |                  |                   |
          v                  v                   v
  DirectoryPolling   KafkaContentHandler   KafkaProducer
       Service          (Parses)             (Kafka)
    (File Polling)
```

**Key Design Principle:** 

`DirectoryPollingService` is a reusable component with zero coupling to Kafka. It only knows: "Poll directory → Read file → Call predicate → Delete if predicate returns true". The lambda/method reference (`processFileContents`) is the glue that connects the generic polling service to the specific Kafka publishing logic.

## Format for the Files
The utility does not expect any format when publishing only a body on a Kafka message. 
The entire contents of the file will be published as the body of the message.
It will not treat the EOL in any special way so this allows the content (like JSON) to be pretty printed and still be sent in its entirety. 

For example a JSON that is pretty printed. 

```
{
	"foo": "bar",
	"code": "smell"
}
```

This allows easy scanning and editing the file when testing.

### Adding a Key and Headers to the Kafka message

However, if a key and headers are needed in the Kafka message the following format can be used:

At the top of the file add the `--key` delimiter.
Every line above that delimiter will be expected to be the value of the key. 
It is also expected that it will be one line and always appear first if it is needed.

At the top of the file (but below the `--key` delimiter if there is one) add the `--header` delimiter.
Every line above that delimiter will be expected to be a key value pair separated by a colon (:).
All of the content below that delimiter will be considered the body of the Kafka message.

```
foo
--key
hello:world
ghost:buster
--header
foobar
```

The contents above will result in a single Kafka message.
The body will be `foobar` and there will be two headers. 
The key for one of the headers will be `hello` and the value will be `world`. 
The key for the other header will be `ghost` and the value will be `buster`. 
The key for the Kafka payload will be `foo`.

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

If the user only wants to run the utility against the directory once then add the parameter (`runOnce`). 
If the user doesn't want to remove the file then add the parameter (`noDeleteFiles`).

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
