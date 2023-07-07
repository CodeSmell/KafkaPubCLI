package codesmell.kafka;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

public class ProducerArgs {
    @Parameter
    private List<String> parameters = new ArrayList<>();

    @Parameter(names = "-help", description = "show all options", help = true)
    private boolean help;

    @Parameter(names = { "-client.id" }, description = "identifies the product working w/ Kafka")
    private String clientId = "kafkautil.producer";

    @Parameter(names = "-topic", description = "the Kafka topic", required = true)
    private String topic;

    @Parameter(names = {
            "-bootstrap-server" }, description = "comma-separated list of kafka brokers (host:port)", required = true)
    private String bootstrap;

    @Parameter(names = "-acks", description = "how many replicas must receive message (0, 1, all)", required = true)
    private String ackMode;

    @Parameter(names = "-retries", description = "how many times failures will be retried")
    private Integer retries = 0;

    @Parameter(names = "-retryDelays", description = "the delay in ms between retries (retry.backoff.ms)")
    private Integer retryDelay = 100;

    @Parameter(names = "-maxInflight", description = "the number of batches on a connection that can be sent to broker without a response")
    private Integer maxInflight = 1;

    @Parameter(names = "-batchSizeBytes", 
            description = "the maximum size in bytes of the buffer used to batch messages before sending to Kafka (batch.size)")
    private Integer batchSizeBytes = 16384;

    @Parameter(names = "-batchDelay", description = "the delay in ms that producer will wait for buffer to be filled (linger.ms)")
    private Integer batchDelay = 0;

    @Parameter(names = "-isSecure", 
            description = "app will connect to the broker in a secure way")
    private boolean isSecure = false;

    @Parameter(names = "-securityProtocol", description = "the security protocol used to communicate w/ brokers")
    private String securityProtocol;
    
    @Parameter(names = "-saslMechanism", description = "SASL mechanism")
    private String saslMechanism;
    
    @Parameter(names = "-saslJaasConfig", description = "SASL JaaS config")
    private String saslJaasConfig;
    
    @Parameter(names = "-trustStoreType", description = "the format of the trust store")
    private String trustStoreType;
    
    @Parameter(names = "-trustStoreLocation", description = "the path to the trust store")
    private String truststoreLocation;
    
    @Parameter(names = "-trustStorePassword", description = "the password for the trust store")
    private String truststorePassword;
    
    //
    // CLI args related to where payload files are located
    // 
    @Parameter(names = {
            "-messageLocation" }, 
            description = "directory where files are located that will be published to topic", 
            required = true)
    private String messageLocation;
    
    @Parameter(names = "-delayInSeconds", 
            description = "how long to wait between file polls looking for new messages")
    private Integer delaySeconds = 5000;
    
    @Parameter(names = "-runOnce",
            description = "only poll messageLocation once when this parameter is added")
    private boolean runOnce = false;
    
    @Parameter(names = "-noDeleteFiles",
            description = "app will delete the files after a poll unless this parameter is added")
    private boolean noDeleteFiles = false;
    

    public boolean isHelp() {
        return help;
    }

    public String getTopic() {
        return topic;
    }

    public String getBootstrap() {
        return bootstrap;
    }

    public String getClientId() {
        return clientId;
    }

    public String getAckMode() {
        return ackMode;
    }

    public Integer getRetries() {
        return retries;
    }

    public Integer getRetryDelay() {
        return retryDelay;
    }

    public Integer getMaxInflight() {
        return maxInflight;
    }

    public Integer getBatchSizeBytes() {
        return batchSizeBytes;
    }

    public Integer getBatchDelay() {
        return batchDelay;
    }

    public String getMessageLocation() {
        return messageLocation;
    }

    public Integer getDelaySeconds() {
        return delaySeconds;
    }

    public boolean isRunOnce() {
        return runOnce;
    }

    public boolean isNoDeleteFiles() {
        return noDeleteFiles;
    }

    public boolean isSecure() {
        return isSecure;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public String getTruststoreLocation() {
        return truststoreLocation;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

}