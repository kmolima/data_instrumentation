package no.smartocean.mqtt.hivemq;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.BeanAccess;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

// Class automatically factored by yaml parser constructor
public class ClientConfig {

    private static ClientConfig instance = null; //singleton

    private String clientId;
    private String host;
    private int port;
    private Object authentication; //TODO

    private List<Topic> topics;

    public ClientConfig() {

    }

    public Object getAuthentication() {
        return authentication;
    }

    private void setAuthentication(Object authentication) {
        this.authentication = authentication;
    }

    public String getClient_id() {
        return clientId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public List<Topic> getTopics() {return this.topics;}


    protected void setClientId(String clientId) {
        this.clientId = clientId;
    }

    protected void setHost(String host) {
        this.host = host;
    }

    protected void setPort(int port) {
        this.port = port;
    }

    private void setTopics(List<Topic> topics) {
        this.topics = topics;
    }

    public static ClientConfig loadFromFile(Path config) throws IOException {

        Constructor constructor = new Constructor(ClientConfig.class); //ClientConfig.class is root
        TypeDescription configDescription = new TypeDescription(ClientConfig.class);
        configDescription.addPropertyParameters("Topics", Topic.class);
        constructor.addTypeDescription(configDescription);
        Yaml yaml = new Yaml(constructor);
        yaml.setBeanAccess(BeanAccess.FIELD);
        instance = yaml.load(Files.newInputStream(config));
        yaml.setBeanAccess(BeanAccess.DEFAULT);
        return instance;
    }

    public static ClientConfig getInstance() {
        return instance;
    }

    public static void main(String args[]) throws IOException {

        Constructor constructor = new Constructor(ClientConfig.class);
        TypeDescription configDescription = new TypeDescription(ClientConfig.class);
        configDescription.addPropertyParameters("topics", Topic.class);
        constructor.addTypeDescription(configDescription);
        Yaml yaml = new Yaml(constructor);

        ClientConfig config = yaml.load(Files.newInputStream(Path.of("config/config_dev_pub.yaml")));
    }
}
