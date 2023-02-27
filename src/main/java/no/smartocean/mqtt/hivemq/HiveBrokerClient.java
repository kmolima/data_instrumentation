package no.smartocean.mqtt.hivemq;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

public class HiveBrokerClient {
	
	private final Mqtt5AsyncClient client;

	private final ClientConfig clientConfig;
	
	public HiveBrokerClient(ClientConfig conf) {

		this.clientConfig = conf;

		this.client = Mqtt5Client.builder() //TODO config data persistence and responsetopic
		        .identifier(conf.getClient_id())
		        .serverHost(conf.getHost())
		        .serverPort(conf.getPort())
		        .buildAsync();

		System.out.println("Broker client configured successfully");
	}

	public Mqtt5AsyncClient getBrokerClient() { return this.client; }

	public ClientConfig getBrokerClientConfig() {return this.clientConfig;}

}
