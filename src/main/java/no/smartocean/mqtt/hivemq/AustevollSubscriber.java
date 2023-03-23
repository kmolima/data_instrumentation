package no.smartocean.mqtt.hivemq;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;

import static com.hivemq.client.mqtt.MqttGlobalPublishFilter.ALL;

public class AustevollSubscriber {

	//Metric Category: Completeness and Integrity
	public static final Counter integrityCounter = Counter.build().namespace("no_smartocean")
			.name("data_ingestion_integrity_faults_count").help("Total integrity faults found in incoming data.")
			.labelNames("service","cause","retained").register(); //TODO Add label: instance of the service

	//Metric Category: Accuracy
	public static final Counter qcCounter = Counter.build().namespace("no_smartocean")
			.name("data_ingestion_quality_controlled_count").help("Total quality controlled ingested data.")
			.labelNames("service","qcFlag","retained","provider").register(); //TODO Add label: instance of the service

	//Metric Category: Timeliness
	public static final Histogram rtDelayHistogram = Histogram.build().namespace("no_smartocean")
			.name("data_ingestion_arrival_delay")
			.help("Delay in seconds between data acquisition and arrival to the platform.")
			.labelNames("service","provider").register();  //TODO Add label: instance of the service

	//Metric Category: System Delay/overhead
	// OPEN-Telemetry - https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/semantic_conventions/faas-metrics.md
	public static final Histogram serviceDelayHistogram = Histogram.build().namespace("no_smartocean")
			.name("faas_invoke_duration")
			.help("Delay in seconds between data acquisition and arrival to the platform.")
			// "invoked_name" label replaced by "service"
			.labelNames("service","trigger").register();  //TODO Add labels: invoked provider & invoked region

	public static void main(String[] args) {

		Path config = args.length > 0 ? Path.of(args[0]) : Path.of("config/config.yaml");

		if (Files.isReadable(config)) {
			try {

				// Load configs form YAML
				final ClientConfig conf = ClientConfig.loadFromFile(config);

				// create an MQTT client
				final HiveBrokerClient hiveclient = new HiveBrokerClient(conf);

                // Initialize Metrics Collector endpoint for Prometheus
				HTTPServer server;
				server = new HTTPServer.Builder()
							.withPort(9091)
							.build();

				hiveclient.getBrokerClient().toAsync().connect().thenAccept(ack -> AustevollSubscriber.logBrokerConnection(ack));

				hiveclient.getBrokerClient()
						.toAsync()
						.subscribeWith()
						.topicFilter(conf.getTopics().get(0).getSubscribeTopic()) //TODO
						.qos(MqttQos.AT_LEAST_ONCE)
						.send();

				//while(true) {

					//set a callback that is called when a message is received (using the async API style)
					hiveclient.getBrokerClient().toAsync().publishes(ALL, new AADIXMLCallback()::accept,
							Executors.newSingleThreadExecutor()); //validates each new message in a new thread
				//}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static void logBrokerConnection(Mqtt5ConnAck connAck){
		System.out.println("Connected to Broker "+connAck); //TODO observability - Log
	}

}
