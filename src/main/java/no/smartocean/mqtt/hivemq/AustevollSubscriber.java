package no.smartocean.mqtt.hivemq;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
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
			.labelNames("service","cause").register(); //TODO Add label: instance of the service

	//Metric Category: Accuracy
	public static final Counter qcCounter = Counter.build().namespace("no_smartocean")
			.name("data_ingestion_quality_controlled_count").help("Total quality controlled ingested data.")
			.labelNames("service","qcFlag","provider").register(); //TODO Add label: instance of the service

	//Metric Category: Timeliness
	public static final Histogram rtDelayHistogram = Histogram.build().namespace("no_smartocean")
			.name("data_ingestion_arrival")
			.help("Delay in seconds between data acquisition and arrival to the platform.").register();
			//.labelNames("service","provider").register();  //TODO Add label: instance of the service

	//Metric Category: Volume? Concordance?
	public static final Histogram validatedHistogram = Histogram.build().namespace("no_smartocean")
			.name("data_ingestion_validated")
			.help("Amount of bytes of messages validated by the platform.").register();
			//.labelNames("service","provider").register();  //TODO Add label: instance of the service

	//Metric Category: Volume? Concordance?
	public static final Histogram retainedHistogram = Histogram.build().namespace("no_smartocean")
			.name("data_ingestion_retained")
			.help("Amount of bytes of messages retained by the platform.").register();
			//.labelNames("service","provider").register();  //TODO Add label: instance of the service

	//Metric Category: System Delay/overhead
	// OPEN-Telemetry - https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/semantic_conventions/faas-metrics.md
	public static final Histogram serviceDelayHistogram = Histogram.build().namespace("no_smartocean")
			.name("faas_invoke_duration_histogram")
			.help("Delay in seconds introduced by this service.").register();
			// "invoked_name" label replaced by "service"
			//.labelNames("service","trigger").register();  //TODO Add labels: invoked provider & invoked region

	//Metric Category: Timeliness
	public static final Gauge rtDelayGauge = Gauge.build().namespace("no_smartocean")
			.name("data_ingestion_arrival_delay")
			.help("Delay in seconds between data acquisition and arrival to the platform.").labelNames("provider").register();
			//.labelNames("service","provider").register();  //TODO Add label: instance of the service

	//Metric Category: Volume? Concordance?
	public static final Gauge validatedGauge = Gauge.build().namespace("no_smartocean")
			.name("data_ingestion_validated_bytes")
			.help("Amount of bytes of messages validated by the platform.").register();
			//.labelNames("service","provider").register();  //TODO Add label: instance of the service

	//Metric Category: Volume? Concordance?
	public static final Gauge retainedGauge = Gauge.build().namespace("no_smartocean")
			.name("data_ingestion_retained_bytes")
			.help("Amount of bytes of messages retained by the platform.").register();
			//.labelNames("service","provider").register();  //TODO Add label: instance of the service

	//Metric Category: System Delay/overhead
	// OPEN-Telemetry - https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/semantic_conventions/faas-metrics.md
	public static final Gauge serviceDelayGauge = Gauge.build().namespace("no_smartocean")
			.name("faas_invoke_duration")
			.help("Delay in seconds introduced by this service.").register();
			// "invoked_name" label replaced by "service"
			//.labelNames("service","trigger").register();  //TODO Add labels: invoked provider & invoked region

	public static void main(String[] args) {

		Path config = args.length > 0 ? Path.of(args[0]) : Path.of("config/config.yaml");

		if (Files.isReadable(config)) {
			try {

				// Load configs form YAML
				final ClientConfig conf = ClientConfig.loadFromFile(config);

				// create an MQTT client
				final HiveBrokerClient hiveclient = new HiveBrokerClient(conf);

                // Initialize Metrics Collector endpoint for Prometheus //TODO authenticate with Prometheus
				HTTPServer server;
				server = new HTTPServer.Builder()
							.withPort(9091) //TODO make it configurable in yaml file
							.build();

				hiveclient.getBrokerClient().toAsync().connect().thenAccept(ack ->
						hiveclient.getBrokerClient()
						.toAsync()
						.subscribeWith()
						.topicFilter(conf.getTopics().get(0).getSubscribeTopic()) //TODO
						.qos(conf.getTopics().get(0).getConfiguredQos())
						.callback(new AADIXMLCallback(hiveclient,ack)::accept)
						.executor(Executors.newSingleThreadExecutor())
						.send());

				//Messages can be consumed globally set a callback that is called when a message is received (using the async API style)
				/*hiveclient.getBrokerClient().toAsync().publishes(ALL, new AADIXMLCallback(hiveclient,connAck)::accept,
						Executors.newSingleThreadExecutor()); //validates each new message in a new thread*/


			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static void logBrokerConnection(Mqtt5ConnAck connAck){
		System.out.println("Connected to Broker "+connAck); //TODO observability - Log/Event
	}

}
