package no.smartocean.mqtt.hivemq;

import com.hivemq.client.mqtt.mqtt5.Mqtt5RxClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.prometheus.client.Histogram;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import  no.smartocean.exceptions.IntegrityException;
import  no.smartocean.observability.data.metrics.DataVerifier;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AADIXMLCallback implements Consumer<Mqtt5Publish> {

    private final HiveBrokerClient publishBroker;
    private final boolean connected;

    public AADIXMLCallback(HiveBrokerClient broker,Mqtt5ConnAck connAck) {
        publishBroker = broker;

        //verify session
        if(connAck.getReasonCode().isError()){
            Mqtt5ConnAck conAck = publishBroker.getBrokerClient().toBlocking().connect();
            if(!conAck.getReasonCode().isError()){
                System.out.println("Connected, " + connAck.getReasonCode());
                connected = true;
            }
            else {
                System.out.println("Connection failed, " + connAck.getReasonString());
                connected = false;
            }
        }
        else
            connected = true;
    }

    @Override
    public void accept(Mqtt5Publish mqttPublish) {
        Histogram.Timer timer = AustevollSubscriber.serviceDelayHistogram.labels("DataService","pubsub").startTimer();
        mqttPublish.getPayload().ifPresent(data -> {
            boolean qc_check = true, integrity_check  = true;
            double bytes = data.limit();

            try {
                ByteArrayInputStream xml = new ByteArrayInputStream(UTF_8.decode(data).toString().getBytes());

                //TODO Forward data to next hop on the pipeline if no integrity issues were found
                DataVerifier verifier = new DataVerifier();
                verifier.verifyAADIXMLStr(xml); //Generates Metrics

                xml.reset();  //Resets buffer for new verification
                DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                Document doc = builder.parse(xml);
                pushDelay(doc,Instant.now()); //TODO more accurate arrival time needed?

                //Forward data to next hop on the pipeline if no other restrictive data quality issues were found
                qc_check = this.qcCheck();

                System.out.println(
                        "Received message: " + mqttPublish.getTopic() + " with " + data.limit() + " bytes.");
                }
            catch (IntegrityException | ParserConfigurationException | SAXException | IOException e) {
                integrity_check = false;

                // Bingo New metric
                AustevollSubscriber.integrityCounter.labels("DataService", e.getLocalizedMessage()).inc();
                System.out.println("Integrity exception when handling received payload from topic:" + mqttPublish.getTopic());
                System.err.println(e.getMessage());
                e.printStackTrace();
            }
            finally {
                if(qc_check & integrity_check & connected){
                    ClientConfig conf = publishBroker.getBrokerClientConfig();

                    //reactive API - https://github.com/hivemq/hivemq-mqtt-client#reactive-api
                    Mqtt5RxClient client = publishBroker.getBrokerClient().toRx();

                    Completable publishScenario = client.publish(Flowable.just(Mqtt5Publish.builder()
                            .topic(conf.getTopics().get(0).getPublishTopic())
                            .qos(conf.getTopics().get(0).getConfiguredQos())
                            .payload(data.rewind())
                            .build()))
                            .doOnNext(publishResult ->
                                    observeValidatedMessage(true,publishResult.getPublish().getPayload().get().limit(),timer))
                            .doOnError(throwable ->
                                    observeValidatedMessage(false,bytes,timer))
                            .ignoreElements();

                    // Reactive types can be easily and flexibly combined
                    publishScenario.blockingAwait();
                }
                else{
                    //retained messages from next hop on the pipeline - unusable data instances
                    AustevollSubscriber.retainedHistogram.labels("DataService","AADI").observe(bytes); //TODO label cause
                    //observability - push metrics of the Uservice overhead
                    double v = timer.observeDuration();
                    System.out.println("Data Validation took "+v+" seconds");

                }
            }
        });
    }

    protected void observeValidatedMessage(boolean onNext,double bytes, Histogram.Timer timer){

        if(onNext){
            System.out.println("Published validated data file with "+ bytes+ " bytes.");
            AustevollSubscriber.validatedHistogram.labels("DataService","AADI").observe(bytes);
        }
        else{
            System.err.println("Error publishing validated data file. "); //TODO retry?
            AustevollSubscriber.retainedHistogram.labels("DataService","AADI").observe(bytes); //TODO label cause
        }

        //observability - push metrics of the Uservice overhead
        double v = timer.observeDuration();
        System.out.println("Data Validation took "+v+" seconds");

    }

    /**
     * Calculates delay between data collection time to arrival at this service
     * The time when the data were actually collected from the sensors. This may differ from the timestamp in the Device
     * element
     * @param doc
     * @return seconds - recommended unit in Prometheus time-series database
     */
    protected void pushDelay(Document doc, Instant end){

        NodeList dataL = doc.getElementsByTagName("Time");

        for(int i=0; i< dataL.getLength(); i++){
          Node data =  dataL.item(i);
          if(data.getParentNode().getNodeName().equals("Device")){
              continue;
          }
          if(data.getParentNode().getNodeName().equals("Data")){
              Instant collectionTime = Instant.parse(data.getTextContent());
              Duration timeElapsed = Duration.between(collectionTime, end);

              double exemplar = ((Number) timeElapsed.toSeconds()).doubleValue();

              // Metric
              //this.rtDelayHistogram.observeWithExemplar(exemplar,"DataService");
              AustevollSubscriber.rtDelayHistogram.labels("DataService","AADI").observe(exemplar);
          }

        }
    }

    //Checks QC information in incoming xml data
    protected boolean qcCheck(){
        //TODO
        //"service","qcFlag","retained","provider"
        AustevollSubscriber.qcCounter.labels("DataService","no_qc","AADI").inc(1);
        return true;
    }

    public static void main(String args[]) {

    }
}
