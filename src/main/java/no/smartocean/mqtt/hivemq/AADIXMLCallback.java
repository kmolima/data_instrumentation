package no.smartocean.mqtt.hivemq;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
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

    @Override
    public void accept(Mqtt5Publish mqttPublish) {
        Histogram.Timer timer = AustevollSubscriber.serviceDelayHistogram.labels("DataService","pubsub").startTimer();
        mqttPublish.getPayload().ifPresent(data -> {
            try {
                ByteArrayInputStream xml = new ByteArrayInputStream(UTF_8.decode(data).toString().getBytes());

                //TODO Forward data to next hop on the pipeline if no integrity issues were found
                DataVerifier verifier = new DataVerifier();
                verifier.verifyAADIXMLStr(xml); //Generates Metrics

                xml.reset();  //Resets buffer for new verification
                DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                Document doc = builder.parse(xml);
                pushDelay(doc,Instant.now()); //TODO more accurate arrival time needed?

                //TODO Forward data to next hop on the pipeline if no other restrictive data quality issues were found
                this.qcCheck();

                System.out.println(
                        "Received message: " + mqttPublish.getTopic() + " -> " + xml.toString());
                }
            catch (IntegrityException e) {
                System.out.println("Integrity exception when handling received payload from topic:" + mqttPublish.getTopic());
                System.err.println(e.getMessage());

                // Bingo New metric
                AustevollSubscriber.integrityCounter.labels("DataService",e.getLocalizedMessage(),"true").inc();

                //TODO retain from next hop on the pipeline - unusable data instances

            } catch (ParserConfigurationException | SAXException | IOException e) {
                e.printStackTrace();
            } finally {
                //TODO observability - push metrics of the Uservice overhead
                double v = timer.observeDuration();
                System.out.println("Data Verifier took "+v+" seconds"); //TODO observability - log
            }
        });
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

    protected void qcCheck(){
        //TODO
        //"service","qcFlag","retained","provider"
        AustevollSubscriber.qcCounter.labels("DataService","no_qc","false","AADI").inc();

    }

    public static void main(String args[]) {

    }
}
