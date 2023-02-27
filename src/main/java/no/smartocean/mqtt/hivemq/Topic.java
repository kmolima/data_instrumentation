package no.smartocean.mqtt.hivemq;

import com.hivemq.client.mqtt.datatypes.MqttQos;

// Class automatically factored by yaml parser constructor
public class Topic {

    private String subscribeTopic;
    private int qos;

    /* Changes qos in yaml config into MqttQos Enum
     * https://hivemq.github.io/hivemq-mqtt-client/docs/mqtt-operations/publish/#quality-of-service-qos
     */
    public MqttQos getConfiguredQos() {
        this.qos =  qos < 0 && qos > 2? 1: qos; //0 - AT_MOST_ONCE  | 1 - AT_MOST_ONCE | 2 - EXACTLY_ONCE
        return MqttQos.fromCode(this.qos);
    }

    public int getQos() {return  this.qos;};

    protected void setQos(int qos) {
        this.qos = qos;
    }

    public String getSubscribeTopic() {
        return subscribeTopic;
    }

    protected void setSubscribeTopic(String subscribe_topic) {
        this.subscribeTopic = subscribe_topic;
    }
}
