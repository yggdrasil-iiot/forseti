package yggdrasil.forseti.bridge;

import java.util.function.LongSupplier;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/** The MQTT side (Paho v3): a clean-session subscriber to the whole Sparkplug namespace at QoS 1, so NDEATH and the Will (published at
 *  QoS 1) arrive at least once and data arrives as published (QoS 0). Each message becomes a {@link Frame} stamped with the bridge's
 *  receipt time and is handed to the {@link FrameQueue}; nothing is decoded on the callback thread. No automatic reconnect: frames lost
 *  while disconnected would leave no trace, so a lost connection fails the run. */
public final class MqttSource implements AutoCloseable {
    public static final String FILTER = SparkplugTopic.NAMESPACE + "/#";
    private final MqttClient client;

    public MqttSource(String broker, String clientId, FrameQueue queue, LongSupplier clock) throws MqttException {
        client = new MqttClient(broker, clientId, new MemoryPersistence());
        client.setCallback(new MqttCallback() {
            @Override public void connectionLost(Throwable cause) { queue.fail(new BridgeFailure("MQTT connection lost", cause)); }
            @Override public void messageArrived(String topic, MqttMessage m) { queue.offer(new Frame(topic, m.getPayload(), clock.getAsLong())); }
            @Override public void deliveryComplete(IMqttDeliveryToken token) {}
        });
        MqttConnectOptions o = new MqttConnectOptions();
        o.setCleanSession(true);
        o.setAutomaticReconnect(false);
        client.connect(o);
        client.subscribe(FILTER, 1);
    }

    @Override public void close() throws MqttException {
        if (client.isConnected()) client.disconnect();
        client.close();
    }
}
