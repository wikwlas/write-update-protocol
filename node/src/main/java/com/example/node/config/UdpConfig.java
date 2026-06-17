package com.example.node.config;

import com.example.node.service.UdpHeartbeatReceiver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.ip.udp.UnicastReceivingChannelAdapter;

/**
 * Configuration class responsible for starting the UDP server.
 * This server listens for asynchronous heartbeat signals sent by the remaining nodes
 * in the P2P network.
 */
@Configuration
public class UdpConfig {

    // UDP port injected from application.properties, for example udp.port=4444.
    @Value("${udp.port:4444}")
    private int udpPort;

    /**
     * Configures the UDP inbound channel adapter.
     * The adapter opens a socket on the selected port and listens for packets.
     */
    @Bean
    public UnicastReceivingChannelAdapter udpInboundAdapter() {
        UnicastReceivingChannelAdapter adapter = new UnicastReceivingChannelAdapter(udpPort);
        // Set a receive buffer size suitable for small heartbeat packets.
        adapter.setReceiveBufferSize(1024);
        return adapter;
    }

    /**
     * Defines the processing pipeline (Integration Flow) for received UDP packets.
     * Converts the raw packet to String and passes it to the dedicated service.
     *
     * @param udpInboundAdapter adapter receiving UDP packets
     * @param heartbeatReceiver service processing received heartbeat logic
     */
    @Bean
    public IntegrationFlow udpHeartbeatFlow(UnicastReceivingChannelAdapter udpInboundAdapter,
                                            UdpHeartbeatReceiver heartbeatReceiver) {
        return IntegrationFlow.from(udpInboundAdapter)
                // Convert payload from byte[] to String for readable message formats.
                .transform(byte[].class, String::new)
                // Pass the processed text message to the receiver service method.
                .handle(String.class, (payload, headers) -> {
                    heartbeatReceiver.processHeartbeat(payload);
                    return null; // The stream ends here (fire-and-forget).
                })
                .get();
    }
}
