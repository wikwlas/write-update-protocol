package com.example.node.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.util.concurrent.TimeUnit;

/**
 * Configuration class for WebClient.
 * Creates a WebClient.Builder factory for asynchronous and non-blocking HTTP REST
 * requests to other nodes.
 */
@Configuration
public class WebClientConfig {

    /**
     * Registers a bean providing WebClient.Builder with strict timeouts.
     * The Builder pattern is used because base URLs are inserted dynamically
     * depending on which node (C# or Python) receives the current request.
     */
    @Bean
    public WebClient.Builder webClientBuilder() {
        // Configure the low-level HttpClient from Reactor Netty.
        HttpClient httpClient = HttpClient.create()
                // Maximum time to establish the physical TCP connection (1 second).
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                // Configure response timeouts at the network pipeline level.
                .doOnConnected(conn -> conn
                        // Maximum time to wait for the next incoming network packet (1 second).
                        .addHandlerLast(new ReadTimeoutHandler(1000, TimeUnit.MILLISECONDS))
                        // Maximum time to write data through the network socket (1 second).
                        .addHandlerLast(new WriteTimeoutHandler(1000, TimeUnit.MILLISECONDS)));

        // Return the configured builder wrapped with a Reactor Netty connector.
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient));
    }
}
