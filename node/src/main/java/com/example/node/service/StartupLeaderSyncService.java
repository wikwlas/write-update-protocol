package com.example.node.service;

import com.example.node.model.SystemNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class StartupLeaderSyncService {

    private final SystemNode systemNode;
    private final WebClient.Builder webClientBuilder;

    @EventListener(ApplicationReadyEvent.class)
    public void synchronizeLeaderAfterStartup() {
        for (Map.Entry<Integer, String> peer : systemNode.getPeers().entrySet()) {
            try {
                Map<?, ?> status = webClientBuilder.build()
                        .get()
                        .uri(peer.getValue() + "/status")
                        .retrieve()
                        .bodyToMono(Map.class)
                        .block(Duration.ofMillis(1000));

                if (status == null || !status.containsKey("leaderId")) {
                    continue;
                }

                Object leaderIdValue = status.get("leaderId");
                if (leaderIdValue instanceof Number leaderId) {
                    systemNode.updateLeader(leaderId.intValue());
                    log.info("Synchronized current leader from Node {} status: Node {}", peer.getKey(), leaderId.intValue());
                    return;
                }
            } catch (Exception exc) {
                log.debug("Could not synchronize leader from Node {} status: {}", peer.getKey(), exc.getMessage());
            }
        }

        log.info("Could not synchronize leader from peers after startup. Keeping local leader state: Node {}", systemNode.getLeaderId());
    }
}
