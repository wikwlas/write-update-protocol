package com.example.node.model;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Klasa reprezentująca tożsamość i stan bieżącego węzła w systemie rozproszonym.
 * Klasa ta działa jako singleton (zarządzany przez Springa komponent @Component),
 * do którego dostęp mają serwisy sieciowe, algorytm Bully oraz warstwa kontrolera.
 */
@Slf4j
@Component
@Getter
@Setter
public class SystemNode {

    // Unikalne ID węzła wstrzykiwane z konfiguracji (dla Javy/Windows domyślnie 3)
    @Value("${node.id:3}")
    private int nodeId;

    // Adres IP oraz port HTTP tego węzła (wstrzykiwane z application.properties)
    @Value("${node.ip:localhost}")
    private String nodeIp;

    @Value("${server.port:8083}")
    private int httpPort;

    // Identyfikator aktualnego Lidera (Home Node) w sieci
    private volatile int leaderId;

    // Flaga określająca, czy ten konkretny węzeł jest aktualnie Liderem
    private volatile boolean isLeader = false;

    // Stan węzła w klastrze (np. "NORMAL", "ELECTION", "RECOVERY")
    private volatile String state = "NORMAL";

    /**
     * Bezpieczna dla wątków, niezmienialna zewnętrznie mapa pozostałych węzłów (Peers).
     * Klucz: ID węzła (1 dla C#, 2 dla Pythona)
     * Wartość: Adres bazowy HTTP (np. "http://192.168.1.50:8082")
     */
    private Map<Integer, String> peers;

    // Surowe właściwości konfiguracyjne wstrzykiwane ze Springa w celu zbudowania mapy peers
    @Value("${peers.node1.url:http://localhost:8081}")
    private String node1Url;

    @Value("${peers.node2.url:http://localhost:8082}")
    private String node2Url;

    /**
     * Metoda inicjalizacyjna, uruchamiana automatycznie po wstrzyknięciu zależności przez Springa.
     * Buduje mapę rówieśników sieciowych na podstawie pliku konfiguracyjnego.
     */
    @PostConstruct
    public void init() {
        Map<Integer, String> tempPeers = new HashMap<>();

        // Mapujemy adresy zgodnie z architekturą (Węzeł 1 = C#/.NET, Węzeł 2 = Python/FastAPI)
        if (nodeId != 1) tempPeers.put(1, node1Url);
        if (nodeId != 2) tempPeers.put(2, node2Url);

        // Opakowujemy w unmodifiableMap dla bezpieczeństwa wątkowego (mapa jest tylko do odczytu)
        this.peers = Collections.unmodifiableMap(tempPeers);

        // Na starcie systemu zakładamy domyślnego lidera z planu projektu (Węzeł 3)
        // Jeśli stan sieci ulegnie zmianie, algorytm Bully to skoryguje
        this.leaderId = 3;
        if (this.nodeId == 3) {
            this.isLeader = true;
            log.info("Węzeł {} zainicjalizowany jako początkowy Lider (Home Node).", nodeId);
        } else {
            log.info("Węzeł {} zainicjalizowany jako Follower. Aktualny lider: Węzeł {}", nodeId, leaderId);
        }
    }

    /**
     * Sprawdza, czy dany węzeł ma wyższy priorytet (ID) niż węzeł bieżący.
     * Wykorzystywane bezpośrednio w logice komunikatów algorytmu Bully.
     */
    public boolean hasHigherPriorityThan(int otherNodeId) {
        return this.nodeId > otherNodeId;
    }

    /**
     * Bezpieczna aktualizacja roli lidera w węźle.
     * Wywoływana, gdy algorytm Bully zakończy wybory lub gdy nadejdzie komunikat o nowym liderze.
     */
    public synchronized void updateLeader(int newLeaderId) {
        this.leaderId = newLeaderId;
        this.isLeader = (this.nodeId == newLeaderId);
        this.state = "NORMAL";
        log.info("Stan węzła zaktualizowany. Nowy lider w sieci: Węzeł {}. Czy ja jestem liderem: {}", newLeaderId, isLeader);
    }
}
