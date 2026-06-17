package com.example.node.controller;

import com.example.node.dto.CacheUpdateRequest;
import com.example.node.dto.ElectionMessage;
import com.example.node.model.DirectoryManager;
import com.example.node.model.LocalCache;
import com.example.node.model.SystemNode;
import com.example.node.service.BullyElectionService;
import com.example.node.service.ReplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Główny kontroler REST aplikacji (odpowiednik komponentu RestAPI z projektu).
 * Obsługuje żądania od klientów oraz komunikację P2P z pozostałymi węzłami (C# i Python).
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class CacheRestController {

    private final SystemNode systemNode;
    private final LocalCache localCache;
    private final DirectoryManager directoryManager;
    private final ReplicationService replicationService;
    private final BullyElectionService electionService;
    private final org.springframework.web.reactive.function.client.WebClient.Builder webClientBuilder;

    /**
     * Endpoint wywoływany przez lokalnego użytkownika/aplikację w celu zapisu lub modyfikacji zmiennej.
     * Realizuje strategię opisaną na diagramie sekwencji w planie projektu.
     */
    @PostMapping("/update-request")
    public ResponseEntity<String> handleUserUpdateRequest(@RequestParam String key, @RequestParam String value) {
        log.info("Otrzymano lokalne żądanie zapisu: {} = {}", key, value);

        if (systemNode.isLeader()) {
            // Sytuacja A: Jesteśmy Liderem (Home Node)
            // 1. Aktualizujemy pamięć główną i rejestrujemy obecność u siebie
            directoryManager.updateMainMemoryValue(key, value);
            directoryManager.registerVariablePresence(key, systemNode.getNodeId());
            localCache.put(key, value);

            // 2. Pobieramy listę innych węzłów posiadających tę zmienną i wysyłamy Write-Update Broadcast
            replicationService.broadcastUpdate(key, value);
            return ResponseEntity.ok("Zmienna zaktualizowana i rozreplikowana przez Lidera.");
        } else {
            int leaderId = systemNode.getLeaderId();
            String leaderUrl = systemNode.getPeers().get(leaderId);

            log.info("Węzeł 3 działa jako Proxy. Przekazuję żądanie do lidera na port {}", leaderId);

            // Synchroniczne (blokujące .block()) oczekiwanie na odpowiedź lidera w imieniu klienta
            String responseFromLeader = webClientBuilder.build()
                    .post()
                    .uri(leaderUrl + "/update-request?key=" + key + "&value=" + value)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block(); // czekamy na wynik, aby oddać go użytkownikowi

            return ResponseEntity.ok("Odpowiedź od Lidera za pośrednictwem Proxy: " + responseFromLeader);
        }
    }

    /**
     * Endpoint wymuszonej aktualizacji (Write-Update) wywoływany przez Lidera w sieci P2P.
     * Zmusza nasz lokalny cache do natychmiastowego nadpisania wartości w sposób bezpieczny wątkowo.
     */
    @PostMapping("/force-update")
    public ResponseEntity<Void> handleForceUpdate(@RequestBody CacheUpdateRequest updateRequest) {
        log.info("Sieć P2P: Otrzymano wymuszoną aktualizację (Write-Update) od Węzła {} dla: {} = {}",
                updateRequest.getSenderNodeId(), updateRequest.getVariableName(), updateRequest.getNewValue());

        // Zapis do pamięci podręcznej chroniony blokadą klasy LocalCache (odpowiedź na Zarzut 4)
        localCache.put(updateRequest.getVariableName(), updateRequest.getNewValue());

        return ResponseEntity.ok().build();
    }

    /**
     * NOWOŚĆ (Naprawa Zarzutu 1 i 5): Endpoint wywoływany przez nowego lidera po wygranych wyborach.
     * Nowy lider prosi nasz węzeł o zrzut lokalnego cache, aby odtworzyć swój "Pusty Katalog".
     */
    @GetMapping("/reconstruct-directory")
    public ResponseEntity<Map<String, String>> handleDirectoryReconstructionRequest() {
        log.info("Sieć P2P: Nowy lider żąda zrzutu lokalnego cache w celu rekonstrukcji globalnego katalogu.");

        // Zwracamy niemodyfikowalną mapę naszego cache [Zmienna -> Wartość]
        return ResponseEntity.ok(localCache.getAll());
    }

    /**
     * Obsługa komunikatów sieciowych protokołu wyboru lidera (Algorytm Bully).
     */
    @PostMapping("/election")
    public ResponseEntity<Boolean> handleElectionMessage(@RequestBody ElectionMessage message) {
        log.info("Algorytm Bully: Otrzymano komunikat typu '{}' od Węzła {}", message.getType(), message.getSenderNodeId());

        switch (message.getType()) {
            case "ELECTION":
                // Jeśli idzie do nas zapytanie od węzła o NIŻSZYM ID, odpowiadamy TRUE (czyli "OK - żyję i przejmuję wybory")
                if (systemNode.hasHigherPriorityThan(message.getSenderNodeId())) {
                    log.info("Ten węzeł ma wyższy priorytet niż Węzeł {}. Odpowiadam ANSWER i uruchamiam własne wybory.", message.getSenderNodeId());

                    // Asynchronicznie odpalamy własne wybory do jeszcze wyższych węzłów
                    // (używamy wątku w tle, aby natychmiast zwrócić odpowiedź HTTP)
                    new Thread(electionService::startElection).start();
                    return ResponseEntity.ok(true);
                }
                return ResponseEntity.ok(false);

            case "COORDINATOR":
                // Silniejszy węzeł ogłosił się nowym Koordynatorem/Liderem sieci
                log.info("Uznano nowego lidera sieci: Węzeł {}", message.getSenderNodeId());
                systemNode.updateLeader(message.getSenderNodeId());
                return ResponseEntity.ok().build();

            default:
                return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Prosty endpoint GET pozwalający użytkownikowi/testerowi sprawdzić zawartość lokalnego cache na danym węźle.
     */
    @GetMapping("/cache/{key}")
    public ResponseEntity<String> getFromCache(@PathVariable String key) {
        String value = localCache.get(key);
        if (value != null) {
            return ResponseEntity.ok(value);
        }
        return ResponseEntity.notFound().build();
    }
}
