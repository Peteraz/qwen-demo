package com.guan.qwen_demo.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/ai")
public class QwenStreamController {
    @Value("${qwen.api-key}")
    private String apiKey;

    @Value("${qwen.url}")
    private String url;

    private final WebClient webClient = WebClient.builder().build();

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> stream(String msg) {

        Map<String, Object> body = new HashMap<>();
        body.put("model", "qwen-plus");
        // 寮€鍚祦寮?
        body.put("stream", true);

        List<Map<String, String>> messages = new ArrayList<>();
        Map<String, String> user = new HashMap<>();
        user.put("role", "user");
        user.put("content", msg);
        messages.add(user);
        body.put("messages", messages);

        ObjectMapper mapper = new ObjectMapper();

        return webClient.post()
                .uri(url)
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .bodyValue(body)
                .retrieve()
                .bodyToFlux(String.class)

                .flatMap(chunk -> {
                    List<String> results = new ArrayList<>();
                    // 鍗冮棶 SSE 鍙兘鏈夊琛?
                    String[] lines = chunk.split("\n");
                    for (String line : lines) {
                        line = line.trim();
                        if (!line.startsWith("data:")) continue;

                        String data = line.substring(5).trim();
                        if (data.isEmpty() || "[DONE]".equals(data)) continue;

                        try {
                            JsonNode jsonNode = mapper.readTree(data);
                            JsonNode choices = jsonNode.path("choices");
                            if (choices.isArray()) {
                                for (JsonNode choice : choices) {
                                    JsonNode content = choice.path("delta").path("content");
                                    if (!content.isMissingNode() && !content.asText().isEmpty()) {
                                        results.add(content.asText());
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    return Flux.fromIterable(results);
                });
    }
}
