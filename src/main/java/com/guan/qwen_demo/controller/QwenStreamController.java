package com.guan.qwen_demo.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Qwen 大模型接口控制器。
 *
 * <p>负责接收前端聊天请求，调用 DashScope 兼容 OpenAI 的聊天接口，
 * 并将模型生成结果以 SSE 方式流式返回给浏览器。</p>
 */
@RestController
@RequestMapping("/ai")
public class QwenStreamController {
    @Value("${qwen.api-key}")
    private String apiKey;

    @Value("${qwen.url}")
    private String url;

    private final WebClient webClient = WebClient.builder().build();
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Qwen 流式聊天接口。
     *
     * <p>前端通过 EventSource 请求该接口，后端开启模型的 stream 模式，
     * 逐段解析模型返回的增量内容，并实时推送给前端页面。</p>
     *
     * @param msg 用户输入的问题
     * @return 模型生成的流式文本片段
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> stream(@RequestParam String msg) {
        Map<String, Object> body = new HashMap<>();
        body.put("model", "qwen-plus");
        body.put("stream", true);

        List<Map<String, String>> messages = new ArrayList<>();
        Map<String, String> user = new HashMap<>();
        user.put("role", "user");
        user.put("content", msg);
        messages.add(user);
        body.put("messages", messages);

        return webClient.post()
                .uri(url)
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(body)
                .retrieve()
                .bodyToFlux(String.class)
                .flatMap(this::parseStreamChunk)
                .onErrorResume(e -> Flux.just("请求 Qwen 接口失败：" + e.getMessage()));
    }

    /**
     * 解析 DashScope 返回的流式数据片段，并提取 choices[].delta.content。
     *
     * <p>WebClient 可能返回原始 SSE 行，例如 "data: {...}"；
     * 也可能返回已经解码后的 JSON 字符串，例如 "{...}"。
     * 这里同时兼容这两种格式。</p>
     */
    private Flux<String> parseStreamChunk(String chunk) {
        List<String> results = new ArrayList<>();
        String[] lines = chunk.split("\\r?\\n");

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            String data = line.startsWith("data:")
                    ? line.substring(5).trim()
                    : line;
            if (data.isEmpty() || "[DONE]".equals(data)) {
                continue;
            }

            try {
                JsonNode choices = mapper.readTree(data).path("choices");
                if (!choices.isArray()) {
                    continue;
                }

                for (JsonNode choice : choices) {
                    JsonNode content = choice.path("delta").path("content");
                    if (!content.isMissingNode() && !content.asText().isEmpty()) {
                        results.add(content.asText());
                    }
                }
            } catch (Exception e) {
                results.add("解析模型响应失败：" + e.getMessage());
            }
        }

        return Flux.fromIterable(results);
    }
}
