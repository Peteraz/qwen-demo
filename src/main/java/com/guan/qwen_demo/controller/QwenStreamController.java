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

/**
 * Qwen 大模型接口控制器。
 *
 * <p>负责接收前端聊天请求，调用 DashScope 兼容 OpenAI 的聊天接口，
 * 并将模型生成结果以 SSE 方式流式返回给浏览器。</p>
 */
@RestController
@RequestMapping("/ai")
public class QwenStreamController {
    @Value("${qwen.api-key:sk-04201bdcab1241e8a11f32374c5a11f9}")
    private String apiKey;

    @Value("${qwen.url:https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions}")
    private String url;

    private final WebClient webClient = WebClient.builder().build();

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
    public Flux<String> stream(String msg) {

        Map<String, Object> body = new HashMap<>();
        body.put("model", "qwen-plus");
        // 开启 DashScope 的流式响应模式。
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
                    // DashScope 返回的是 SSE 格式，这里只解析 data 行中的增量内容。
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
