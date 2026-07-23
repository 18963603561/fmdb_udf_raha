package com.fiberhome.ml.raha.annotation.auto;

import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用 Java 8 HttpURLConnection 调用 OpenAI 兼容的聊天补全接口。
 */
public final class OpenCompatibleLlmClient implements LlmClient {

    /** 日志记录器，不记录密钥和业务请求正文。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            OpenCompatibleLlmClient.class);
    /** 模型配置。 */
    private final AutoAnnotationConfig config;

    public OpenCompatibleLlmClient(AutoAnnotationConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("大模型配置不能为空");
        }
        this.config = config;
    }

    @Override
    @SuppressWarnings("unchecked")
    public String complete(String systemPrompt, String userPrompt) {
        HttpURLConnection connection = null;
        long startedAt = System.currentTimeMillis();
        try {
            URL url = new URL(config.getModelUrl());
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(config.getBatchTimeoutMillis());
            connection.setReadTimeout(config.getBatchTimeoutMillis());
            // 禁止携带密钥跨地址自动重定向，重定向响应按普通失败处理。
            connection.setInstanceFollowRedirects(false);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Authorization",
                    "Bearer " + config.getApiKey());
            Map<String, Object> body = new LinkedHashMap<String, Object>();
            body.put("model", config.getModel());
            body.put("temperature", Double.valueOf(config.getTemperature()));
            body.put("response_format", singleton("type", "json_object"));
            body.put("max_tokens", Integer.valueOf(config.getMaxOutputTokens()));
            List<Map<String, String>> messages =
                    new ArrayList<Map<String, String>>();
            messages.add(message("system", systemPrompt));
            messages.add(message("user", userPrompt));
            body.put("messages", messages);
            byte[] requestBytes = FmdbJsonCodec.write(body)
                    .getBytes(StandardCharsets.UTF_8);
            LOGGER.debug("开始调用自动标注模型，batchRequestBytes={}，model={}",
                    requestBytes.length, config.getModel());
            try (OutputStream output = connection.getOutputStream()) {
                output.write(requestBytes);
            }
            int status = connection.getResponseCode();
            InputStream stream = status >= 200 && status < 300
                    ? connection.getInputStream() : connection.getErrorStream();
            String response = readLimited(stream, config.getMaxResponseBytes());
            if (status < 200 || status >= 300) {
                // 外部服务错误正文可能回显请求内容，因此只记录状态码。
                throw new IOException("模型接口 HTTP 状态码 " + status);
            }
            Map<String, Object> root;
            try {
                root = FmdbJsonCodec.readObject(response);
            } catch (IllegalArgumentException exception) {
                // 不把解析器可能附带的外部响应片段放入异常链。
                throw new IOException("模型接口响应 JSON 格式无效");
            }
            Object choices = root.get("choices");
            if (!(choices instanceof List) || ((List<?>) choices).isEmpty()) {
                throw new IOException("模型响应缺少 choices");
            }
            Object first = ((List<?>) choices).get(0);
            if (!(first instanceof Map)) {
                throw new IOException("模型响应 choices 元素无效");
            }
            Object message = ((Map<String, Object>) first).get("message");
            if (!(message instanceof Map)) {
                throw new IOException("模型响应缺少 message");
            }
            String content = String.valueOf(
                    ((Map<String, Object>) message).get("content"));
            LOGGER.debug("自动标注模型调用完成，httpStatus={}，responseBytes={}，elapsedMillis={}",
                    status, response.getBytes(StandardCharsets.UTF_8).length,
                    System.currentTimeMillis() - startedAt);
            return content;
        } catch (IOException exception) {
            LOGGER.warn("自动标注模型调用失败，model={}，elapsedMillis={}",
                    config.getModel(), System.currentTimeMillis() - startedAt,
                    exception);
            throw new IllegalStateException("自动标注模型调用失败", exception);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static Map<String, String> message(String role, String content) {
        Map<String, String> result = new LinkedHashMap<String, String>();
        result.put("role", role);
        result.put("content", content == null ? "" : content);
        return result;
    }

    private static Map<String, String> singleton(String key, String value) {
        Map<String, String> result = new LinkedHashMap<String, String>();
        result.put(key, value);
        return result;
    }

    private static String readLimited(InputStream stream, int maximumBytes)
            throws IOException {
        if (stream == null) {
            return "";
        }
        try (InputStream input = stream;
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int total = 0;
            int count;
            while ((count = input.read(buffer)) >= 0) {
                total += count;
                if (total > maximumBytes) {
                    throw new IOException("模型响应超过配置大小限制");
                }
                output.write(buffer, 0, count);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }

}
