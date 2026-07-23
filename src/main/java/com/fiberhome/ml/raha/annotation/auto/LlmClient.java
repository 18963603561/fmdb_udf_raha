package com.fiberhome.ml.raha.annotation.auto;

/**
 * 抽象大模型调用边界，便于生产使用 HTTP 并在测试中使用模拟客户端。
 */
public interface LlmClient {

    /**
     * 调用模型并返回模型消息中的 JSON 正文。
     *
     * @param systemPrompt 系统提示词
     * @param userPrompt 用户 JSON 请求
     * @return 模型 JSON 正文
     */
    String complete(String systemPrompt, String userPrompt);
}
