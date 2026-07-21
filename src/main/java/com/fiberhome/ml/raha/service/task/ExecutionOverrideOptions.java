package com.fiberhome.ml.raha.service.task;

/**
 * 保存一次任务请求的执行覆盖选项。
 *
 * <p>该对象只表达调用方是否要求跳出默认幂等域，以及是否提供本次强制运行的稳定标识。
 * 真正参与哈希计算的运行标识由 {@link ExecutionFingerprint} 在请求工厂阶段统一生成。</p>
 */
public final class ExecutionOverrideOptions {

    /** 默认不强制运行的覆盖选项。 */
    public static final ExecutionOverrideOptions DEFAULT =
            new ExecutionOverrideOptions(false, null);

    /** 是否强制生成新的最终执行输入指纹。 */
    private final boolean forceRun;
    /** 调用方指定的强制运行幂等标识，为空时由系统生成一次性运行标识。 */
    private final String forceRunId;

    public ExecutionOverrideOptions(boolean forceRun, String forceRunId) {
        String normalizedForceRunId = trimToNull(forceRunId);
        if (!forceRun && normalizedForceRunId != null) {
            throw new IllegalArgumentException(
                    "forceRunId 只有在 forceRun=true 时才允许传入");
        }
        this.forceRun = forceRun;
        this.forceRunId = normalizedForceRunId;
    }

    public boolean isForceRun() {
        return forceRun;
    }

    public String getForceRunId() {
        return forceRunId;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
