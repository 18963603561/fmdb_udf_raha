package com.fiberhome.ml.raha.udf;

/**
 * 保存由 FMDB 函数注册进程初始化的 UDF 任务提交器。
 */
public final class RahaUdfRuntime {

    /** 当前执行进程共享的任务提交器。 */
    private static volatile RahaUdfJobSubmitter submitter;

    private RahaUdfRuntime() {
    }

    /**
     * 在注册三个表级函数前配置任务提交器。
     */
    public static synchronized void configure(RahaUdfJobSubmitter value) {
        if (value == null) {
            throw new IllegalArgumentException("UDF 运行时提交器不能为空");
        }
        submitter = value;
    }

    /**
     * 测试或进程关闭时清理运行时引用。
     */
    public static synchronized void clear() {
        submitter = null;
    }

    static RahaUdfJobSubmitter requireSubmitter() {
        RahaUdfJobSubmitter current = submitter;
        if (current == null) {
            throw new RahaUdfException("UDF_RUNTIME_UNAVAILABLE",
                    "Raha UDF 运行时尚未配置任务提交器");
        }
        return current;
    }
}
