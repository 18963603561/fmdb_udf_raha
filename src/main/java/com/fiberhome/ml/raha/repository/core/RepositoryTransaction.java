package com.fiberhome.ml.raha.repository.core;

/**
 * 统一仓储事务回调，回调抛出运行时异常时开发期仓储回滚全部写入。
 */
public interface RepositoryTransaction {

    /**
     * 在仓储事务边界内执行写入。
     *
     * @param repository 当前事务仓储
     */
    void execute(RahaRepository repository);
}

