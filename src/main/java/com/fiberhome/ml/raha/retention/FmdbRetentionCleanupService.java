package com.fiberhome.ml.raha.retention;

import com.fiberhome.ml.raha.audit.RahaAuditAction;
import com.fiberhome.ml.raha.audit.RahaAuditService;
import com.fiberhome.ml.raha.audit.RahaAuditStatus;
import com.fiberhome.ml.raha.fmdb.FmdbTableGateway;
import com.fiberhome.ml.raha.security.RahaAccessController;
import com.fiberhome.ml.raha.security.RahaPermissionAction;
import com.fiberhome.ml.raha.security.RahaPermissionChecker;
import com.fiberhome.ml.raha.security.RahaPermissionRequest;
import com.fiberhome.ml.raha.security.RahaResourceType;
import com.fiberhome.ml.raha.util.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 按表级保留规则清理 FMDB 过期中间结果，并执行权限校验和操作审计。
 */
public final class FmdbRetentionCleanupService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbRetentionCleanupService.class);
    /** FMDB 表操作网关。 */
    private final FmdbTableGateway tableGateway;
    /** 不可变表级保留规则。 */
    private final List<RetentionTableRule> rules;
    /** 清理操作访问控制器。 */
    private final RahaAccessController accessController;
    /** 清理操作审计服务。 */
    private final RahaAuditService auditService;
    /** 提供可测试清理时间的时钟。 */
    private final Clock clock;

    public FmdbRetentionCleanupService(FmdbTableGateway tableGateway,
                                       List<RetentionTableRule> rules,
                                       RahaPermissionChecker permissionChecker,
                                       RahaAuditService auditService,
                                       Clock clock) {
        if (tableGateway == null || rules == null || rules.isEmpty()
                || permissionChecker == null || auditService == null || clock == null) {
            throw new IllegalArgumentException("FMDB 保留清理服务依赖和规则不能为空");
        }
        this.tableGateway = tableGateway;
        this.rules = new ArrayList<RetentionTableRule>(rules);
        this.accessController = new RahaAccessController(permissionChecker);
        this.auditService = auditService;
        this.clock = clock;
    }

    /**
     * 清理调用方有权限访问且已经超过保留天数的中间结果。
     */
    public RetentionCleanupResult cleanup(String actor, String datasetId) {
        String validatedActor = ValueUtils.requireNotBlank(actor, "清理调用方");
        String validatedDataset = ValueUtils.requireNotBlank(datasetId, "清理数据集标识");
        long now = Math.max(1L, clock.millis());
        Map<String, Long> deleted = new LinkedHashMap<String, Long>();
        LOGGER.info("开始执行 FMDB 过期结果清理，datasetId={}，ruleCount={}",
                validatedDataset, rules.size());
        for (RetentionTableRule rule : rules) {
            accessController.requireAllowed(new RahaPermissionRequest(
                    validatedActor, RahaPermissionAction.CLEANUP,
                    RahaResourceType.INTERMEDIATE_DATA, rule.getTableName(),
                    validatedDataset));
            if (!tableGateway.tableExists(rule.getTableName())) {
                LOGGER.warn("FMDB 保留规则目标表不存在，跳过清理，tableName={}",
                        rule.getTableName());
                deleted.put(rule.getTableName(), 0L);
                continue;
            }
            try {
                long count = tableGateway.deleteOlderThan(rule.getTableName(),
                        rule.getTimestampColumn(), rule.cutoff(now));
                deleted.put(rule.getTableName(), count);
                auditService.record(validatedActor,
                        RahaAuditAction.RETENTION_CLEANUP,
                        RahaAuditStatus.SUCCEEDED,
                        RahaResourceType.INTERMEDIATE_DATA, rule.getTableName(),
                        validatedDataset, null, null,
                        "过期中间结果清理成功，删除数量=" + count);
            } catch (RuntimeException exception) {
                // 单表清理失败必须立即终止，避免输出部分成功却被误判为整批完成。
                LOGGER.error("FMDB 过期结果清理失败，datasetId={}，tableName={}",
                        validatedDataset, rule.getTableName(), exception);
                auditService.record(validatedActor,
                        RahaAuditAction.RETENTION_CLEANUP,
                        RahaAuditStatus.FAILED,
                        RahaResourceType.INTERMEDIATE_DATA, rule.getTableName(),
                        validatedDataset, null, null, "过期中间结果清理失败");
                throw exception;
            }
        }
        RetentionCleanupResult result = new RetentionCleanupResult(deleted);
        LOGGER.info("FMDB 过期结果清理完成，datasetId={}，totalDeleted={}",
                validatedDataset, result.getTotalDeleted());
        return result;
    }
}
