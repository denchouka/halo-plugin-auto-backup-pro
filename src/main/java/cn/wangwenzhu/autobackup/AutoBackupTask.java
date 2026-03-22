package cn.wangwenzhu.autobackup;

import static run.halo.app.extension.index.query.Queries.empty;

import cn.wangwenzhu.autobackup.cos.CosConfig;
import cn.wangwenzhu.autobackup.scheduled.AbstractReschedulingConfigurer;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.PutObjectResult;
import com.qcloud.cos.region.Region;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.config.Task;
import org.springframework.scheduling.config.TriggerTask;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.stereotype.Component;
import run.halo.app.extension.ExtensionClient;
import run.halo.app.extension.ListOptions;
import run.halo.app.extension.Metadata;
import run.halo.app.infra.BackupRootGetter;
import run.halo.app.migration.Backup;
import run.halo.app.plugin.SettingFetcher;

@Log4j2
@Component
public class AutoBackupTask extends AbstractReschedulingConfigurer {

    private final BackupRootGetter backupRootGetter;

    private final static Map<String, ChronoUnit> timeUnitMapping = Map.of(
        "HOUR", ChronoUnit.HOURS,
        "DAY", ChronoUnit.DAYS
    );

    private final ExtensionClient client;

    private final SettingFetcher settingFetcher;

    public AutoBackupTask(BackupRootGetter backupRootGetter, ExtensionClient client, SettingFetcher settingFetcher) {
        super(true);
        this.backupRootGetter = backupRootGetter;
        this.client = client;
        this.settingFetcher = settingFetcher;
    }

    private void autoBackup() {
        log.info("Auto backup task started");

        String seq = Long.toHexString(System.currentTimeMillis() / 1000);

        var metadata = new Metadata();
        metadata.setName("backup-" + seq);
        var backup = new Backup();
        backup.setMetadata(metadata);
        backup.getSpec().setFormat("zip");

        Date expiresAt = DateUtils.addDays(new Date(), 7);
        backup.getSpec().setExpiresAt(expiresAt.toInstant());

        client.create(backup);

        Optional<AutoBackupConfig> config =
            settingFetcher.fetch("base", AutoBackupConfig.class);

        if (config.isPresent()) {
            int maxBackupCount = config.get().getMaxBackupCount();
            client.listAll(
                    Backup.class,
                    ListOptions.builder().fieldQuery(empty()).build(),
                    Sort.by(Sort.Order.desc("metadata.creationTimestamp"))
                ).stream()
                .skip(maxBackupCount)
                .forEach(client::delete);
            
            // 上传到COS
            uploadToCos(seq);
        }

        log.info("Auto backup task finished");
    }

    /**
     * 上传到COS
     * @param seq
     */
    private void uploadToCos(String seq) {

        try {

            Optional<CosConfig> config = settingFetcher.fetch("cos", CosConfig.class);
            CosConfig cosConfig = config.get();
            if (!cosConfig.isEnabled()) {
                log.debug("COS upload is disabled");
                return;
            }

            // 验证必要参数
            if (cosConfig.getRegion() == null || cosConfig.getRegion().isBlank() ||
                cosConfig.getSecretId() == null || cosConfig.getSecretId().isBlank() ||
                cosConfig.getSecretKey() == null || cosConfig.getSecretKey().isBlank() ||
                cosConfig.getBucketName() == null || cosConfig.getBucketName().isBlank() ||
                cosConfig.getUploadPath() == null || cosConfig.getUploadPath().isBlank()) {
                log.warn("COS configuration is incomplete, skipping upload");
                return;
            }

            // 等待备份完成
            Backup completedBackup = waitForBackupCompletion(seq);
            if (completedBackup == null) {
                log.error("Backup completion check failed");
                return;
            }

            // 获取备份创建时间
            Instant creationTimestampInstant = completedBackup.getMetadata().getCreationTimestamp();
            // 转换成yyyyMMddHHmmss格式
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("Asia/Shanghai"));
            String creationTimestamp = formatter.format(creationTimestampInstant);

            // 上传路径
            String uploadPath = cosConfig.getUploadPath();
            if (!uploadPath.endsWith("/")) {
                uploadPath = uploadPath + "/";
            }

            // 获取当前年份和月份
            LocalDate now = LocalDate.now();
            String year = String.valueOf(now.getYear());
            String month = String.format("%02d", now.getMonthValue());

            // 构造备份文件名（和实际备份文件名一致）
            String backupFileName = creationTimestamp + "-" + completedBackup.getMetadata().getName() + ".zip";
            // 上传到COS的路径
            String key = uploadPath + year + "/" + month + "/" + backupFileName;

            // 获取备份文件路径（需要从 Backup 资源中获取）
            Path backupFilePath = getBackupFilePath(backupFileName);
            if (backupFilePath == null || !Files.exists(backupFilePath)) {
                log.error("Backup file not found at expected location");
                return;
            }

            // 创建 COS 客户端
            COSCredentials cred = new BasicCOSCredentials(
                cosConfig.getSecretId(),
                cosConfig.getSecretKey()
            );
            
            ClientConfig clientConfig = new ClientConfig(
                new Region(cosConfig.getRegion())
            );
            clientConfig.setHttpProtocol(HttpProtocol.https);
            
            COSClient cosClient = new COSClient(cred, clientConfig);
            try {
                // 上传文件
                File localFile = backupFilePath.toFile();
                PutObjectRequest putObjectRequest = new PutObjectRequest(
                    cosConfig.getBucketName(),
                    key, 
                    localFile
                );
                
                PutObjectResult result = cosClient.putObject(putObjectRequest);
                
                log.info("Successfully uploaded {} to COS. Path: {}, ETag: {}", backupFileName, key, result.getETag());
            } finally {
                cosClient.shutdown();
            }

        } catch (Exception e) {
            log.error("Failed to upload backup to COS", e);
        }
    }

    /**
     * 等待备份完成
     * @param seq
     * @return
     * @throws InterruptedException
     */
    private Backup waitForBackupCompletion(String seq) throws InterruptedException {
        // 等待备份完成，最多等待 5 分钟
        int maxAttempts = 300;
        int attempt = 0;

        while (attempt < maxAttempts) {
            Optional<Backup> backupOpt = client.listAll(
                    Backup.class,
                    ListOptions.builder().fieldQuery(empty()).build(),
                    Sort.by(Sort.Order.desc("metadata.creationTimestamp"))
                ).stream()
                .filter(b -> b.getMetadata().getName().contains(seq))
                .findFirst();

            if (backupOpt.isPresent()) {
                Backup backup = backupOpt.get();
                // 检查备份状态
                if (backup.getStatus() != null && backup.getStatus().getPhase() == Backup.Phase.SUCCEEDED) {
                    return backup;
                }
            }

            Thread.sleep(1000);
            attempt++;
        }

        return null;
    }

    /**
     * 获取备份文件路径
     * @param backupFileName
     * @return
     */
    private Path getBackupFilePath(String backupFileName) {

        // 备份文件路径
        Path backupDir = backupRootGetter.get();
        if (Files.exists(backupDir)) {
            Path backupFile = backupDir.resolve(backupFileName);
            
            if (Files.exists(backupFile)) {
                return backupFile;
            }
        }
        
        return null;
    }

    @Override
    public Task configureTask() {
        return new TriggerTask(this::autoBackup, triggerContext -> {

            Optional<AutoBackupConfig> config =
                settingFetcher.fetch("base", AutoBackupConfig.class);

            if (config.isPresent()) {
                String timeUnit = config.get().getTimeUnit();
                int interval = config.get().getInterval();

                PeriodicTrigger trigger = new PeriodicTrigger(
                    Duration.of(interval, timeUnitMapping.get(timeUnit))
                );
                return trigger.nextExecution(triggerContext);
            }
            return null;
        });
    }
}
