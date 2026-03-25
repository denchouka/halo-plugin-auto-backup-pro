package cn.wangwenzhu.autobackup.cos;

import lombok.Data;

@Data
public class CosConfig {

    private boolean enabled;
    private String region;
    private String secretId;
    private String secretKey;
    private String bucketName;
    private String uploadPath;
    private boolean deleteOld;
    private int maxCosBackupCount;

    public CosConfig() {
    }

    public CosConfig(boolean enabled, String region, String secretId, String secretKey, 
                     String bucketName, String uploadPath, boolean deleteOld, int maxCosBackupCount) {
        this.enabled = enabled;
        this.region = region;
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.bucketName = bucketName;
        this.uploadPath = uploadPath;
        this.deleteOld = deleteOld;
        this.maxCosBackupCount = maxCosBackupCount;
    }
}
