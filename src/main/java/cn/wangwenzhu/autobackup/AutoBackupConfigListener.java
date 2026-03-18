package cn.wangwenzhu.autobackup;

import lombok.extern.java.Log;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import run.halo.app.plugin.PluginConfigUpdatedEvent;
import tools.jackson.databind.json.JsonMapper;

@Log
@Component
public class AutoBackupConfigListener {

    private final AutoBackupTask autoBackupTask;

    public AutoBackupConfigListener(AutoBackupTask autoBackupTask) {
        this.autoBackupTask = autoBackupTask;
    }


    @EventListener
    public void onConfigUpdated(PluginConfigUpdatedEvent event) {

        JsonMapper jsonMapper = JsonMapper.builder().build();
        AutoBackupConfig newConfig = jsonMapper.readValue(
            event.getNewSettingValues().get("base").toString(), AutoBackupConfig.class);

        AutoBackupConfig oldConfig = jsonMapper.readValue(
            event.getOldSettingValues().get("base").toString(), AutoBackupConfig.class);

        if (!newConfig.equals(oldConfig)) {
            log.info("AutoBackup plugin config updated");
            autoBackupTask.rescheduleTask(false);
        }
    }
}
