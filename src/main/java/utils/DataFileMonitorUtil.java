package utils;

import dto.IndexInfoDTO;
import model.command.CommandPos;
import service.NormalStore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import static service.NormalStore.RW_MODE;

public class DataFileMonitorUtil implements Runnable {
    private final String dataDir;
    private final long MAX_FILE_SIZE = 3 * 1024; // 100KB
    private final long CHECK_INTERVAL = 1000; // 10 seconds

    public DataFileMonitorUtil(String dataDir) {
        this.dataDir = dataDir;
    }

    @Override
    public void run() {
        while (true) {
            try {
                File dataFile = new File(dataDir + File.separator + NormalStore.NAME + NormalStore.TABLE);
                if (dataFile.length() >= MAX_FILE_SIZE) {
                    compressDataFile();
                    RandomAccessFile DATAFile = new RandomAccessFile(dataDir + File.separator + NormalStore.NAME + NormalStore.TABLE, RW_MODE);
                    DATAFile.setLength(0);

                }
                Thread.sleep(CHECK_INTERVAL);
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void compressDataFile() throws IOException {
        DataFileUtil dataFileUtil = new DataFileUtil(dataDir);
        dataFileUtil.checkAndCompressDataFile();
    }
}