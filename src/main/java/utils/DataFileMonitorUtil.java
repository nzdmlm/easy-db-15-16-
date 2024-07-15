package utils;

import lombok.SneakyThrows;
import service.NormalStore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static service.NormalStore.RW_MODE;

public class DataFileMonitorUtil implements Runnable {
    private final String dataDir;
    private final long MAX_FILE_SIZE = 3 * 1024; // 100KB
    private final long CHECK_INTERVAL = 1000; // 10 seconds
    private  final ReentrantReadWriteLock lock;
    private final ExecutorService executorService;

    public DataFileMonitorUtil(String dataDir) {
        this.dataDir = dataDir;
        this.lock = new ReentrantReadWriteLock();
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void run() {
//        while (true) {
//            try {
//
//                File dataFile = new File(dataDir + File.separator + NormalStore.NAME + NormalStore.TABLE);
////                File dataFile = new File("data/data.table");
//                if (dataFile.length() >= MAX_FILE_SIZE) {
//
//                    compressDataFile();
//                    RandomAccessFile DATAFile = new RandomAccessFile(dataDir + File.separator + NormalStore.NAME + NormalStore.TABLE, RW_MODE);
//                    DATAFile.setLength(0);
//
//                }
//                Thread.sleep(CHECK_INTERVAL);
//            } catch (InterruptedException | IOException e) {
//                e.printStackTrace();
//            }
//        }

        while (true) {
            try {
                File dataFile = new File(dataDir, NormalStore.NAME + NormalStore.TABLE);
                if (dataFile.exists() && dataFile.length() >= MAX_FILE_SIZE) {
                    executorService.submit(() -> {
                        lock.writeLock().lock();
                        try {
                            compressDataFile();
                            RandomAccessFile DATAFile = new RandomAccessFile(dataDir + File.separator + NormalStore.NAME + NormalStore.TABLE, RW_MODE);
                            DATAFile.setLength(0);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            lock.writeLock().unlock();
                        }
                    });
                }
                Thread.sleep(CHECK_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }

    }

    private void compressDataFile() throws IOException {
        DataFileUtil dataFileUtil = new DataFileUtil(dataDir);
        dataFileUtil.checkAndCompressDataFile();
    }
}