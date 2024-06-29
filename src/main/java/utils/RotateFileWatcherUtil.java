package utils;

import java.io.*;
import java.util.Enumeration;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class RotateFileWatcherUtil extends Thread {
    private String directoryPath;//扫描目录路径
    private String zipFilePath;//压缩文件路径
    private final ReentrantLock lock = new ReentrantLock();//可重入锁

    public RotateFileWatcherUtil(String directoryPath, String zipFilePath) {//路径初始化
        this.directoryPath = directoryPath;
        this.zipFilePath = zipFilePath;
    }

    @Override
    public void run() {
        try {
            while (true) {
                lock.lock(); // 获取锁
                try {
                    List<String> rotateFiles = listRotateFiles();//待压缩文件列表
                    List<String> alreadyCompressedFiles = getAlreadyCompressedFiles();//已经压缩文件列表
                    List<String> allowCompressedFiles = new ArrayList<>();//允许压缩文件列表
                    File file;
                    if (!rotateFiles.isEmpty()) {
                        for (String rotateFile : rotateFiles) {
                            if (!alreadyCompressedFiles.contains(rotateFile)) {
                                file = new File(rotateFile);
                                if (file.length() >= 1024) {
                                    allowCompressedFiles.add(rotateFile);
                                }
                            }
                        }
                        ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipFilePath));
                        zipRotateFiles(zipOut, allowCompressedFiles);
                    } else {
                        System.out.println("没有找到需要压缩的文件");
                    }
                } finally {
                    lock.unlock(); // 释放锁
                }
                Thread.sleep(10000); // 每10秒检查一次Rotate文件
            }
        } catch (InterruptedException | IOException e) {
            // Handle exceptions
        }
    }

    private List<String> getAlreadyCompressedFiles() {//获取已压缩文件方法
        lock.lock();
        List<String> compressedFiles = new ArrayList<>();
        try (ZipFile zipFile = new ZipFile(zipFilePath)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                compressedFiles.add(entry.getName());
            }
        } catch (IOException e) {
            // Handle exceptions
        } finally {
            lock.unlock();
        }
        return compressedFiles;
    }

    private List<String> listRotateFiles() {//获取待压缩文件方法
        lock.lock();
        List<String> rotateFiles = new ArrayList<>();
        try {
            File directory = new File(directoryPath);
            File[] files = directory.listFiles();
            String regex = "data[1-9]*.table";
            for (File file : files) {
                if (file.isFile() && file.getName().matches(regex)) {
                    rotateFiles.add(file.getAbsolutePath());
                }
            }
        } finally {
            lock.unlock();
        }
        return rotateFiles;
    }

    private void zipRotateFiles(ZipOutputStream zipOut, List<String> rotateFiles) throws IOException {
        lock.lock();
        try {
            for (String file : rotateFiles) {
                File fileToZip = new File(file);
                try (FileInputStream fis = new FileInputStream(fileToZip)) {
                    ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
                    zipOut.putNextEntry(zipEntry);

                    byte[] bytes = new byte[1024];
                    int length;
                    while ((length = fis.read(bytes)) >= 0) {
                        zipOut.write(bytes, 0, length);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        zipOut.close();
    }

    public static void main(String[] args) {
        String directoryPath = "frame"; // 指定要监控的目录
        String zipFilePath = "frame/easy_db_data.zip"; // 指定压缩文件路径

        RotateFileWatcherUtil watcher = new RotateFileWatcherUtil(directoryPath, zipFilePath);
        watcher.start();
    }
}