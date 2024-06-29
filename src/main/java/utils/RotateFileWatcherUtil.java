package utils;

import java.io.*;
import java.util.*;
import java.util.zip.*;

public class RotateFileWatcherUtil extends Thread {//多线程压缩文件
    private String directoryPath;
    private String zipFilePath;

    public RotateFileWatcherUtil(String directoryPath, String zipFilePath) {
        this.directoryPath = directoryPath;
        this.zipFilePath = zipFilePath;
    }

    @Override
    public void run() {
        try {
            while (true) {
                List<String> rotateFiles = listRotateFiles();
                List<String> alreadyCompressedFiles = getAlreadyCompressedFiles();
                List<String> allowCompressedFiles = new ArrayList<>();
                File file;
                if (!rotateFiles.isEmpty()) {
                    for (String rotateFile : rotateFiles) {
                        if (!alreadyCompressedFiles.contains(rotateFile)) {
                            file = new File(rotateFile);
                            if(file.length()>=1024){
                                allowCompressedFiles.add(rotateFile);
                            }
                        }
                    }
                    ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipFilePath));
                    zipRotateFiles(zipOut, allowCompressedFiles);
                } else {
                    System.out.println("No Rotate files found. Skipping compression.");
                }
                Thread.sleep(5000); // 每5秒检查一次Rotate文件
            }
        } catch (InterruptedException | IOException e) {
//            e.printStackTrace();
        }
    }

    private List<String> getAlreadyCompressedFiles() {
        List<String> compressedFiles = new ArrayList<>();

        try (ZipFile zipFile = new ZipFile(zipFilePath)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                compressedFiles.add(entry.getName());
            }
        } catch (IOException e) {
//            e.printStackTrace();
        }

        return compressedFiles;
    }

    private List<String> listRotateFiles() {
        List<String> rotateFiles = new ArrayList<>();
        File directory = new File(directoryPath);
        File[] files = directory.listFiles();
        String regex = "data[0-9]*.table";
        for (File file : files) {
            if (file.isFile() && file.getName().matches(regex)) {
                rotateFiles.add(file.getAbsolutePath());
            }
        }
        return rotateFiles;
    }

    private void zipRotateFiles(ZipOutputStream zipOut, List<String> rotateFiles) throws IOException {
        for (String file : rotateFiles) {
            File fileToZip = new File(file);
//                System.out.println(Thread.currentThread().getName() + " is compressing " + fileToZip.getName());
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
        zipOut.close();
    }

    public static void main(String[] args) {
        String directoryPath = "frame"; // 指定要监控的目录
        String zipFilePath = "frame/easy_db_data.zip"; // 指定压缩文件路径

        RotateFileWatcherUtil watcher = new RotateFileWatcherUtil(directoryPath, zipFilePath);
        watcher.start();
    }
}
