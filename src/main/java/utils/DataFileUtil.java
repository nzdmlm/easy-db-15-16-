package utils;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import dto.IndexInfoDTO;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.NormalStore;

public class DataFileUtil {
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";
    private final String dataDir;
    private final String dataFileName;
    private final String RW_MODE = "rw";
    private final long MAX_FILE_SIZE = 3 * 1024; // 10KB
    private final ReadWriteLock indexLock;
    private int fileCount = 0;
    private TreeMap<String, IndexInfoDTO> index = new TreeMap<>();

    public DataFileUtil(String dataDir) {
        this.dataDir = dataDir;
        this.dataFileName = this.dataDir + File.separator + "data.table";
        this.indexLock = new ReentrantReadWriteLock();
        this.fileCount = findMaxFileNumber();
    }

    private int findMaxFileNumber() {
        int maxNumber = 0;
        File directory = new File(dataDir);

        if (!directory.exists() || !directory.isDirectory()) {
            System.out.println("Invalid directory: " + dataDir);
            return maxNumber;
        }

        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().matches("data\\d+\\.table")) {
                    int fileNumber = Integer.parseInt(file.getName().replaceAll("\\D", ""));
                    if (fileNumber > maxNumber) {
                        maxNumber = fileNumber;
                    }
                }
            }
        }

        return maxNumber;
    }

    public void checkAndCompressDataFile() throws IOException {
        indexLock.writeLock().lock();
        File dataFile = new File(dataFileName);
        try {
            if (dataFile.length() >= MAX_FILE_SIZE) {
                new Thread(() -> {
                    try {
                        compressDataFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }).start();
            }
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    private void compressDataFile() throws IOException {
        File dataFile = new File(dataDir + File.separator + "data.table");
        String newFileName = generateNewFileName();
        File compressedFile = new File(dataDir, newFileName);

        RandomAccessFile dataFileReader = null;
        RandomAccessFile compressedFileWriter = null;

        try {
            dataFileReader = new RandomAccessFile(dataFile, RW_MODE);
            compressedFileWriter = new RandomAccessFile(compressedFile, RW_MODE);

            TreeMap<String, Command> latestCommands = new TreeMap<>();

            long start = 0;
            long dataFileLength = dataFile.length();
            dataFileReader.seek(start);

            while (start < dataFileLength) {
                int cmdLen = dataFileReader.readInt();
                byte[] bytes = new byte[cmdLen];
                dataFileReader.read(bytes);
                JSONObject value = JSONObject.parseObject(new String(bytes, StandardCharsets.UTF_8), Feature.OrderedField);
                Command command = CommandUtil.jsonToCommand(value);

                if (command != null) {
                    latestCommands.put(command.getKey(), command);
                }
                start += 4 + cmdLen;
            }

            for (Map.Entry<String, Command> entry : latestCommands.entrySet()) {
                Command command = entry.getValue();
                if (command instanceof SetCommand) {
                    byte[] commandBytes = JSONObject.toJSONBytes(command);
                    compressedFileWriter.writeInt(commandBytes.length);
                    compressedFileWriter.write(commandBytes);

//                    CommandPos cmdPos = new CommandPos((int) compressedFileWriter.getFilePointer() - commandBytes.length, commandBytes.length);

                } else if (command instanceof RmCommand) {
                    // 如果是 remove 命令，请从索引中删除所有相关条目
//                    for (Iterator<Map.Entry<String, Command>> iterator = index.entrySet().iterator(); iterator.hasNext(); ) {
//                        Map.Entry<String, CommandPos> indexEntry = iterator.next();
//                        if (indexEntry.getKey().equals(command.getKey())) {
//                            iterator.remove();
//                        }
//                    }

                }
            }


        } finally {
            if (dataFileReader != null) {
                dataFileReader.close();
            }
            if (compressedFileWriter != null) {
                compressedFileWriter.close();
            }
        }


        reloadIndex(newFileName);
        LoggerUtil.debug(LOGGER, logFormat, "reload index: "+index.toString());
        NormalStore.setMap(index);
    }

    private String generateNewFileName() {
        return "data" + ++fileCount + ".table";
    }

    public void reloadIndex(String filePath) {
        try {
            RandomAccessFile file = new RandomAccessFile((dataDir + File.separator + filePath), RW_MODE);
            long len = file.length();
            long start = 0;
            IndexInfoDTO indexInfoDTO = null;
            file.seek(start);
            while (start < len) {
                int cmdLen = file.readInt();//从文件读取4个字节，返回4字节32位的数据
                byte[] bytes = new byte[cmdLen];
                file.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    indexInfoDTO = new IndexInfoDTO(filePath, cmdPos);
                    index.put(command.getKey(), indexInfoDTO);
                }
                start += cmdLen;
            }
            file.seek(file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }
        LoggerUtil.debug(LOGGER, logFormat, "reload index: "+index.toString());
    }
}

