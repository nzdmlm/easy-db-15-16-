/*
 *@Type NormalStore.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:07
 * @version
 */
package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import dto.IndexInfoDTO;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NormalStore implements Store {

    public static final String TABLE = ".table";
    public static final String TXT = ".txt";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    public static final String LOGNAME = "wal";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";
    private static Map<String, IndexInfoDTO> map = new HashMap<>();


    /**
     * 内存表，类似缓存
     */
    private TreeMap<String, Command> memTable;

    /**
     * hash索引，存的是数据长度和偏移量
     * */
    private HashMap<String, CommandPos> index;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁，支持多线程，并发安全写入
     */
    private final ReadWriteLock indexLock;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile writerReader;

    /**
     * 持久化阈值
     */
    private final int storeThreshold = 5;

    public static void setMap(Map<String, IndexInfoDTO> map1) {
        for(Map.Entry<String, IndexInfoDTO> entry : map1.entrySet()){
            map.put(entry.getKey(), entry.getValue());
        }
    }


    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<String, Command>();
        this.index = new HashMap<>();

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER, logFormat, "NormalStore", "dataDir isn't exist,creating...");
            file.mkdirs();
        }

        RedologUtil redologUtil = new RedologUtil(dataDir);
        try {
            redologUtil.flushWALToDataFile(memTable, index);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.reloadIndex();

        //固化数据建立映射在map中
        List<String> list = listFiles();
        for(String fileName : list) {
            reloadIndexConsist(fileName);
        }

        Thread dataFileMonitorThread = new Thread(new DataFileMonitorUtil(dataDir));
        dataFileMonitorThread.setDaemon(true);
        dataFileMonitorThread.start();

    }

    //数据固化文件在数据库启动建立映射
    public void reloadIndexConsist(String filePath) {
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
                    map.put(command.getKey(), indexInfoDTO);
                }
                start += cmdLen;
            }
            file.seek(file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }
        LoggerUtil.debug(LOGGER, logFormat, "reload index: "+index.toString());
    }


    public String genFilePath () {
            return this.dataDir + File.separator + NAME + TABLE;
        }

        public String genLogFilePath () {
            return this.dataDir + File.separator + LOGNAME + TXT;
        }

    public List<String> listFiles(){
        File file = new File(dataDir);
        File[] files = file.listFiles();
        List<String> list = new ArrayList<>();
        int count = 1;
        String regex = "data" + count + ".table";
        for(File file1 : files){
            if(file1.isFile() && file1.getName().matches(regex)){
                list.add(file1.getName());
                count++;
                regex = "data"+ count +".table";
            }
        }
        return list;
    }


    public void reloadIndex() {
        try {
            RandomAccessFile file = new RandomAccessFile(this.genFilePath(), RW_MODE);
            long len = file.length();
            long start = 0;
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
                    index.put(command.getKey(), cmdPos);
                }
                start += cmdLen;
            }
            file.seek(file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }
        LoggerUtil.debug(LOGGER, logFormat, "reload index: "+index.toString());
    }

    @Override
    public void set(String key, String value) {

        RedologUtil redologUtil = new RedologUtil(dataDir);
        try {
            redologUtil.flushWALToDataFile(memTable, index);
            this.reloadIndex();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘

            //添加
            if(memTable.size() < storeThreshold){
                memTable.put(key, command);
            }
            //截至

            // 写table（wal）日志文件
            RandomAccessFileUtil.writeInt(this.genLogFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genLogFilePath(), commandBytes);
            // 保存到memTable
            // 添加索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
            // TODO://判断是否需要将内存表中的值写回table
            //写回磁盘

            //添加
            if(memTable.size() >= storeThreshold){
                for(Map.Entry<String,Command> entry : memTable.entrySet()){
                    String key1 = entry.getKey();
                    Command command1 = entry.getValue();

                    if(command1 instanceof SetCommand){
                        byte[] commandBytes1 = JSONObject.toJSONBytes(command1);

                        RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes1.length);
                        int pos1 = RandomAccessFileUtil.write(this.genFilePath(), commandBytes1);

                        CommandPos cmdPos1 = new CommandPos(pos1, commandBytes1.length);
                        index.put(key1, cmdPos1);
                    }
                }
                memTable.clear();
                cleanLog(this.genLogFilePath());
            }
            //截至

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }



    @Override
    public String get(String key) {

        RedologUtil redologUtil = new RedologUtil(dataDir);
        try {
            redologUtil.flushWALToDataFile(memTable, index);
            this.reloadIndex();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        boolean flag = true;

        try {
            indexLock.readLock().lock();
            // 从索引中获取信息
            CommandPos cmdPos = index.get(key);
            if (cmdPos == null) {
                flag = false;
            }

            if (flag) {
                byte[] commandBytes = RandomAccessFileUtil.readByIndex(this.genFilePath(), cmdPos.getPos(), cmdPos.getLen());

                try {
                    JSONObject value = JSONObject.parseObject(new String(commandBytes));
                    Command cmd = CommandUtil.jsonToCommand(value);
                    if (cmd instanceof SetCommand) {
                        return ((SetCommand) cmd).getValue();
                    }
                    if (cmd instanceof RmCommand) {
                        return null;
                    }
                } catch (JSONException e) {
                    if (e.getMessage().contains("unterminated json string")) {
                        // 检查 JSON 字符串是否正确终止
                        String jsonString = new String(commandBytes);
                        int lastCharIndex = jsonString.lastIndexOf('}');
                        if (lastCharIndex != -1 && jsonString.charAt(lastCharIndex) != '}') {
                            // 添加缺少的关闭括号
                            jsonString += "}";
                            commandBytes = jsonString.getBytes();
                            JSONObject value = JSONObject.parseObject(new String(commandBytes));
                            Command cmd = CommandUtil.jsonToCommand(value);
                            if (cmd instanceof SetCommand) {
                                return ((SetCommand) cmd).getValue();
                            }
                            if (cmd instanceof RmCommand) {
                                return null;
                            }
                        }
                    } else {
                        JSONObject value = JSONObject.parseObject(new String(commandBytes));
                        Command cmd = CommandUtil.jsonToCommand(value);
                        if (cmd instanceof SetCommand) {
                            return ((SetCommand) cmd).getValue();
                        }
                        if (cmd instanceof RmCommand) {
                            return null;
                        }
                    }
                }
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }

        try {
            indexLock.readLock().lock();
            // 从索引中获取信息
            IndexInfoDTO indexInfoDTO = map.get(key);
            if (indexInfoDTO == null) {
                return null;
            }
            String path = indexInfoDTO.getPath();
            CommandPos commandpos = indexInfoDTO.getCommandpos();
            byte[] commandBytes = RandomAccessFileUtil.readByIndex((dataDir + File.separator + path), commandpos.getPos(), commandpos.getLen());

            try {
                JSONObject value = JSONObject.parseObject(new String(commandBytes));
                Command cmd = CommandUtil.jsonToCommand(value);
                if (cmd instanceof SetCommand) {
                    return ((SetCommand) cmd).getValue();
                }
                if (cmd instanceof RmCommand) {
                    return null;
                }
            } catch (JSONException e) {
                if (e.getMessage().contains("unterminated json string")) {
                    // 检查 JSON 字符串是否正确终止
                    String jsonString = new String(commandBytes);
                    int lastCharIndex = jsonString.lastIndexOf('}');
                    if (lastCharIndex != -1 && jsonString.charAt(lastCharIndex) != '}') {
                        // 添加缺少的关闭括号
                        jsonString += "}";
                        commandBytes = jsonString.getBytes();
                        JSONObject value = JSONObject.parseObject(new String(commandBytes));
                        Command cmd = CommandUtil.jsonToCommand(value);
                        if (cmd instanceof SetCommand) {
                            return ((SetCommand) cmd).getValue();
                        }
                        if (cmd instanceof RmCommand) {
                            return null;
                        }
                    }
                } else {
                    JSONObject value = JSONObject.parseObject(new String(commandBytes));
                    Command cmd = CommandUtil.jsonToCommand(value);
                    if (cmd instanceof SetCommand) {
                        return ((SetCommand) cmd).getValue();
                    }
                    if (cmd instanceof RmCommand) {
                        return null;
                    }
                }
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }

        return null;
    }

    @Override
    public void rm(String key) {

        RedologUtil redologUtil = new RedologUtil(dataDir);
        try {
            redologUtil.flushWALToDataFile(memTable, index);
            this.reloadIndex();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘

            //添加
            if(memTable.size() < storeThreshold){
                memTable.put(key, command);
            }
            //截至

            // 写table（wal）文件
            RandomAccessFileUtil.writeInt(this.genLogFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genLogFilePath(), commandBytes);
            // 保存到memTable

            // 添加索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);

            // TODO://判断是否需要将内存表中的值写回table
            //写回磁盘

            //添加
            if(memTable.size() >= storeThreshold){
                for(Map.Entry<String,Command> entry : memTable.entrySet()){
                    String key1 = entry.getKey();
                    Command command1 = entry.getValue();

                    if(command1 instanceof RmCommand){
                        byte[] commandBytes1 = JSONObject.toJSONBytes(command1);

                        RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes1.length);
                        int pos1 = RandomAccessFileUtil.write(this.genFilePath(), commandBytes1);

                        CommandPos cmdPos1 = new CommandPos(pos1, commandBytes1.length);
                        index.put(key1, cmdPos1);
                    }
                }
                memTable.clear();
                cleanLog(this.genLogFilePath());
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    public void cleanLog(String logFilePath) throws IOException {
        RandomAccessFile logFile = new RandomAccessFile(logFilePath, RW_MODE);
        logFile.setLength(0);
    }

    @Override
    public void close() throws IOException {
    }

}
