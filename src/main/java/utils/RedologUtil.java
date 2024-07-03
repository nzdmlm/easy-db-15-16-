package utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import model.command.Command;
import model.command.CommandPos;
import service.NormalStore;

public class RedologUtil {
    private final String dataDir;
    private final String walFileName;
    private final String dataFileName;
    private final String RW_MODE = "rw";

    public RedologUtil(String dataDir) {
        this.dataDir = dataDir;
        this.walFileName = this.dataDir + File.separator + NormalStore.LOGNAME + NormalStore.TXT;
        this.dataFileName = this.dataDir + File.separator + NormalStore.NAME + NormalStore.TABLE;
    }

    public void flushWALToDataFile(TreeMap<String, Command> memTable, HashMap<String, CommandPos> index) throws IOException {
        RandomAccessFile walFile = null;
        RandomAccessFile dataFile = null;

        try {
            walFile = new RandomAccessFile(walFileName, RW_MODE);
            dataFile = new RandomAccessFile(dataFileName, RW_MODE);

            long dataFileLength = dataFile.length();
            dataFile.seek(dataFileLength);

            walFile.seek(0);
            long walFileLength = walFile.length();
            long start = 0;

            while (start < walFileLength) {
                int cmdLen = walFile.readInt();
                byte[] bytes = new byte[cmdLen];
                walFile.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);

                if (command != null) {
                    byte[] commandBytes = JSONObject.toJSONBytes(command);
                    dataFile.writeInt(commandBytes.length);
                    dataFile.write(commandBytes);

                    CommandPos cmdPos = new CommandPos((int) dataFileLength, commandBytes.length);
                    //index.put(command.getKey(), cmdPos);
                    dataFileLength += 4 + commandBytes.length;
                }
                start += 4 + cmdLen;
            }

            walFile.setLength(0);
        } finally {
            if (walFile != null) {
                walFile.close();
            }
            if (dataFile != null) {
                dataFile.close();
            }
        }
    }
}