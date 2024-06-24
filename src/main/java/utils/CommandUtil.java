/*
 *@Type ConvertUtil.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:09
 * @version
 */
package utils;

import com.alibaba.fastjson.JSONObject;
import model.command.Command;
import model.command.CommandTypeEnum;
import model.command.RmCommand;
import model.command.SetCommand;

public class CommandUtil {
    public static final String TYPE = "type";

    //Command接口类型
    //根据value中TYPE来动态创建不同的Command对象
    public static Command jsonToCommand(JSONObject value){
        if (value.getString(TYPE).equals(CommandTypeEnum.SET.name())) {
            return value.toJavaObject(SetCommand.class);
        } else if (value.getString(TYPE).equals(CommandTypeEnum.RM.name())) {
            return value.toJavaObject(RmCommand.class);
        }
        return null;
    }
}
