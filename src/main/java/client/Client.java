/*
 *@Type Client.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 13:15
 * @version
 */
package client;

public interface Client {

    //增加和修改
    void set(String key, String value);

    //查询
    String get(String key);

    //删除
    void rm(String key);
}
