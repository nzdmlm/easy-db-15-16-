/*
 *@Type Client.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 13:15
 * @version
 */
package client;

public interface Client {

    //���Ӻ��޸�
    void set(String key, String value);

    //��ѯ
    String get(String key);

    //ɾ��
    void rm(String key);
}
