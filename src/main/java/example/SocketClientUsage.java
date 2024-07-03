/*
 *@Type SocketClientUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:07
 * @version
 */
package example;

import client.Client;
import client.CmdClient;
import client.SocketClient;

public class SocketClientUsage {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        Client client = new SocketClient(host, port);
        CmdClient cmdClient = new CmdClient(client);
        cmdClient.main();
//        client.get("zsy1");
//        client.set("zsy12","for test");
//        client.set("zsy12","for test1");
//        client.set("zsy13","for test");
//        client.set("zsy14","for test");
//        client.set("zsy15","for test1");
//        client.set("zsy16","for test1");
//        client.set("zsy177","for test");
//        client.set("zsy18","for test");
//        client.set("zsy19","for test12");
//        client.get("zsy12");
//        client.rm("zsy177");
//        client.set("zsy12","for test1");
//        client.set("zsy13","for test");
//        client.set("zsy14","for test");
//        client.set("zsy15","for test1");
//        client.set("zsy16","for test1");
//        client.set("zsy17","for test");
//        client.rm("zsy13");
//        client.rm("zsy14");
//        client.rm("zsy15");
//          client.get("zsy12");
//        client.get("zsy16");
//        client.get("zsy17");
//
//        client.get("zsy15");
    }
}