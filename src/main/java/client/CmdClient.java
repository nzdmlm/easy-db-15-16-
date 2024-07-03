/*
 *@Type CmdClient.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 13:58
 * @version
 */
package client;

import org.apache.commons.cli.*;
import java.util.Scanner;

public class CmdClient{
    public Client client;

    public CmdClient(Client client) {
        this.client = client;
    }

    public void CMD(String[] input) {
        Options options = new Options();

        Option setOption = Option.builder("s")
                .longOpt("set")
                .hasArgs()
                .desc("Set key and value (e.g. -s key value)")
                .build();
        options.addOption(setOption);

        Option getOption = Option.builder("g")
                .longOpt("get")
                .hasArg()
                .desc("Get value by key (e.g. -g key)")
                .build();
        options.addOption(getOption);

        Option removeOption = Option.builder("rm")
                .longOpt("remove")
                .hasArg()
                .desc("Remove key (e.g. -rm key)")
                .build();
        options.addOption(removeOption);

        Option exitOption = Option.builder("e")
                .longOpt("exit")
                .desc("Exit the program")
                .build();
        options.addOption(exitOption);

        Option helpOption = Option.builder("h")
                .longOpt("help")
                .desc("Print this help message")
                .build();
        options.addOption(helpOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, input);

            if (cmd.hasOption("h")) {
                formatter.printHelp("CmdClient", options);
            } else if (cmd.hasOption("s")) {
                String[] values = cmd.getOptionValues("s");
                if (values.length != 2) {
                    System.out.println("Error: 'set' option requires exactly 2 arguments.");
                    formatter.printHelp("CmdClient", options);
                    return;
                }
                String key = values[0];
                String value = values[1];
                client.set(key, value);
            } else if (cmd.hasOption("g")) {
                String key = cmd.getOptionValue("g");
                String result = client.get(key);
                System.out.println("Value for key '" + key + "': " + result);
            } else if (cmd.hasOption("rm")) {
                String key = cmd.getOptionValue("rm");
                client.rm(key);
                System.out.println("Key '" + key + "' removed.");
            }else if (cmd.hasOption("e")) {
                // �˳�����
                System.out.println("Exiting the program.");
                System.exit(0); // �˳� JVM
            } else {
                // ���û��ƥ���ѡ���ӡ������Ϣ�Ͱ�����Ϣ
                System.out.println("Error: No valid option provided.");
                formatter.printHelp("CmdClient", options);
            }
        } catch (ParseException e) {
            System.out.println("Error parsing command line options: " + e.getMessage());
            formatter.printHelp("CmdClient", options);
        }
    }

    public void main() {
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String line = sc.nextLine();
            if (line.isEmpty()) {
                continue; // ���Կ�������
            }
            String[] input = line.split("\\s+"); // ����һ�������ո�ָ�����
            CMD(input);
        }
    }
}