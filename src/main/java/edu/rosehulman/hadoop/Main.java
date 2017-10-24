package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Main {
	private TableName table1 = TableName.valueOf("Table1");
	private String family1 = "Family1";
	private String family2 = "Family2";

	public void run() {
		attemptConnection();
		Scanner in = new Scanner(System.in);
		while (true) {
			System.out.print("PlayBall > ");
			String line = in.nextLine();
			if (line.equals("exit") || line.equals("quit")) {
				System.out.println("Exiting");
				in.close();
				exit();
			}
		}
	}

	private void attemptConnection() {
		try {
			System.out.println("Attempting Connection...");
			connect();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("IOException when connecting");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("I CAUGHT AN ERROR. FIX THIS LATER");
		} finally {
			System.out.println("------------------FINALLY---------------");
		}
	}

	private void connect() throws IOException {
		System.out.println("Constructing Config");
		Configuration config = HBaseConfiguration.create();
		System.out.println("Adding Resource");
		config.addResource("hbase-site.xml");
		config.set("zookeeper.znode.parent", "");
		System.out.println("Creating Connection");
		Connection conn = ConnectionFactory.createConnection(config);
		System.out.println("Getting Admin");
		Admin admin = conn.getAdmin();
		System.out.println("Tables Should be Soon");
		TableName[] tables = admin.listTableNames();
		System.out.println("Table Names: " + tables);
	}

	public void exit() {
		// CleanUp
		System.exit(0);
	}

	public static void main(String[] args) {
		initLogger();
		Main main = new Main();
		main.run();
	}

	public static void initLogger() {
		PatternLayout layout = new PatternLayout("%-5p %d %m%n");
		ConsoleAppender appender = new ConsoleAppender();
		appender.setName("PlayBall");
		appender.activateOptions();
		appender.setLayout(layout);
		Logger.getRootLogger().addAppender(appender);
	}
}
