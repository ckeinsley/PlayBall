package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Main {

	private Admin admin;

	public void run() {
		attemptConnection();
		printTables();
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
		}
	}

	private void connect() throws IOException {
		System.out.println("Constructing Config");
		Configuration config = HBaseConfiguration.create();
		config.addResource("hbase-site.xml");
		config.set("zookeeper.znode.parent", "/hbase-unsecure");
		
		System.out.println("Creating Connection");
		Connection conn = ConnectionFactory.createConnection(config);
		admin = conn.getAdmin();
	}

	private void printTables() {
		try {
			TableName[] tables = getTables();
			System.out.println("Table Names: " + Arrays.toString(tables));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private TableName[] getTables() throws IOException {
		System.out.println("Retrieving Tables");
		return admin.listTableNames();
	}

	public void exit() {
		try {
			admin.getConnection().close();
			System.exit(0);
		} catch (IOException e) {
			System.out.println("Error attempting to close connection");
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void main(String[] args) {
		initLogger();
		Main main = new Main();
		main.run();
	}

	public static void initLogger() {
		PatternLayout layout = new PatternLayout("%-5p %d %m%n");
		FileAppender appender = new FileAppender();
		appender.setName("PlayBall");
		appender.setFile("playBall.log");
		appender.activateOptions();
		appender.setLayout(layout);
		Logger.getRootLogger().addAppender(appender);
	}
}
