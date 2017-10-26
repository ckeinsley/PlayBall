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
	private boolean debug;
	private Searcher searcher;

	public Main(boolean debugMode) {
		debug = debugMode;
	}

	public void run() {
		attemptConnection();
		Scanner in = new Scanner(System.in);
		while (true) {
			System.out.print("PlayBall > ");
			String line = in.nextLine();
			if (line.startsWith("exit") || line.startsWith("quit")) {
				in.close();
				exit();
			} else if (line.startsWith("search")) {
				search(line);
			} else if (debug && line.startsWith("show tables")) {
				printTables();
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
		searcher = new Searcher(admin);
	}

	private void search(String line) {
		try {
			searcher.search(line.contains("-n"), line);
		} catch (MismatchedArgsException e) {
			System.out.println(e.getMessage());
		}
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
		System.out.println("Exiting");
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
		boolean debugMode = false;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-d")) {
				debugMode = true;
			}
		}
		initLogger();
		Main main = new Main(debugMode);
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
