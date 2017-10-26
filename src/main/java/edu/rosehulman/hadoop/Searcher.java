package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class Searcher {

	private Configuration config;
	private Connection conn;
	private String homeTeam;
	private String awayTeam;
	private String year;
	private String month;
	private String day;
	private String startTime;

	public Searcher(Configuration configuration, Connection connection) {
		resetFields();
		conn = connection;
		config = configuration;
	}

	public void search(boolean newSearch, String line) {
		if (newSearch) {
			resetFields();
		}
		try {
			String[] tokens = line.split("\\s+");
			parseFields(tokens);
			System.out.println(this);
			lookupTeam();
			practiceSearch();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new MismatchedArgsException("Wrong number of arguments for the specified fields");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Failed when looking up " + this);
		}
	}

	private void resetFields() {
		homeTeam = "";
		awayTeam = "";
		year = "";
		day = "";
		month = "";
		startTime = "";
	}

	private void parseFields(String[] tokens) {
		for (int i = 0; i < tokens.length; i++) {
			switch (tokens[i]) {
			case ("-homeTeam"):
				homeTeam = tokens[i + 1];
				break;
			case ("-awayTeam"):
				awayTeam = tokens[i + 1];
				break;
			case ("-year"):
				year = tokens[i + 1];
				break;
			case ("-day"):
				day = tokens[i + 1];
				break;
			case ("-month"):
				month = tokens[i + 1];
				break;
			case ("-startTime"):
				startTime = tokens[i + 1];
				break;
			default:
				break;
			}
		}
	}

	private void lookupTeam() {

	}

	private void practiceSearch() throws IOException {
		// Can pass in the TableName object from conn.getAdmin().listTables();
		Table table = conn.getTable(TableName.valueOf("plays2015"));
		Scan scanner = new Scan();
		// scanner.addFamily(Bytes.toBytes("play_data"));
		// scanner.addFamily(Bytes.toBytes("play_num"));
		// scanner.addFamily(Bytes.toBytes("game"));
		ResultScanner results = table.getScanner(scanner);
		Iterator<Result> iter = results.iterator();
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
	}

	@Override
	public String toString() {
		return "Searcher [homeTeam=" + homeTeam + ", awayTeam=" + awayTeam + ", year=" + year + ", month=" + month
				+ ", day=" + day + ", startTime=" + startTime + "]";
	}

}
