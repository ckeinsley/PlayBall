package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class WindStatsFinder {

	private Connection conn;
	private String year;
	private String stat;

	public WindStatsFinder(Connection connection) {
		resetFields();
		conn = connection;
	}

	private void resetFields() {
		year = "";
		stat = "";
	}

	public void search(boolean newSearch, String line) {
		if (newSearch) {
			resetFields();
		}
		try {
			search(line);
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new MismatchedArgsException("Wrong number of arguments for the specified fields");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Failed when looking up " + this);
		}
	}

	private void search(String line) throws IOException {
		String[] tokens = line.split("\\s+");
		parseFields(tokens);
		if (Main.debug) {
			System.out.println(this);
		}
		if (year.isEmpty()) {
			performAllYearsSearch();
		} else {
			performSearch();
		}
	}

	private void parseFields(String[] tokens) {
		for (int i = 0; i < tokens.length; i++) {
			switch (tokens[i]) {
			case ("-year"):
				year = tokens[i + 1];
				break;
			case ("-stat"):
				stat = tokens[i + 1];
				break;
			default:
				break;
			}
		}
	}
	
	private void performAllYearsSearch() throws IOException {
		Table table = conn.getTable(TableName.valueOf("windstats"));
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for (Result result: scanner) {
			if (stat.isEmpty()) {
				String year = Bytes.toString(result.getRow());
				String mode = Bytes.toString(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("mode")));
				String avg = Bytes.toString(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("avg")));
				String min = Bytes.toString(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("min")));
				String max = Bytes.toString(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("max")));
				System.out.println("- - - - - - - - - - - - - - - - - - - ");
				System.out.println("Wind stats for " + year + ":"
						+ "\n  Max wind speed " + max
						+ "\n  Min wind speed " + min
						+ "\n  Average wind speed " + avg
						+ "\n  Mode wind speed " + mode);
			} else {
				String year = Bytes.toString(result.getRow());
				String thestat = Bytes.toString(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes(stat)));
				System.out.println("- - - - - - - - - - - - - - - - - - - ");
				System.out.println(stat + " wind speed for " + year + " is " + thestat);
			}
		}
		System.out.println("- - - - - - - - - - - - - - - - - - - ");
	}

	private void performSearch() throws IOException {
		Table table = conn.getTable(TableName.valueOf("windstats"));
		Get get = new Get(Bytes.toBytes(year));
		Result res = table.get(get);
		System.out.println("- - - - - - - - - - - - - - - - - - - ");
		if (stat.isEmpty()) {
			String mode = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("mode")));
			String avg = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("avg")));
			String min = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("min")));
			String max = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes("max")));
			System.out.println("Wind stats for " + year + ":"
					+ "\n  Max wind speed " + max
					+ "\n  Min wind speed " + min
					+ "\n  Average wind speed " + avg
					+ "\n  Mode wind speed " + mode);
		} else {
			String thestat = Bytes.toString(res.getValue(Bytes.toBytes("stats"), Bytes.toBytes(stat)));
			System.out.println(stat + " wind speed for " + year + " is " + thestat);
		}
		System.out.println("- - - - - - - - - - - - - - - - - - - ");
	}
}
