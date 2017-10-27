package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Searcher {

	private Connection conn;
	private String homeTeam;
	private String awayTeam;
	private String year;
	private String month;
	private String day;
	private String startTime;

	public Searcher(Connection connection) {
		resetFields();
		conn = connection;
	}

	public void search(boolean newSearch, String line) {
		if (newSearch) {
			resetFields();
		}
		try {
			String[] tokens = line.split("\\s+");
			parseFields(tokens);
			if (!isYearValid()) {
				System.out.println("Please search a year: -year <Year>");
				return;
			}
			System.out.println(this);
			performSearch();
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

	private boolean isYearValid() throws IOException {
		TableName[] tables = conn.getAdmin().listTableNames();
		for (int i = 0; i < tables.length; i++) {
			if (tables[i].toString().contains(year)) {
				return true;
			}
		}
		return false;
	}

	private void performSearch() throws IOException {
		String homeTeamID;
		if (!homeTeam.isEmpty()) {
			try {
				homeTeamID = getTeamID(homeTeam);
				System.out.println(homeTeamID);
			} catch (MismatchedArgsException e) {
				System.out.println(e.getMessage());
				return;
			}
		}
	}

	private String getTeamID(String teamName) throws IOException {
		Table table = conn.getTable(TableName.valueOf("teams2015"));
		ResultScanner scanner = table.getScanner(new Scan());
		Iterator<Result> results = scanner.iterator();
		Result result = null;
		String foundTeam = null;
		while (results.hasNext()) {
			result = results.next();
			foundTeam = Bytes.toString(result.getValue(Bytes.toBytes("teams_data"), Bytes.toBytes("name")));
			if (homeTeam.equals(foundTeam)) {
				return Bytes.toString(result.getRow());
			}
		}
		throw new MismatchedArgsException("Team: " + homeTeam + " not found");
	}

	@Override
	public String toString() {
		return "Searcher [homeTeam=" + homeTeam + ", awayTeam=" + awayTeam + ", year=" + year + ", month=" + month
				+ ", day=" + day + ", startTime=" + startTime + "]";
	}

}
