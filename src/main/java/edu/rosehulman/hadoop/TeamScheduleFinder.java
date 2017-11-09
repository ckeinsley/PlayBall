package edu.rosehulman.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;

public class TeamScheduleFinder {

	private Connection conn;
	private String teamName;
	private String year;
	
	public TeamScheduleFinder(Connection connection) {
		resetFields();
		conn = connection;
	}

	private void resetFields() {
		teamName = "";
		year = "";
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
		performSearch();
	}

	private void parseFields(String[] tokens) {
		for (int i = 0; i < tokens.length; i++) {
			switch (tokens[i]) {
			case ("-team"):
				teamName = tokens[i + 1];
				if (i + 2 < tokens.length) {
					if (!tokens[i + 2].contains("-")) {
						teamName += " " + tokens[i + 2];
					}
				}
				break;
			case ("-year"):
				year = tokens[i + 1];
				break;
			default:
				break;
			}
		}
	}
	
	private void performSearch() throws IOException {
		Table table = conn.getTable(TableName.valueOf("teamschedule"));
		String teamId = getTeamId(teamName);
		Scan scan = new Scan(Bytes.toBytes(teamId + year), Bytes.toBytes(teamId + (Integer.parseInt(year) + 1)));
		ResultScanner scanner = table.getScanner(scan);
		for(Result res: scanner) {
			printTeamSchedule(res);
		}
		System.out.println("-----------------------");
	}
	
	private String getTeamId(String teamName) throws IOException {
		Table table = conn.getTable(TableName.valueOf("teams" + year));
		FilterList filter = new FilterList(Operator.MUST_PASS_ONE);
		SingleColumnValueFilter teamNameFilter = new SingleColumnValueFilter(Bytes.toBytes("teams_data"),
				Bytes.toBytes("name"), CompareOp.EQUAL, new SubstringComparator(teamName));
		filter.addFilter(teamNameFilter);
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner results = table.getScanner(scan);
		Result res = results.next();
		if (res == null) {
			System.out.println("Unknown Team Name: " + teamName);
			throw new IOException();
		}
		return Bytes.toString(res.getRow());
	}
	
	private String getTeamName(String teamCode) throws IOException {
		Table table = conn.getTable(TableName.valueOf("teams" + year));
		Get get = new Get(Bytes.toBytes(teamCode));
		Result res = table.get(get);
		return Bytes.toString(res.getValue(Bytes.toBytes("teams_data"), Bytes.toBytes("name")));
	}

	private void printTeamSchedule(Result res) throws IOException {
		String key = Bytes.toString(res.getRow());
		String date = key.substring(3, key.length() - 1);
		String opTeam = getTeamName(Bytes.toString(res.getValue(Bytes.toBytes("sched"), Bytes.toBytes("opposing"))));
		String score = Bytes.toString(res.getValue(Bytes.toBytes("sched"), Bytes.toBytes("score")));
		String opScore = Bytes.toString(res.getValue(Bytes.toBytes("sched"), Bytes.toBytes("op_score")));
		String home = Bytes.toString(res.getValue(Bytes.toBytes("sched"), Bytes.toBytes("home")));
		
		System.out.println("-----------------------");
		System.out.println("Date: " + date.substring(4,6) + "/" + date.substring(6) + "/" + date.substring(0,4));
		System.out.println((home.equals("Y") ? "Home " : "Away " ) + (Integer.parseInt(score) > Integer.parseInt(opScore) ? "Win" : "Loss") );
		System.out.println("\t" + teamName + "\t" + score);
		System.out.println("\t" + opTeam + "\t" + opScore);
	}
}
