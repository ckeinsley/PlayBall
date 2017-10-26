package edu.rosehulman.hadoop;

import org.apache.hadoop.hbase.client.Admin;

public class Searcher {

	private Admin admin;
	private String homeTeam;
	private String awayTeam;
	private String year;
	private String month;
	private String day;
	private String startTime;

	public Searcher(Admin hbaseAdmin) {
		resetFields();
		admin = hbaseAdmin;
	}

	public void search(boolean newSearch, String line) {
		if (newSearch) {
			resetFields();
		}
		try {
			String[] tokens = line.split("\\s+");
			parseFields(tokens);
			System.out.println(this);
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new MismatchedArgsException("Wrong number of arguments for the specified fields");
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

	@Override
	public String toString() {
		return "Searcher [homeTeam=" + homeTeam + ", awayTeam=" + awayTeam + ", year=" + year + ", month=" + month
				+ ", day=" + day + ", startTime=" + startTime + "]";
	}

}
