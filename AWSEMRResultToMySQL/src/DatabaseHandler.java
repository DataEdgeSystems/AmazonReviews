import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DatabaseHandler {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "amazonreviews.c8nbucbkmxlh.us-west-2.rds.amazonaws.com";

	static final String USER = "amazonreviews";
	static final String PASS = "amazonMSB143";

	Connection conn = null;
	Statement stmt = null;

	public DatabaseHandler() {
		try {
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			String endpoint = prop.getProperty("endpoint");
			String username = prop.getProperty("username");
			String password = prop.getProperty("password");
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void updateMatchOppositeTeam() {
		try {
			String query = "SELECT distinct match_id, team_id FROM main_batting_innings";
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			while (rs.next()) {
				String match_id = rs.getString("match_id");
				String team_id = rs.getString("team_id");
				String opposite_team_id = getOppositeTeamID(match_id, team_id);
				updateMatchOppositeTeam(match_id, team_id, opposite_team_id);
				updateMatchOppositeTeam(match_id, opposite_team_id, team_id);
			}
			st.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String getOppositeTeamID(String match_id, String team_id) {
		try {
			String query = "SELECT team_id FROM main_batting_innings where match_id='" + match_id + "' AND team_id!='"
					+ team_id + "'";
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			while (rs.next()) {
				String opp_team_id = rs.getString("team_id");
				return opp_team_id;
			}
			st.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private void updateMatchOppositeTeam(String match_id, String team_id, String opposite_team_id) {
		try {
			stmt = conn.createStatement();
			String query = "UPDATE main_batting_innings SET opposite_team_id=? WHERE match_id = ? AND team_id = ?";
			PreparedStatement preparedStmt = conn.prepareStatement(query);
			preparedStmt.setString(1, opposite_team_id);
			preparedStmt.setString(2, match_id);
			preparedStmt.setString(3, team_id);
			preparedStmt.execute();
			System.out.println("MATCH ID " + match_id);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void updateMatchData() {
		try {
			String query = "SELECT match_id, start_date FROM main_match";
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			while (rs.next()) {
				String match_id = rs.getString("match_id");
				Date match_date = rs.getDate("start_date");
				String year = match_date.toString().split("-")[0];
				updateMatchYear(match_id, year);
			}
			st.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void updateMatchYear(String matchID, String year) {
		try {
			stmt = conn.createStatement();
			String query = "UPDATE main_batting_innings SET year=? WHERE match_id = ?";
			PreparedStatement preparedStmt = conn.prepareStatement(query);
			preparedStmt.setString(1, year);
			preparedStmt.setString(2, matchID);
			preparedStmt.execute();
			System.out.println("MATCH ID " + matchID);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void updateMatchType() {
		try {
			String query = "SELECT match_id, match_type FROM main_match";
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			while (rs.next()) {
				String match_id = rs.getString("match_id");
				String match_type = rs.getString("match_type");
				updateMatchType(match_id, match_type);
			}
			st.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void updateMatchType(String match_id, String match_type) {
		try {
			stmt = conn.createStatement();
			String query = "UPDATE main_batting_innings SET matchType=? WHERE match_id = ?";
			PreparedStatement preparedStmt = conn.prepareStatement(query);
			preparedStmt.setString(1, match_type);
			preparedStmt.setString(2, match_id);
			preparedStmt.execute();
			System.out.println("MATCH ID " + match_id);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void updateOutType() {
		try {
			String query = "SELECT match_id, status_id, batsman_id FROM main_batting_innings";
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			while (rs.next()) {
				String match_id = rs.getString("match_id");
				String status_id = rs.getString("status_id");
				String batsman_id = rs.getString("batsman_id");
				updateOutType(match_id, batsman_id, getStatusID(status_id));
			}
			st.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String getStatusID(String status_id) {
		try {
			String query = "SELECT value FROM main_batting_status where batting_status_id=" + status_id;
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			while (rs.next()) {
				String value = rs.getString("value");
				return value;
			}
			st.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private void updateOutType(String match_id, String batsman_id, String out_type) {
		try {
			stmt = conn.createStatement();
			String query = "UPDATE main_batting_innings SET out_type=? WHERE match_id = ? and batsman_id = ?";
			PreparedStatement preparedStmt = conn.prepareStatement(query);
			preparedStmt.setString(1, out_type);
			preparedStmt.setString(2, match_id);
			preparedStmt.setString(3, batsman_id);
			preparedStmt.execute();
			System.out.println("MATCH ID " + match_id + " AND BATSMAN ID " + batsman_id + " AND OUT TYPE " + out_type);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
