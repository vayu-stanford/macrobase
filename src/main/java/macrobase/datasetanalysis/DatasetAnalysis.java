package macrobase.datasetanalysis;

import java.sql.*;
import java.util.ArrayList;

import macrobase.ingest.SQLLoader;
import macrobase.ingest.result.Schema;
import macrobase.ingest.result.Schema.SchemaColumn;


public class DatasetAnalysis {

	 // JDBC driver name and database URL
    SQLLoader sqlLoader;
	
	public DatasetAnalysis(SQLLoader sqlLoader){
		this.sqlLoader = sqlLoader;
	}
	public ArrayList<String> getColumnNames(){
		
		ArrayList<String> columnNames = new ArrayList<String>();
		try {
			Schema schema = sqlLoader.getSchema();
			for(SchemaColumn sc: schema.getColumns()){
				columnNames.add(sc.getName());
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return columnNames;
	}

	
	public ColumnInfo getColumnInfo(String columnName){
		
	
		int numRows = 0;
		int numNonNullRows = 0;
		int numDistinctRows = 0;
		
		Statement stmt = null;
		
		String sql = String.format( "SELECT %s FROM (%s) baseQuery" ,
				"count(*), count(" + columnName+"), " + "count(distinct " + columnName + ")",
				sqlLoader.getBaseQuery().replaceAll(";", ""));
		
		
//		String sql = String.format("select "
//				+ "count(*), " // number of rows
//				+ "count(%s), "  // number of non-null rows
//				+ "count(distinct %s) " // number of distinct rows
//				+ " from %s",
//				columnName, columnName, tableName);
				
		try {
			stmt = sqlLoader.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			
			while(rs.next()){
				numRows = rs.getInt(1);
				numNonNullRows = rs.getInt(2);
				numDistinctRows = rs.getInt(3);
			}
			
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		Object min = null;
		Object max = null;
		Object average = null;
		
		sql = String.format( "SELECT %s FROM (%s) baseQuery" ,
				"min(" +  columnName+"), " + "max( " + columnName + "), " + "avg(" + columnName + ")",
				sqlLoader.getBaseQuery().replaceAll(";", ""));
	
		
//		sql = String.format("select "
//				+ "min(%s), " // min
//				+ "max(%s), "  // max
//				+ "avg(%s) " // avg
//				+ " from %s",
//				columnName, columnName, columnName, tableName);
//		
		try {
			stmt = sqlLoader.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			
			while(rs.next()){
				min = rs.getObject(1);
				max = rs.getObject(2);
				average = rs.getObject(3);
			}
			
			stmt.close();
		} catch (SQLException e) {
			//no min, max, average defined
		}
		
		ColumnInfo ci =  new ColumnInfo(columnName,numRows,numRows - numNonNullRows, numDistinctRows);
		
		if(min!=null){
			ci.setMin(min);
		}
		if(max != null)
			ci.setMax(max);
		if(average != null)
			ci.setAverage(average);
		
		
		return ci;
	}
	
	class ColumnInfo{
		String columnName;
		
		int numRows;
		int numNulls;
		int numDistinct;
		
		
		
		
		Object min;
		public Object getMin() {
			return min;
		}

		public void setMin(Object min) {
			this.min = min;
		}

		Object max;
		public Object getMax() {
			return max;
		}

		public void setMax(Object max) {
			this.max = max;
		}

		Object average;
		
		public Object getAverage() {
			return average;
		}

		public void setAverage(Object average) {
			this.average = average;
		}

		public ColumnInfo(String columnName, 
				int numRows,
				int numNulls, 
				int numDistinct){
			this.columnName = columnName;
			
			this.numRows = numRows;
			this.numNulls = numNulls;
			this.numDistinct = numDistinct;
			
			
		}
		
		public ColumnInfo(String columnName, 
				int numRows,
				int numNulls, 
				int numDistinct,
				Object min,
				Object max,
				Object average){
			
			this.columnName = columnName;
			
			this.numRows = numRows;
			this.numNulls = numNulls;
			this.numDistinct = numDistinct;
			
			this.min = min;
			this.max = max;
			this.average = average;
		}
		
		public String toString(){
			StringBuilder sb =new StringBuilder();
			sb.append("Column: " + columnName + "\n");
			sb.append("\t" + " numRows: " + numRows + "\n");
			sb.append("\t" + " numNulls: " + numNulls + "\n");
			sb.append("\t" + " numDistinctValues: " + numDistinct + "\n");
			
			if(min!=null)
				sb.append("\t" + " min: " + min.toString());
			if(max != null)
				sb.append("\t" + " max: " + max.toString());
			if(average != null)
				sb.append("\t" + " average: " + average.toString());
			
			return sb.toString();
		}
	}
	
}
