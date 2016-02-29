package macrobase.datasetanalysis;

import org.junit.*;

import com.mockrunner.jdbc.StatementResultSetHandler;
import com.mockrunner.mock.jdbc.JDBCMockObjectFactory;
import com.mockrunner.mock.jdbc.MockConnection;
import com.mockrunner.mock.jdbc.MockResultSet;
import com.mockrunner.mock.jdbc.MockResultSetMetaData;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datasetanalysis.DatasetAnalysis.ColumnInfo;
import macrobase.ingest.SQLLoader;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class DatasetAnalysisTest {
	
	public class TestSQLLoader extends SQLLoader {
	        public TestSQLLoader(MacroBaseConf conf, Connection connection)
	                throws ConfigurationException, SQLException {
	            super(conf, connection);
	        }

	        @Override
	        public String getDriverClass() {
	            return "fake";
	        }

	        @Override
	        public String getJDBCUrlPrefix() {
	            return "fake";
	        }
	    }
	
	
	public DatasetAnalysisTest() {
		
	}
	
	@Test
	public void simpleTest() throws Exception, SQLException{

        JDBCMockObjectFactory factory = new JDBCMockObjectFactory();
        factory.registerMockDriver();
        MockConnection connection = factory.getMockConnection();
        StatementResultSetHandler statementHandler =
                connection.getStatementResultSetHandler();
        MockResultSet result = statementHandler.createResultSet();

        MockResultSetMetaData metaData = new MockResultSetMetaData();

        List<Integer> fakeData = new ArrayList<>();

        final int NUM_ROWS = 100;
        final int NUM_ATTRS = 5;
        final int NUM_HIGH = 2;	
        final int NUM_LOW = 3;
        final int NUM_AUXILIARY = 1;

        final int DIMENSION = NUM_ATTRS + NUM_HIGH + NUM_LOW + NUM_AUXILIARY;

        Integer val = 1;
        Set<String> firstVals = new HashSet<>();
        for(int rno = 0; rno < NUM_ROWS; ++rno) {
            List<Object> rowString = new ArrayList<>();
            firstVals.add(val.toString());
            for(Integer i = 0; i < DIMENSION; ++i) {
                rowString.add(val.toString());
                val++;
            }
            result.addRow(rowString);
        }

        int column = 1;
        List<String> attributes = new ArrayList<>();
        for(int i = 0; i < NUM_ATTRS; ++i) {
            String attrName = String.format("attr%d", i);
            metaData.setColumnName(column, attrName);
            attributes.add(attrName);
            column++;
        }

        List<String> lowMetrics = new ArrayList<>();
        for(int i = 0; i < NUM_LOW; ++i) {
            String metricName = String.format("lowMetric%d", i);
            metaData.setColumnName(column, metricName);
            lowMetrics.add(metricName);
            column++;
        }

        List<String> highMetrics = new ArrayList<>();
        for(int i = 0; i < NUM_HIGH; ++i) {
            String metricName = String.format("highMetric%d", i);
            metaData.setColumnName(column, metricName);
            highMetrics.add(metricName);
            column++;
        }

        List<String> auxiliaryAttributes = new ArrayList<>();
        for(int i = 0; i < NUM_AUXILIARY; ++i) {
            String metricName = String.format("auxiliary%d", i);
            metaData.setColumnName(column, metricName);
            auxiliaryAttributes.add(metricName);
            column++;
        }

        metaData.setColumnCount(DIMENSION);

        result.setResultSetMetaData(metaData);

        statementHandler.prepareGlobalResultSet(result);
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, attributes);
        conf.set(MacroBaseConf.LOW_METRICS, lowMetrics);
        conf.set(MacroBaseConf.HIGH_METRICS, highMetrics);
        conf.set(MacroBaseConf.AUXILIARY_ATTRIBUTES, auxiliaryAttributes);

        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test");
        conf.set(MacroBaseConf.DATA_TRANSFORM_TYPE, MacroBaseConf.DataTransformType.IDENTITY);


        SQLLoader loader = new TestSQLLoader(conf, connection);
        
    	DatasetAnalysis da = new DatasetAnalysis(loader);
    	List<String> columnNames = da.getColumnNames();
    	assertEquals(columnNames.size(), 11	);
    	for(String columnName: columnNames){
    		//This is just for test purposes, not working properly, because not set the fake results
    		ColumnInfo ci = da.getColumnInfo(columnName);
    		
    	}
	}

}
