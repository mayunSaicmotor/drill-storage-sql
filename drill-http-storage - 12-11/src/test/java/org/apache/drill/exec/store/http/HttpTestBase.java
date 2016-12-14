/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.http;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.http.util.DrillJdbcTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.beust.jcommander.internal.Lists;

public class HttpTestBase extends PlanTestBase implements HttpTestConstants {
	
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpTestBase.class);
  protected static HttpStoragePlugin storagePlugin;
  protected static HttpStoragePluginConfig storagePluginConfig;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initHttpStoragePlugin();
  }

  public static void initHttpStoragePlugin() throws Exception {
    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    storagePlugin = (HttpStoragePlugin) pluginRegistry.getPlugin(HttpStoragePluginConfig.NAME);
    storagePluginConfig = storagePlugin.getConfig();
    storagePluginConfig.setEnabled(true);
    pluginRegistry.createOrUpdate(HttpStoragePluginConfig.NAME, storagePluginConfig, true);
    if (System.getProperty("drill.mongo.tests.bson.reader", "true").equalsIgnoreCase("false")) {
      testNoResult(String.format("alter session set `%s` = false", ExecConstants.MONGO_BSON_RECORD_READER));
    } else {
      testNoResult(String.format("alter session set `%s` = true", ExecConstants.MONGO_BSON_RECORD_READER));
    }
  }

  public List<QueryDataBatch> runHttpSQLWithResults(String sql)
      throws Exception {
    return testSqlWithResults(sql);
  }

/*  public void runHttpSQLVerifyCount(String sql, int expectedRowCount)
      throws Exception {
    List<QueryDataBatch> results = runHttpSQLWithResults(sql);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }*/

  public List<List<String>> runHttpSQLVerifyCount(String sql, int expectedRowCount)
	      throws Exception {
	    List<QueryDataBatch> results = runHttpSQLWithResults(sql);
	    List<List<String>> resultLists = getResult(results);
	    return resultLists;
	  }
  
/*  public void printResultAndVerifyRowCount(List<QueryDataBatch> results,
      int expectedRowCount) throws SchemaChangeException {
    int rowCount = printResult(results);
	  
	  
    if (expectedRowCount != -1) {
     // Assert.assertEquals(expectedRowCount, rowCount);
    }
  }*/

  public  List<List<String>> executeSql(String sql, DataSource db) {
	  
	  sql = sql.replace("http.`", "");
	  sql = sql.replaceAll("`", "");
	  sql = sql.replaceAll("TO_NUMBER\\(", "");
	  sql = sql.replaceAll("\\,\\ \\'#*\\'\\)", "");

		
	  List<List<String>> resultLists =Lists.newArrayList();
	  List<String> allCols = Lists.newArrayList();
		logger.info("start query sql:" + sql);  
		ResultSet rs = null;
		Statement stmt = null;
		Connection conn = null;
		try {
			conn = db.getConnection();
			stmt = conn.createStatement();
			long start = System.currentTimeMillis();
			rs = stmt.executeQuery(sql);
			ResultSetMetaData m = rs.getMetaData(); // 获得列集

			int columns = m.getColumnCount();
			// 显示列,表格的表头
			for (int i = 1; i <= columns; i++) {
				allCols.add(m.getColumnName(i));
			}
			logger.info(allCols.toString());

	
			
			while (rs.next()) {
				List<String> 	 resultList = Lists.newArrayList();
				for (int i = 1; i <=allCols.size(); i++) {
					//logger.info(i);
					String currentCol = allCols.get(i-1);
					Object o = rs.getObject(i);
					String value = null;
					 if (o instanceof BigDecimal) {
				    	  
				    	BigDecimal bigDecimal = (BigDecimal) o;
				    	
				    	value = bigDecimal.setScale(4, BigDecimal.ROUND_HALF_EVEN).stripTrailingZeros().toPlainString();
					 } else {
						 value = o.toString();
					 }
					//String value = rs.getString(i);

					resultList.add(value);

				}
				
				resultLists.add(resultList);
				
			}
			
			//testTimes.add(System.currentTimeMillis() - start);

			//logger.info("execute cache sql time:" + cacheTimes);
			//logger.info("execute run sql time: " + testTimes);

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
					rs = null;
				}
				if (stmt != null) {
					stmt.close();
					stmt = null;
				}
				
				if (conn != null) {
					conn.close();
					conn = null;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return resultLists;
	}
  
  public void testHelper(String query, String expectedExprInPlan,
      int expectedRecordCount) throws Exception {
    testPhysicalPlan(query, expectedExprInPlan);
    int actualRecordCount = testSql(query);
    assertEquals(
        String.format(
            "Received unexpected number of rows in output: expected=%d, received=%s",
            expectedRecordCount, actualRecordCount), expectedRecordCount,
        actualRecordCount);
  }

  @AfterClass
  public static void tearDownHttpTestBase() throws Exception {
	  
	  logger.info("DrillJdbcTest result "+DrillJdbcTest.testSqlResult.toString());
    storagePlugin = null;
  }

}