package org.apache.drill.exec.store.http.util;


import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import javax.sql.DataSource;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * This example demonstrates an alternative way to call a URL
 * using the Apache HttpClient HttpGet and HttpResponse
 * classes.
 * 
 * I copied the guts of this example from the Apache HttpClient
 * ClientConnectionRelease class, and decided to leave all the
 * try/catch/finally handling in the class. You don't have to catch
 * all the exceptions individually like this, I just left the code
 * as-is to demonstrate all the possible exceptions.
 * 
 * Apache HttpClient: http://hc.apache.org/httpclient-3.x/
 *
*/
public class DrillJdbcTest {
	
	private static final Logger logger = LoggerFactory.getLogger(DrillJdbcTest.class);
	  private static final Map<String, DataSource> dataSourceMap =new HashMap<String, DataSource>();
	private static Map<String, Integer> cacheSqlMap = new LinkedHashMap<String, Integer>();
	private static Map<String, Integer> testSqlMap = new LinkedHashMap<String, Integer>();

	private static List<Long> drillTimes = Lists.newArrayList();
	private static List<Long> mysqlTimes = Lists.newArrayList();
	public static List<String> testSqlResult = Lists.newArrayList();
	
	private static List<String> groupByColumns = Lists.newArrayList();
	private static String ALL_COLUMNS="id,vin,data_date,data_version,event_id,work_can,work_model,bmscelltempmax,bmscelltempmin,bmscoolanttemp,bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspackcurrent,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus,chargerhvcurrent,chargerhvrelaystatus,chargerhvvolt,chargerlvcurrent,chargerlvvolt,closemainrelaysreq,clutch1state,clutch2state,cruisetargetspeedset,dccurrenthv,dccurrentmaxhv,dcdcchargefault,dcmodereq,dcovercurrenthv,dcovertemp,dcovervolthv,dcovervoltlv,dcstate,dctemp,dcundervolthv,dcundervoltlv,dcvoltfail,dcvolthv,dcvoltlv,dcvoltsetpointfb,directfuelcutreq,drivertorquereq,drivertorquereqvalid,educoolanttemphs,ememergencyshutdown,egasopenreq,emergencypoweroffreq,engdragtorquevalid,engfuelpumpreq,engtorquefastreq,engtorqueslowreq,fuelcutrequest,gearactualvalid,gearshiftactive,gearshiftposn,gearshiftposnvalid,gps_status,hevfault,hevsystemmode,hevtorquemax,isgacshortcircready,isgdampingctrlreq,isgfailure,isghvready,isginvttemp,isgmodereq,isgoffsetangle,isgpumpstatus,isgrotortemp,isgspeed,isgspeedreq,isgstate,isgtemp,isgtorqueactual,isgtorquereq,keyswitchstatecrk,keyswitchstateign,latitude,longitude,original_latitude,original_longitude,mbregen,odo_primary,orientation,ptready,ptsyspwr,reserved1,reserved10,reserved11,reserved12,reserved13,reserved14,reserved15,reserved16,reserved17,reserved2,reserved3,reserved4,reserved5,reserved6,reserved7,reserved8,reserved9,shifterlockreq,tcfailure,tmacshortcircready,tmhvready,tminvttemp,tmmodereq,tmoffsetangle,tmrotortemp,tmspeed,tmspeedreq,tmstate,tmtemp,tmtorqueactual,tmtorquelimitmax,tmtorquereq,torqueincactive,torqueredactive,transinputspeed,transinputspeedvalid,transinputtorquevalid,status,create_by,create_date,last_update_by,last_update_date,row_version,is_valid,description,status_id,isgfailure_b0,isgfailure_b1,isgfailure_b2,isgfailure_b3,isgfailure_b4,isgfailure_b5,isgfailure_b6,isgfailure_b7,tcfailure_b0,tcfailure_b1,tcfailure_b2,tcfailure_b3,tcfailure_b4,tcfailure_b5,tcfailure_b6,tcfailure_b7,gearactual_h2,bmsbasicstat,isgfaultlampon,tmfaultlampon,educoolanttemp,emsfaultlevel,shifterfail,msg_mbregenfault,brakevacuumpumpinfo,enginecoolanttemp,vehiclespeedhsc,reserved18,reserved19,reserved20";
	private static  List<String> allColumns = Lists.newArrayList(ALL_COLUMNS.split(","));
	
	
	private final static String RANDOM_SQL_TEMPLATE  = "select COLUMNS, count(avgfuelconsumption) from ip24data_parquet group by COLUMNS order by COLUMNS limit 100";
	
	private static int mysqlDbGrpCnt = 8;
	  
	private static boolean testflag =true;
	
	public static void addTestSql(String sql){
		
		if(testflag){
			//synchronized(DrillJdbcTest.class){
				DrillJdbcTest.testSqlResult.add(sql);
				//}
		}

	}
	public static String getRandomGroupBySQL(int groupByColCnt) {
		Set<String> colSet = Sets.newLinkedHashSet();
		while (colSet.size() < groupByColCnt){
			int number = new Random().nextInt(allColumns.size());
			colSet.add(allColumns.get(number));
		}

		String colsStr = colSet.toString();
		colsStr = colsStr.substring(1);
		colsStr = colsStr.substring(0, colsStr.length()-1);
		return RANDOM_SQL_TEMPLATE.replaceAll("COLUMNS", colsStr);

	}


	private static void initTestData() {
 		  cacheSqlMap.put("select id from ip24data_parquet group by id order by id limit 100", 1);
		  cacheSqlMap.put("select id,vin from ip24data_parquet group by id,vin order by id,vin limit 100", 1);
		  cacheSqlMap.put("select id,vin,data_date from ip24data_parquet group by id,vin,data_date order by id,vin,data_date limit 100", 1);
		  cacheSqlMap.put("select id,vin,data_date,data_version from ip24data_parquet group by id,vin,data_date,data_version order by id,vin,data_date,data_version limit 100", 1);
		  cacheSqlMap.put("select id,vin,data_date,data_version,event_id from ip24data_parquet group by id,vin,data_date,data_version,event_id order by id,vin,data_date,data_version,event_id limit 100", 1);
		  cacheSqlMap.put("select id,vin,data_date,data_version,event_id,work_can from ip24data_parquet group by id,vin,data_date,data_version,event_id,work_can order by id,vin,data_date,data_version,event_id,work_can limit 100", 1);
		  cacheSqlMap.put("select id,vin,data_date,data_version,event_id,work_can,work_model from ip24data_parquet group by id,vin,data_date,data_version,event_id,work_can,work_model order by id,vin,data_date,data_version,event_id,work_can,work_model limit 100", 1);
		  cacheSqlMap.put("select id,vin,data_date,data_version,event_id,work_can,work_model,bmscelltempmax from ip24data_parquet group by id,vin,data_date,data_version,event_id,work_can,work_model,bmscelltempmax order by id,vin,data_date,data_version,event_id,work_can,work_model,bmscelltempmax limit 100", 1);
		  cacheSqlMap.put("select id,vin,data_date,data_version,event_id,work_can,work_model,bmscelltempmax,bmscelltempmin from ip24data_parquet group by id,vin,data_date,data_version,event_id,work_can,work_model,bmscelltempmax,bmscelltempmin order by id,vin,data_date,data_version,event_id,work_can,work_model,bmscelltempmax,bmscelltempmin limit 100", 1);
		  cacheSqlMap.put("select id,vin,data_date,data_version,event_id,work_can,work_model,bmscelltempmax,bmscelltempmin,bmscoolanttemp from ip24data_parquet group by id,vin,data_date,data_version,event_id,work_can,work_model,bmscelltempmax,bmscelltempmin,bmscoolanttemp order by id,vin,data_date,data_version,event_id,work_can,work_model,bmscelltempmax,bmscelltempmin,bmscoolanttemp limit 100", 1);
	  
		  testSqlMap.put("select bmshvilstat from ip24data_parquet group by  bmshvilstat order by	bmshvilstat limit 100", 1);
		  testSqlMap.put("select bmshvilstat,bmsmainralaysstat from ip24data_parquet group by  bmshvilstat,bmsmainralaysstat order by	bmshvilstat,bmsmainralaysstat limit 100", 1);
		  testSqlMap.put("select bmshvilstat,bmsmainralaysstat,bmsptisolation from ip24data_parquet group by  bmshvilstat,bmsmainralaysstat,bmsptisolation order by	bmshvilstat,bmsmainralaysstat,bmsptisolation limit 100", 1);
		  testSqlMap.put("select bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc from ip24data_parquet group by  bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc order by	bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc limit 100", 1);
		  testSqlMap.put("select bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt from ip24data_parquet group by  bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt order by	bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt limit 100", 1);
		  testSqlMap.put("select bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat from ip24data_parquet group by  bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat order by	bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat limit 100", 1);
		  testSqlMap.put("select bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus from ip24data_parquet group by  bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus order by	bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus limit 100", 1);
		  testSqlMap.put("select bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus,chargerhvcurrent from ip24data_parquet group by  bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus,chargerhvcurrent order by	bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus,chargerhvcurrent limit 100", 1);
		  testSqlMap.put("select bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus,chargerhvcurrent,chargerhvrelaystatus from ip24data_parquet group by  bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus,chargerhvcurrent,chargerhvrelaystatus order by	bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus,chargerhvcurrent,chargerhvrelaystatus limit 100", 1);
		  testSqlMap.put("select bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus,chargerhvcurrent,chargerhvrelaystatus,chargerhvvolt from ip24data_parquet group by  bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus,chargerhvcurrent,chargerhvrelaystatus,chargerhvvolt order by	bmshvilstat,bmsmainralaysstat,bmsptisolation,bmspacksoc,bmspackvolt,bmspumpstat,chargerfanstatus,chargerhvcurrent,chargerhvrelaystatus,chargerhvvolt limit 100", 1);

		  //HttpStoragePluginConfig httpStoragePluginConfig = new HttpStoragePluginConfig(null, null, null, null, null, null);
		  
		  Map<String, Map<String, String>> dbConfigs = Maps.newHashMap();
		logger.info("mysqlDbGrpCnt: "+mysqlDbGrpCnt);  
		for (int i = 1; i <= mysqlDbGrpCnt; i++) {
			String dbName = "student" + i;
			Map<String, String> dbConfig = Maps.newHashMap();
			String host = "";
			String password = "123456";
			if (i == 1 || i == 2) {

				host = "10.32.47.103";
				password = "123";
			} else if (i == 3 || i == 4) {

				host = "10.129.96.9";
			} else if (i == 5 || i == 6) {

				host = "10.129.96.10";
			} else if (i == 7 || i == 8) {

				host = "10.129.96.11";
			}

			dbConfig.put("driver", "com.mysql.jdbc.Driver");
			dbConfig.put("url", "jdbc:mysql://"+host+":3306/" + dbName);
			dbConfig.put("username", "root");
			dbConfig.put("password", password);

			dbConfigs.put(dbName, dbConfig);

			dbName = "student1" + i;
			dbConfig = Maps.newHashMap();
			dbConfig.put("driver", "com.mysql.jdbc.Driver");
			dbConfig.put("url", "jdbc:mysql://"+host+":3306/" + dbName);
			dbConfig.put("username", "root");
			dbConfig.put("password", password);

			dbConfigs.put(dbName, dbConfig);
		}
		  

		  Map<String, String> dbConfig =  Maps.newHashMap();
		  dbConfig.put("driver", "org.apache.drill.jdbc.Driver");
		  dbConfig.put("url", "jdbc:drill:zk=saic-mgmt01:2181,saic-mgmt02:2181,saic-mgmt03:2181/drill/drillbits2;schema=http");	
		  dbConfig.put("username", "");
		  dbConfig.put("password", "");	
		  
		  dbConfigs.put("drilldb", dbConfig);
		   
		  DBUtil.initDataSource(dbConfigs);
	} 
	
/*	
	public static synchronized void initDataSource(Map<String, Map<String, String>> dbConfig) {

		if(dataSourceMap.size()>0){
			return;
		}
		logger.info("start to initial data sources");
		for (String dbName : dbConfig.keySet()) {
			Map<String, String> db = dbConfig.get(dbName);

			
			DataSource dataSource = DBUtil.createDataSource(db.get("driver"), db.get("url"), db.get("username"), db.get("password"));
			dataSourceMap.put(dbName, dataSource);
		}
		
	}*/
/*	class Thread1 extends Thread{
		private String sql;
		DataSource dataSource;
	    public Thread1(DataSource dataSource, String sql) {
	    	this.dataSource =dataSource;
	       this.sql=sql;
	    }
		public void run() {

			
	       
		}
	}*/
	

	public static void executeSqlsTest(int maxGrgCols) {

		logger.info("start to do init test data");
		  initTestData();
		  
		  
		logger.info("start to do SQL test");
		// run cache sql
		//int i =0;
		for (String sql : cacheSqlMap.keySet()) {
			//i++;
			executeSqlBetweenDrillAndMySql(sql);

		}

		for (String sql : testSqlMap.keySet()) {

			executeSqlBetweenDrillAndMySql(sql);
		}
		
		// random sql query
		for (int j=1 ; j <= maxGrgCols;j++) {
			
			int randomColCnt = j%cacheSqlMap.size();
			randomColCnt = randomColCnt == 0?cacheSqlMap.size():randomColCnt;
			String sql = getRandomGroupBySQL(randomColCnt);
			executeSqlBetweenDrillAndMySql(sql);
		}

		printTestTimes();

	}


	private static void executeSqlBetweenDrillAndMySql(String sql) {
		//executeMySqlInSingleThread(sql);
		mysqlTimes.add(executeMySqlInMultiThreads(sql));

		DataSource drillDs = DBUtil.getDataSourceCache("drilldb");
		drillTimes.add(doExecuteSql(sql, drillDs));
	}

	private static void printTestTimes() {
		logger.info("execute sql on drill time:" + drillTimes);  
		   logger.info("execute sql on mysql time: " + mysqlTimes);
	}

	public class TaskCallable implements Callable<Long>{
		private String sql;
		DataSource dataSource;
	
	    public TaskCallable(DataSource dataSource, String sql) {
	    	this.dataSource =dataSource;
	       this.sql=sql;
	    }
	    @Override
	    public Long call() throws Exception {
	        
	    	return doExecuteSql(sql, dataSource);
	    }
	}
	
	private static Long executeMySqlInSingleThread(String sql) {
		
		List<Long> mysqlTempTimes = Lists.newArrayList();
		for (int i = 1; i <= mysqlDbGrpCnt; i++) {
			String dbName = "student" + i;
			DataSource mysqlDs = DBUtil.getDataSourceCache(dbName);
			mysqlTempTimes.add(doExecuteSql(sql, mysqlDs));
			
			dbName = "student1" + i;
			mysqlDs = DBUtil.getDataSourceCache(dbName);
			mysqlTempTimes.add(doExecuteSql(sql, mysqlDs));
		}
		
		return Collections.max(mysqlTempTimes);
	}
	private static Long executeMySqlInMultiThreads(final String sql) {
		
		List<Long> mysqlTempTimes = Lists.newArrayList();
		List<Future<Long>> futures = Lists.newArrayList();
		List<TaskCallable> taskCallables = Lists.newArrayList();
		ExecutorService exec = Executors.newCachedThreadPool();
		
		for (int i = 1; i <= mysqlDbGrpCnt; i++) {	        
		        
			String dbName = "student" + i;
			DataSource mysqlDs = DBUtil.getDataSourceCache(dbName);
			//mysqlTempTimes.add(doExecuteSql(sql, mysqlDs1));
			
		/*	Callable<Long> callable = new Callable<Long>() {
				public Long call() throws Exception {
					
					return doExecuteSql(sql, mysqlDs1);
				}
			};*/
			taskCallables.add((new DrillJdbcTest()).new  TaskCallable(mysqlDs, sql));
			
			//FutureTask<Long> future = new FutureTask<Long>(callable);
	        //new Thread(future).start();        
	        //futures.add(future);
			
			dbName = "student1" + i;
			 mysqlDs = DBUtil.getDataSourceCache(dbName);
			 taskCallables.add((new DrillJdbcTest()).new  TaskCallable(mysqlDs, sql));
			//mysqlTempTimes.add(doExecuteSql(sql, mysqlDs2));
			
	
			//future = new FutureTask<Long>(callable);
	        //new Thread(future).start();        
	        //futures.add(future);
        
		}
		
		

		
		for(TaskCallable t: taskCallables){
			
			futures.add((Future<Long>) exec.submit(t));
		}
		
		for(Future<Long> f : futures){
			
			try {
				mysqlTempTimes.add((Long) f.get());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
		}
		
		return Collections.max(mysqlTempTimes);
	}

	private static Long doExecuteSql(String sql, DataSource db) {
		
		logger.info("start query sql:" + sql);  
		ResultSet rs = null;
		Statement stmt = null;
		Connection conn = null;
		try {
			conn = db.getConnection();
			stmt = conn.createStatement();
			long start = System.currentTimeMillis();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {

				// logger.info("result:" +rs.getString(1));
			}
			long end = System.currentTimeMillis();
			
			return new Long(end -start);
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
		return -1l;
	}

	  
/*	  public static void createDataSource(String driver, String url, String userName, String password) {
		  
		  
		  ResultSet rs = null;  
		  Statement stmt = null;  
		  Connection conn = null;  
		  try {  
		   Class.forName("org.apache.drill.jdbc.Driver");  
		   //new oracle.jdbc.driver.OracleDriver();  
		   conn = DriverManager.getConnection("jdbc:drill:zk=saic-mgmt01:2181,saic-mgmt02:2181,saic-mgmt03:2181/drill/drillbits2;schema=http", "", "");  
		   stmt = conn.createStatement();  
		   
		   
		   //run cache sql
		   for(String sql : cacheSqlMap.keySet()){
			   long start = System.currentTimeMillis();
			   rs = stmt.executeQuery(sql);
			   
			   while(rs.next()) {  
				   
				   //logger.info("result:" +rs.getString(1));  
			   }
			   
			   drillTimes.add(System.currentTimeMillis() -start);
			   //logger.info("execute cache sql time:" +(System.currentTimeMillis() -start));  
		   }
		 
		   
		   for(String sql : testSqlMap.keySet()){
			   long start = System.currentTimeMillis();
			   rs = stmt.executeQuery(sql);   
			   
			   while(rs.next()) {  
				   
				   //logger.info("result:" +rs.getString(1));  
			   } 
			   testTimes.add(System.currentTimeMillis() -start);
			   //logger.info("execute run sql time:" +(System.currentTimeMillis() -start));  
		   }
		   
		   logger.info("execute cache sql time:" + drillTimes);  
		   logger.info("execute run sql time: " + testTimes);  
		   
		  // int columnNum = rs.get;
		   while(rs.next()) {  
			   
			   logger.info("result:" +rs.getString(1));  

		   }  
		  } catch (ClassNotFoundException e) {  
		   e.printStackTrace();  
		  } catch (SQLException e) {  
		   e.printStackTrace();  
		  } finally {  
		   try {  
		    if(rs != null) {  
		     rs.close();  
		     rs = null;  
		    }  
		    if(stmt != null) {  
		     stmt.close();  
		     stmt = null;  
		    }  
		    if(conn != null) {  
		     conn.close();  
		     conn = null;  
		    }  
		   } catch (SQLException e) {  
		    e.printStackTrace();  
		   }  
		  }  
		 }  */
	  


	  
	  
	public static void main(String[] args) {
		
		//createDataSource("", "jdbc:drill:zk=10.32.47.104:2181,10.32.47.105:2181,10.32.47.106:2181", "", "");
		getRandomGroupBySQL(5);
		executeSqlsTest(cacheSqlMap.size());

	}
	public static String doPost()  
    {  
        String uriAPI = "http://saic-dn01:8047";//Post方式没有参数在这里  
        String result = "";  
        HttpPost httpRequst = new HttpPost(uriAPI);//创建HttpPost对象  
          
        List <NameValuePair> params = new ArrayList<NameValuePair>();  
        params.add(new BasicNameValuePair("queryType", "SQL"));  
        params.add(new BasicNameValuePair("query", "select count(*) from http.`student`"));  
          
        try {  
            httpRequst.setEntity(new UrlEncodedFormEntity(params,HTTP.UTF_8));  
            HttpResponse httpResponse = new DefaultHttpClient().execute(httpRequst);  
            if(httpResponse.getStatusLine().getStatusCode() == 200)  
            {  
                HttpEntity httpEntity = httpResponse.getEntity();  
                result = EntityUtils.toString(httpEntity);//取出应答字符串  
                
                logger.info("result: "+result);
            }  
        } catch (UnsupportedEncodingException e) {  
            // TODO Auto-generated catch block  
            e.printStackTrace();  
            result = e.getMessage().toString();  
        }  
        catch (ClientProtocolException e) {  
            // TODO Auto-generated catch block  
            e.printStackTrace();  
            result = e.getMessage().toString();  
        }  
        catch (IOException e) {  
            // TODO Auto-generated catch block  
            e.printStackTrace();  
            result = e.getMessage().toString();  
        }  
        return result;  
    }
	
	public static void get2(String[] args) {
	    DefaultHttpClient httpclient = new DefaultHttpClient();
	    try {
	      // specify the host, protocol, and port
	      HttpHost target = new HttpHost("10.32.47.108", 8047, "http");
	      
	      // specify the get request
	      HttpGet getRequest = new HttpGet("/forecastrss?p=80020&u=f");

	      System.out.println("executing request to " + target);

	      HttpResponse httpResponse = httpclient.execute(target, getRequest);
	      
	      HttpEntity entity = httpResponse.getEntity();

	      System.out.println("----------------------------------------");
	      System.out.println(httpResponse.getStatusLine());
	      Header[] headers = httpResponse.getAllHeaders();
	      for (int i = 0; i < headers.length; i++) {
	        System.out.println(headers[i]);
	      }
	      System.out.println("----------------------------------------");

	      if (entity != null) {
	        System.out.println(EntityUtils.toString(entity));
	      }

	    } catch (Exception e) {
	      e.printStackTrace();
	    } finally {
	      // When HttpClient instance is no longer needed,
	      // shut down the connection manager to ensure
	      // immediate deallocation of all system resources
	      httpclient.getConnectionManager().shutdown();
	    }
	  }
	
  public final static void get() {
    
    HttpClient httpClient = new DefaultHttpClient();
    try {
      // this twitter call returns json results.
      // see this page for more info: https://dev.twitter.com/docs/using-search
      // http://search.twitter.com/search.json?q=%40apple

      // Example URL 1: this yahoo weather call returns results as an rss (xml) feed
      //HttpGet httpGetRequest = new HttpGet("http://weather.yahooapis.com/forecastrss?p=80020&u=f");
      
      // Example URL 2: this twitter api call returns results in a JSON format
      HttpGet httpGetRequest = new HttpGet("http://10.32.47.108:8047/");

      // Execute HTTP request
      HttpResponse httpResponse = httpClient.execute(httpGetRequest);

      System.out.println("----------------------------------------");
      System.out.println(httpResponse.getStatusLine());
      System.out.println("----------------------------------------");

      // Get hold of the response entity
      HttpEntity entity = httpResponse.getEntity();

      // If the response does not enclose an entity, there is no need
      // to bother about connection release
      byte[] buffer = new byte[1024];
      if (entity != null) {
        InputStream inputStream = entity.getContent();
        try {
          int bytesRead = 0;
          BufferedInputStream bis = new BufferedInputStream(inputStream);
          while ((bytesRead = bis.read(buffer)) != -1) {
            String chunk = new String(buffer, 0, bytesRead);
            System.out.println(chunk);
          }
        } catch (IOException ioException) {
          // In case of an IOException the connection will be released
          // back to the connection manager automatically
          ioException.printStackTrace();
        } catch (RuntimeException runtimeException) {
          // In case of an unexpected exception you may want to abort
          // the HTTP request in order to shut down the underlying
          // connection immediately.
          httpGetRequest.abort();
          runtimeException.printStackTrace();
        } finally {
          // Closing the input stream will trigger connection release
          try {
            inputStream.close();
          } catch (Exception ignore) {
          }
        }
      }
    } catch (ClientProtocolException e) {
      // thrown by httpClient.execute(httpGetRequest)
      e.printStackTrace();
    } catch (IOException e) {
      // thrown by entity.getContent();
      e.printStackTrace();
    } finally {
      // When HttpClient instance is no longer needed,
      // shut down the connection manager to ensure
      // immediate deallocation of all system resources
      httpClient.getConnectionManager().shutdown();
    }
  }
}
