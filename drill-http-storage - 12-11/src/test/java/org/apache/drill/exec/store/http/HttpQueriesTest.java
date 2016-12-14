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

import java.util.List;

import org.apache.drill.exec.store.http.util.DBUtil;
import org.apache.drill.exec.store.http.util.DrillJdbcTest;
import org.junit.Test;

import junit.framework.Assert;

//G:groupby L:limit O;order by S: select F: filter
public class HttpQueriesTest extends HttpTestBase {

	private void assertResult(String testSql, String expected, Boolean orderFlg) throws Exception {
		List<List<String>> drillResult = runHttpSQLVerifyCount(testSql, 1);
		Assert.assertEquals(expected, DrillJdbcTest.testSqlResult.toArray()[DrillJdbcTest.testSqlResult.size() - 1]);
		
		List<List<String>> mysqlResult = this.executeSql(testSql, DBUtil.getDataSourceCache("student"));
		if(orderFlg == null){
			
			return;
		}
		if(orderFlg){
			Assert.assertEquals(drillResult, mysqlResult);
			
		}{
			for(List l : drillResult){
				
				mysqlResult.remove(l);
					
			}
			
			Assert.assertEquals(0, mysqlResult.size());
			
			
		}
		
	}

	@Test
	public void test() throws Exception {
		// String testSql = "select * from http.`/testquery`";
		// String testSql = "select * from http.`/testquery` where firstName =
		// 'Jason' and lastName = 'Hunter'";
		// String testSql = "select * from http.`/testquery` group firstName =
		// 'Jason'";
		// String testSql = "select firstName, from http.`test` where firstName
		// = 'Jason' group by firstName";
		// String testSql ="select firstName , sum(TO_NUMBER(code, '######')) ,
		// avg(TO_NUMBER(code, '######')) , avg(TO_NUMBER(score, '######')) from
		// http.`test` where firstName = 'Jason' group by firstName";

		// String testSql ="select firstName, avg(TO_NUMBER(code, '######')) ,
		// avg(TO_NUMBER(score, '######')) from http.`test` where firstName =
		// 'Jason' or firstName = 'Jason1' group by firstName";

		// OK
		/*
		 * File destDir = new File("src"); JCodeModel cm = new JCodeModel();//
		 * 实例化 CodeModel JDefinedClass dc = cm._class("dw.example"); // 创建
		 * example 类 JType type = cm.parseType("String"); JType type1 =
		 * cm.parseType("myType");
		 * 
		 * JMethod exampleMethod = dc.method(JMod.PUBLIC, cm.VOID,
		 * "exampleMethod"); JBlock exampleMethodBlk = exampleMethod.body();
		 * JVar var = exampleMethodBlk.decl(type, "fieldVar"); JVar var1 =
		 * exampleMethodBlk.decl(type, "fieldVar1",JExpr.lit(5));
		 */

		// String testSql = "select name as name1, code as code1, sum(score) as
		// sum_score, avg(score) from http.`student` group by code, name order
		// by code1, sum_score desc, name1 desc limit 5";
		// runHttpSQLVerifyCount(testSql,1);

		String testSql = "select name,  code,  sum(score), avg(score) from http.`student` group by name, code  order by name desc, code desc limit 5";
		String expected = "select name,code,SUM(score) as $f2,COUNT(score) as $f3,SUM(score) as $f4 from student group by name, code order by name DESC, code DESC limit 5";
		assertResult(testSql, expected, true);

	}

	@Test
	public void test_1sum_2G_L_1F() throws Exception {

		String testSql = "select  code , sum(code)  from http.`student` where name = 'Tom3' group by name, code limit 2";

		String expected = "select name,code,SUM(code) as EXPR$1 from student where name='Tom3' group by name, code order by name ASC, code ASC limit 2";
		assertResult(testSql, expected, true);
	}

	@Test
	public void test_2G_L_1F() throws Exception {

		String testSql = "select name,  code  from http.`student` where name = 'Tom3' group by name, code limit 2";

		String expected = "select name,code from student where name='Tom3' group by name, code order by name ASC, code ASC limit 2";
		assertResult(testSql, expected, true);

	}

	@Test
	public void test_2G_2O_L() throws Exception {

		// OK
		String mysql = "select name, avg(score) as avg_score from student group by name order by name, avg_score limit 2";
		String testSql = "select name,  code,  avg(score) from http.`student` group by name, code  order by name, code limit 2";

		

		String expected = "select name,code,SUM(score) as $f2,COUNT(score) as $f3 from student group by name, code order by name ASC, code ASC limit 2";
		assertResult(testSql, expected, true);

	}

	@Test
	public void test_2G_2Odesc_L() throws Exception {

		// OK
		String mysql = "select name, avg(score) as avg_score from student group by name order by name, avg_score limit 5";
		String testSql = "select name,  code,  avg(score) from http.`student` group by name, code  order by name desc, code desc limit 5";

		
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);
		String expected = "select name,code,SUM(score) as $f2,COUNT(score) as $f3 from student group by name, code order by name DESC, code DESC limit 5";
		assertResult(testSql, expected, true);
	}

	@Test
	public void test_2G_2OdescAndasc_L() throws Exception {

		// OK
		String mysql = "select name, avg(score) as avg_score from student group by name order by name, avg_score limit 2";
		String testSql = "select name,  code,  avg(score) from http.`student` group by name, code  order by name desc, code asc limit 4";

		
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);
		String expected = "select name,code,SUM(score) as $f2,COUNT(score) as $f3 from student group by name, code order by name DESC, code ASC limit 4";
		assertResult(testSql, expected, true);
	}

	@Test
	public void test_1G_1O1avgO_L() throws Exception {

		// OK
		String mysql = "select name, avg(score) as avg_score from student group by name order by name, avg_score limit 2";
		String testSql = "select name, avg(score) as avg_score from http.`student` group by name order by name, avg_score limit 2";

		
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);
		String expected = "select name,SUM(score) as $f1,COUNT(score) as $f2 from student group by name order by name ASC limit 2";
		assertResult(testSql, expected, true);
	}

	@Test
	// TODO
	public void test_1G_1avgO_L() throws Exception {

		// OK
		String mysql = "select name, avg(score) as avg_score from student group by name order by avg_score limit 3";
		String testSql = "select name, avg(score) as avg_score from http.`student` group by name order by avg_score limit 3";

		
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);
		String expected = "select name,SUM(score) as $f1,COUNT(score) as $f2 from student group by name order by name ASC";
		assertResult(testSql, expected, true);
	}

	@Test
	// TODO
	public void test_1G_O1sum_L() throws Exception {

		// OK

		// TODO
		String mysql = "select name, sum(score) as sum_score from student group by name order by sum_score limit 3";
		// String testSql = "select name, sum(score) as sum_score from
		// http.`student` group by name order by sum_score limit 3";
		String testSql = "select name, count(name) as count_score  , code from http.`student` group by name, code order by count_score desc limit 3";


		String expected = "select name,code,COUNT(name) as count_score from student group by name, code order by name ASC, code DESC";
		assertResult(testSql, expected, null);
	}

	@Test
	public void test_As_In_Select() throws Exception {

		// OK
		String mysql = "select name,  code, count(name) as count_name, count(name) as count_name1, sum(code) as sum_code, count(code) as count_code, avg(score) as avg_score from student where name = 'Tom0' group by name, code";
		String testSql = "select name,  code, count(name) as count_name, count(name) as count_name1, sum(code) as sum_code, count(code) as count_code, avg(score) as avg_score from http.`student` where name = 'Tom0' group by name, code";

		
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);
		String expected = "select name,code,COUNT(name) as count_name1,SUM(code) as $f3,COUNT(code) as $f4,COUNT(code) as count_code,SUM(score) as $f6,COUNT(score) as $f7 from student where name='Tom0' group by name, code order by name ASC, code ASC";
		assertResult(testSql, expected, false);
	}

	@Test
	public void test_As_In_O() throws Exception {

		// OK

		String mysql = "select name as name_as,  code as code_as,  avg(score) as score_as from student group by name_as, code_as  order by name_as, code_as limit 100";
		// GROUP alias col name is not supported
		// String testSql = "select name as name_as, code as code_as, avg(score)
		// as score_as from http.`student` group by name_as, code_as order by
		// name_as, code_as limit 100";
		String testSql = "select name as name_as,  code as code_as,  avg(score) as score_as from http.`student` group by name, code  order by name_as, code_as limit 100";

		
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);
		String expected = "select name,code,SUM(score) as $f2,COUNT(score) as $f3 from student group by name, code order by name ASC, code ASC limit 100";
		assertResult(testSql, expected, true);
	}

	@Test
	public void test_1sumToNumber_1F_2G_1L() throws Exception {

		// OK
		String mysql = "select name,  code,  sum(score)  from student where name = 'Tom0' group by name, code limit 2";
		// String testSql ="select name, code, avg(score) from http.`student`
		// where name = 'Tom0' group by name, code limit 2";
		String testSql = "select name,  code,  sum(TO_NUMBER(score, '######'))  from http.`student` where name = 'Tom0' group by name, code limit 2";

		
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);
		String expected = "select name,code,SUM(score) as EXPR$2 from student where name='Tom0' group by name, code order by name ASC, code ASC limit 2";
		assertResult(testSql, expected, false);
	}

	@Test
	public void test_1avgToNumber_1F_2G_1L() throws Exception {
		// String testSql = "select * from http.`/testquery`";
		// String testSql = "select * from http.`/testquery` where firstName =
		// 'Jason' and lastName = 'Hunter'";
		// String testSql = "select * from http.`/testquery` group firstName =
		// 'Jason'";
		// String testSql = "select firstName, from http.`test` where firstName
		// = 'Jason' group by firstName";
		// String testSql ="select firstName , sum(TO_NUMBER(code, '######')) ,
		// avg(TO_NUMBER(code, '######')) , avg(TO_NUMBER(score, '######')) from
		// http.`test` where firstName = 'Jason' group by firstName";

		// String testSql ="select firstName, avg(TO_NUMBER(code, '######')) ,
		// avg(TO_NUMBER(score, '######')) from http.`test` where firstName =
		// 'Jason' or firstName = 'Jason1' group by firstName";

		// OK
		String mysql = "select name,  code,  avg(code)  from student where name = 'Tom0' group by name, code limit 2";
		// String testSql ="select name, code, avg(score) from http.`student`
		// where name = 'Tom0' group by name, code limit 2";
		String testSql = "select name,  code,  avg(TO_NUMBER(code, '######'))  from http.`student` where name = 'Tom0' group by name, code limit 2";

		
		String expected = "select name,code,SUM(code) as $f2,COUNT(code) as $f3 from student where name='Tom0' group by name, code order by name ASC, code ASC limit 2";
		assertResult(testSql, expected, false);
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);

	}

	@Test
	public void test_1avg_1F_2G_1L() throws Exception {
		// String testSql = "select * from http.`/testquery`";
		// String testSql = "select * from http.`/testquery` where firstName =
		// 'Jason' and lastName = 'Hunter'";
		// String testSql = "select * from http.`/testquery` group firstName =
		// 'Jason'";
		// String testSql = "select firstName, from http.`test` where firstName
		// = 'Jason' group by firstName";
		// String testSql ="select firstName , sum(TO_NUMBER(code, '######')) ,
		// avg(TO_NUMBER(code, '######')) , avg(TO_NUMBER(score, '######')) from
		// http.`test` where firstName = 'Jason' group by firstName";

		// String testSql ="select firstName, avg(TO_NUMBER(code, '######')) ,
		// avg(TO_NUMBER(score, '######')) from http.`test` where firstName =
		// 'Jason' or firstName = 'Jason1' group by firstName";

		// OK
		String mysql = "select name,  code,  avg(score) from student where name = 'Tom0' group by name, code limit 2";
		// String testSql ="select name, code, avg(score) from http.`student`
		// where name = 'Tom0' group by name, code limit 2";
		String testSql = "select name,  code,  avg(score) from http.`student` where name = 'Tom0' group by name, code limit 2";

		
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);
		String expected = "select name,code,SUM(score) as $f2,COUNT(score) as $f3 from student where name='Tom0' group by name, code order by name ASC, code ASC limit 2";
		assertResult(testSql, expected, true);
	}

	@Test
	public void test_All() throws Exception {

		String mysql = "select * from  student limit 1000";
		String testSql = "select * from  http.`student` limit 1000";

		

		String expected = "select * from student limit 1000";
		assertResult(testSql, expected, null);
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);

	}

	@Test
	public void test_Sum_All() throws Exception {

		String mysql = "select sum(code) from  student";
		String testSql = "select sum(code) from  http.`student`";

		

		String expected = "select SUM(code) as EXPR$0 from student";
		assertResult(testSql, expected, true);
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);

	}

	@Test
	public void test_Count_All() throws Exception {

		String mysql = "select count(*) from  student";
		String testSql = "select count(*) from  http.`student`";

		
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);

		String expected = "select COUNT(*) as EXPR$0 from student";
		assertResult(testSql, expected, true);

	}

	@Test
	public void test_All_1F() throws Exception {


		String testSql = "select * from  http.`student` where name = 'Tom0'";
		String expected = "select * from student where name='Tom0'";
		assertResult(testSql, expected, false);
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);

	}

	@Test
	// TODO
	public void test_All_2F() throws Exception {

		String mysql = "select * from  student where sex = 0 and score=1299930200";
		String testSql = "select * from  http.`student` where sex = 0 and score=1299930200";

		

		String expected = "select * from student where sex='0' and score='1299930200'";
		assertResult(testSql, expected, true);
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);

	}

	@Test
	public void test_2count_1sum_1Avg_1F_2G() throws Exception {

		// OK
	
		String testSql = "select name,  code, count(name), sum(code), count(code), avg(score) from http.`student` where name = 'Tom0' group by name, code";

		
		String expected = "select name,code,COUNT(name) as EXPR$2,SUM(code) as $f3,COUNT(code) as $f4,COUNT(code) as EXPR$4,SUM(score) as $f6,COUNT(score) as $f7 from student where name='Tom0' group by name, code order by name ASC, code ASC";
		assertResult(testSql, expected, false);
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);

	}

	@Test
	public void test_1Avg_1F_2G() throws Exception {

		// OK
		String mysql = "select name,  code,  avg(score) from student where name = 'Tom0' group by name, code";
		String testSql = "select name,  code,  avg(score) from http.`student` where name = 'Tom0' group by name, code";

		
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);
		String expected = "select name,code,SUM(score) as $f2,COUNT(score) as $f3 from student where name='Tom0' group by name, code order by name ASC, code ASC";
		assertResult(testSql, expected, false);
	}

	// TODO
	public void test_1Sum_1Avg_1AvgToNumber_1F_1G() throws Exception {

		String mysql = "select name, sum(code), avg(code), avg(score) from student where name = 'Tom0' group by name";
		String testSql = "select name, sum(code), avg(TO_NUMBER(code, '######')), avg(score) from http.`student` where name = 'Tom0' group by name";

		
		// List<QueryDataBatch> list = testRunAndReturn(QueryType.SQL,
		// queryString);
		// System.out.println(list);
		String expected = "select name,SUM(code) as $f1,COUNT(code) as $f2,SUM(score) as $f3,COUNT(score) as $f4 from student where name='Tom0' group by name order by name ASC";
		assertResult(testSql, expected, true);

	}

	@Test
	public void test_1Sum_2Avg_1F_1G() throws Exception {

		String mysql = "select name, sum(code), avg(code), avg(score) from student where name = 'Tom0' group by name";
		String testSql = "select name, sum(code), avg(code), avg(score) from http.`student` where name = 'Tom0' group by name";

		String expected = "select name,SUM(code) as $f1,COUNT(code) as $f2,SUM(code) as $f3,SUM(score) as $f4,COUNT(score) as $f5 from student where name='Tom0' group by name order by name ASC";
		assertResult(testSql, expected, true);
	}

	@Test
	// TODO
	public void test_2AvgToNumber_1F_1G() throws Exception {

		String mysql = "select name, avg(code) , avg(score)  from  student where name = 'Tom0' group by name";

		String testSql = "select name, avg(TO_NUMBER(code, '######')) , avg(TO_NUMBER(score, '######'))  from  http.`student` where name = 'Tom0' group by name";

		String expected = "select name,SUM(code) as $f1,COUNT(code) as $f2,SUM(score) as $f3,COUNT(score) as $f4 from student where name='Tom0' group by name order by name ASC";
		assertResult(testSql, expected, true);
	}

	@Test
	public void test_2Avg_1F_1G() throws Exception {

		String testSql = "select name, avg(code), avg(score) from http.`student` where name = 'Tom0' group by name";

		String expected = "select name,SUM(code) as $f1,COUNT(code) as $f2,SUM(score) as $f3,COUNT(score) as $f4 from student where name='Tom0' group by name order by name ASC";
		assertResult(testSql, expected, false);
	}
}
