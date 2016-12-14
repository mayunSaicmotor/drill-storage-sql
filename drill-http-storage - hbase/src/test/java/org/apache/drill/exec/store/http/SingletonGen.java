package org.apache.drill.exec.store.http;

import java.io.File;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JDocComment;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;

/**
 * This class will generate a singleton class "Singleton.java" and a test class
 * "SingletonTest.java" under the specified package.
 * 
 * @author Sonia (sxyu@cn.ibm.com)
 * 
 */
public class SingletonGen {
	public void genSingleton() throws Exception {
		JCodeModel cm = new JCodeModel();
		JType type = cm.parseType("Singleton");
		File destDir = new File("src");
		JDefinedClass dc = cm._class("org.apache.drill.exec.store.http.Singleton");
		// 定义静态成员变量
		dc.field(JMod.PRIVATE + JMod.STATIC, type, "instance");
		// 定义单例类 Singleton 的构造函数
		dc.constructor(JMod.PRIVATE);

		// 生成 Singleton 类的成员方法 getInstanceMethod
		JMethod getInstanceMethod = dc.method(JMod.PUBLIC + JMod.STATIC, type, "getInstance");
		JBlock getInstanceBody = getInstanceMethod.body();
		JFieldRef fieldRef = JExpr.ref("instance");
		JConditional conditionIf = getInstanceBody._if(fieldRef.eq(JExpr._null()));
		JBlock thenPart = conditionIf._then();
		thenPart.assign(fieldRef, JExpr._new(type));
		getInstanceBody._return(fieldRef);
		// 生成 Singleton 类的成员方法 sayHelloMethod
		JMethod sayHelloMethod = dc.method(JMod.PUBLIC, cm.parseType("void"), "sayHello");
		// 生成方法级的 javadoc
		sayHelloMethod.javadoc().add("This method will say Hello to the name.");
		JBlock sayHelloBody = sayHelloMethod.body();
		sayHelloMethod.param(cm.parseType("String"), "name");
		JClass sys = cm.ref("java.lang.System");
		JFieldRef ot = sys.staticRef("out");
		JExpression sentance1 = JExpr.lit("Hello ").invoke("concat").arg(JExpr.ref("name"));
		JExpression sentance2 = sentance1.invoke("concat").arg("!");
		sayHelloBody.invoke(ot, "println").arg(sentance2);
		cm.build(destDir);
	}

	public void genTest() throws Exception {
		JCodeModel cm = new JCodeModel();
		File destDir = new File("src");
		JDefinedClass dc = cm._class("org.apache.drill.exec.store.http.singletonTest");
		// 生成类级的 javadoc
		JDocComment jdoc = dc.javadoc();
		jdoc.add("This is the class to test the Singleton class.");
		jdoc.addXdoclet("author Sonia (sxyu@cn.ibm.com)");
		// 生成 main 方法
		JMethod mainMethod = dc.method(JMod.PUBLIC + JMod.STATIC, cm.parseType("void"), "main");
		JBlock mainBody = mainMethod.body();
		mainMethod.param(cm.parseType("String[]"), "args");
		JClass singleton = cm.ref("Singleton");
		JInvocation getIns = singleton.staticInvoke("getInstance");
		mainBody.invoke(getIns, "sayHello").arg(JExpr.lit("CodeModel"));
		cm.build(destDir);
	}

	public static void main(String args[]) {
		SingletonGen sg = new SingletonGen();
		try {
			sg.genSingleton();
			sg.genTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
