package com.sakthiinfotec.hive.custom;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for upper case conversion.
 * 
 * @author Sakthi
 */
public class ReplaceAllUDFTest {

	/**
	 *  +VE: Test
	 */
	@Test
	public void testReplaceAllUDF() {
		ReplaceAllUDF replace = new ReplaceAllUDF();
		Assert.assertEquals(new Text("AAA,B"), replace.evaluate(new Text("A,AA,AAA,B,BB,BBB"), new Text("BB,A,BBB,AA"), ","));
	}

}
