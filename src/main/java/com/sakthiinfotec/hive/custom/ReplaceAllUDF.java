package com.sakthiinfotec.hive.custom;

import com.google.common.base.Joiner;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Custom ReplaceAll conversion UDF.
 * 
 * @author peng
 */
@Description(name = "replaceAllUDF", value = "Returns replace of a given string", extended = "SELECT replaceAllUDF('A,AA,AAA,B,BB,BBB','A,AA,BB,BBB');")
public class ReplaceAllUDF extends UDF {

	private Text result = new Text();

	public Text evaluate(Text str, Text nextStr, Text regexText) {
		if (str == null) {
			return null;
		}
		if (nextStr == null) {
			return str;
		}
		String regex = regexText.toString();
		List<String> prevList = new ArrayList<>(Arrays.asList(str.toString().split(regex)));
		prevList.removeAll(new ArrayList<>(Arrays.asList(nextStr.toString().split(regex))));
		String resultStr = Joiner.on(regex).join(prevList);
		result.set(resultStr);
		return result;
	}

}
