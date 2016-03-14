package space.yixian.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequentPatternMapper2 extends Mapper<Object, Object, Text, Text>{
	@Override
	protected void map(Object key, Object value, Mapper<Object, Object, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub 
		super.map(key, value, context);
	}
}
