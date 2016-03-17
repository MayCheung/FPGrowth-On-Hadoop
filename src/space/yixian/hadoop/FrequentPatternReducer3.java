package space.yixian.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FrequentPatternReducer3 extends Reducer<Text, Text, Text, Text>{
	

	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		HashMap<String, Integer> map = new HashMap<String,Integer>();
		
		for(Text value : values){
			
			String[] allItem = value.toString().split(",");
			
			for(String aItem : allItem){
				if(map.containsKey(aItem)){
					map.put(aItem, (map.get(aItem)+1) );
				}else{
					map.put(aItem,1);
				}
			}
			
			
		}
		
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			if(entry.getValue() >= Main.SUPPORT_DEGREE){
				context.write(new Text(key), new Text(entry.getKey())); 
			}
		}
		
		
		
	}
}
