package space.yixian.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TransactionsReducer2 extends Reducer<Text, Text, Text, Text> {
	
	//input:  key: userid   value: movie-rank-list   --  a transaction
	//output: key1: a movie in movie-rank-list 	value1: the movies which rank lower than key1  --  conditional transactions
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
			
		TreeMap<Integer, String> map = new TreeMap<Integer,String>();
		
		for(Text value : values){
			String movie = value.toString().split(",")[0];
			Integer rank = Integer.valueOf(value.toString().split(",")[1]);
			
			map.put(rank, movie);
		}
		
		
		ArrayList<String> arrayList = new ArrayList<>();
		for(Map.Entry<Integer, String> entry : map.entrySet()){
			
			arrayList.add(entry.getValue()); // movie 
			//context.write(new Text(entry.getValue()), new Text((entry.getKey()).toString()) ); //movie rank
		}
		
	

			String valueStr = null;
			for(int i = arrayList.size()-1 ; i > 0; i--){

				for (int j = i - 1 ; j >= 0; j--){
					
					if(valueStr == null) {
						valueStr = arrayList.get(j);
					}else{
						valueStr += "," + arrayList.get(j);
					}
					
				}
				
				
				context.write(new Text(arrayList.get(i)), new Text(valueStr));
				
			}
		
		
		
		
	}
	
	
	
}
