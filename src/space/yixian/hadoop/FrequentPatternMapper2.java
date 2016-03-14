package space.yixian.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FrequentPatternMapper2 extends Mapper<Object, Object, Text, Text>{
	
	private static HashMap<Text, Text> movRankMap = new HashMap<Text,Text>();
	
	
	//get movie-frequency-map from MapReduce1
	static{
		Configuration configuration = new Configuration();
		configuration.set("fs.defalutFS","hdfs://localhost:8020");
		FileSystem fileSystem = FileSystem.get(configuration);
		BufferedReader reader = null;
		
		try {
					
			FSDataInputStream inputStream = fileSystem.open(new Path(inputAddr));
			reader = new BufferedReader(new InputStreamReader(inputStream));	
			 			 
			HashMap< String, Integer > map = new HashMap<>();
			String aLine;
			while(( aLine = reader.readLine()) != null){
				
				String[] split = aLine.split("\t");
				map.put(split[0], Integer.valueOf(split[1]));

			}

			ArrayList< Map.Entry<String, Integer> > list = new ArrayList< Map.Entry<String, Integer>>(map.entrySet());
			
			Collections.sort( list,new Comparator<Map.Entry<String, Integer>>() {

				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					return o1.getValue().equals(o2.getValue()) ? 0 : ( o1.getValue() < o2.getValue() ? 1 : -1 );
				}
			} );
			
			
			for(Integer i = 0 ; i < list.size(); i++){
			//	String line = i.toString() + "\t" + list.get(i).getKey() + "\t" + list.get(i).getValue() + "\n";
				movRankMap.put(new Text(list.get(i).getKey()), new Text(i.toString()));
			}
			
					
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	
	
	
	
	@Override
	protected void map(Object key, Object value, Mapper<Object, Object, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		
		
		
		
	}
}
