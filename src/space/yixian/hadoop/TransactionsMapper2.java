package space.yixian.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 1.sort and rank the output of Mapper1
 * 2.sort the rated movie of one user by the frequency 
 * 
 * @author may
 * 
 * e.g.:
 * in Reducer1's result:
 * <f,4> <a,3> <c,4> <m,3> <p,3> <b,3>
 * 
 * 1.sort the output of Mapper1 in descending order:
 * <f,4> <c,4> <a,3> <m,3> <p,3> <b,3>
 * rank them: <f,0> <a,1> <c,2> <m,3> <p,4> <b,5> -- <f,a,c,m,p,b>
 * 
 * 2.sort the rated movie of one user according to the order of step1.'s result 
 *  	2.1. Mapper2: read a line of dataset, if the movie is frequent, output:<user, (movie,rank)>
 *  	2.2. Reducer2: sort the rated movie of one user and get conditional transactions	
 * 
 * 		e.g.: 	a f c e l p m n -- user5	
 * 			2.1. 
 * 			according to rank<f,a,c,m,p,b>, eliminate the infrequent item <d,g,i>, get: <a,f,c,p,m>
 * 		    set <a,f,c,p,m> in Mapper2 output in format <user, (movie,rank)>:
 * 				<user5, (a,1)> <user5, (f,0)> <user5, (c,2)> <user5, (p,4)> <user5, (m,3)>
 * 			2.2.
 * 			sort <a,f,c,p,m> according to the rank sequence <f,a,c,m,p,b>, get <f,a,c,m,p> ( namely sorted transactions )
 * 			get conditional transactions of <f,a,c,m,p>: 
 * 				p: f,a,c,m
 * 				m: f,a,c
 * 				c: f,a
 * 				a: f
 */
public class TransactionsMapper2 extends Mapper<Object, Object, Text, Text>{
	
	private static HashMap<String, String> movRankMap = new HashMap<String,String>();
	
	
	
	//get movie-frequency-map from MapReduce1's result
	static{
		Configuration configuration = new Configuration();
		configuration.setBoolean("dfs.support.append", true);
		configuration.set("fs.defalutFS","hdfs://localhost:8020");
		
		FileSystem fileSystem = null;
		FileStatus[] status = null; 
		
		String addr = "hdfs://localhost:8020/FP/job1/part-r-*";
	//	String addr = "/home/may/app/part-r-*";
		
		try {
			fileSystem = FileSystem.get(configuration);
			status = fileSystem.globStatus(new Path(addr));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		
		BufferedReader reader = null;
		for(FileStatus fileStatus : status){
		
			try {
						
				FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath());
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
					movRankMap.put(list.get(i).getKey(), i.toString()); // <movie-rank>
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
	}
	
	
	
	
	
	@Override
	protected void map(Object key, Object value, Mapper<Object, Object, Text, Text>.Context context)
			throws IOException, InterruptedException {

		
		String[] split = value.toString().split("\t");
		String user = split[0];
		String movie = split[1];
		
		
		if(movRankMap.containsKey(movie)){ // infrequent items eliminated
			
			String rank = movRankMap.get(movie);
			context.write(new Text(user), new Text(movie+","+rank));
			
		}
		
		
		
		
		
	}
}
