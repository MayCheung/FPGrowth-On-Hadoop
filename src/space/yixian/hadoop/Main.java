package space.yixian.hadoop;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;

public class Main {
	
	public final static Integer SUPPORT_DEGREE = 10;
	
	
	
	static void presenceSort(String inputAddr) throws IOException {
		
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);
		BufferedReader input = null;
		BufferedWriter output = null ;
		
		try {
		
			//FSDataInputStream inputStream = fileSystem.open(new Path(inputAddr));
			 input = new BufferedReader(new FileReader(inputAddr));
			
			//FSDataOutputStream outputStream = fileSystem.create(new Path("hdfs://localhost:8020/sort"));
			 output = new BufferedWriter(new FileWriter("hdfs://localhost:8020/sort"));
			
			
			HashMap< String, Integer > map = new HashMap<>();
			
			String aLine;
			while(( aLine = input.readLine()) != null){
				
				String[] split = aLine.split("\t");
				
				map.put(split[0], Integer.valueOf(split[1]));

			}

			ArrayList< Map.Entry<String, Integer> > list = new ArrayList< Map.Entry<String, Integer>>(map.entrySet());
			
			Collections.sort( list,new Comparator<Map.Entry<String, Integer>>() {

				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					return o1.getValue().equals(o2.getValue()) ? 0 : ( o1.getValue() < o2.getValue() ? 1 :-1 );
				}
			} );
			
			for(Integer i = 0 ; i < list.size(); i++){
				String line = i.toString() + "\t" + list.get(i).getKey() + "\t" + list.get(i).getValue() + "\n";
				output.write(line);;
				
			}
			
			
		
			
			
			
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			try {
				input.close();
				output.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		String[] address = {"hdfs://localhost:8020/u.data","hdfs://localhost:8020/job1"};
		
		int res = ToolRunner.run(new Configuration(), new CountDriver(), address);
		//System.exit(res);
		
		presenceSort(address[1]+"/part-r-00000");
		
		
		
		
		
		
	}
	
}
