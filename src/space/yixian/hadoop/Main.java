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

public class Main {
	
	public final static Integer SUPPORT_DEGREE = 50;
	
	public static void main(String[] args) throws Exception {
			
			String HDFSAddr = "hdfs://localhost:8020/";
			String folderAddr = HDFSAddr + "FP/";
			
			String[] address = {HDFSAddr+"u.data",folderAddr+"job1"};		
//			int res = ToolRunner.run(new Configuration(), new CountDriver(), address);
//			System.exit(res);
//			
		presenceSort(folderAddr+"job1/part-r-00000");
							
			
			
	}
	
	
	
	
		
	static void presenceSort(String inputAddr) throws IOException {
		
		Configuration configuration = new Configuration();
		configuration.set("fs.defalutFS","hdfs://localhost:8020");
		FileSystem fileSystem = FileSystem.get(configuration);
		BufferedReader reader = null;
		BufferedWriter writer = null;
		try {
					
			//Path dstDir = new Path("hdfs://localhost:8020/u.data");  
			//FileSystem hdfs = dstDir.getFileSystem(configuration);  

			FSDataInputStream inputStream = fileSystem.open(new Path(inputAddr));
			reader = new BufferedReader(new InputStreamReader(inputStream));	
			 
			FSDataOutputStream outputStream = fileSystem.create(new Path("hdfs://localhost:8020/FP/sort"));
			writer = new BufferedWriter(new OutputStreamWriter(outputStream));
			
			 
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
				String line = i.toString() + "\t" + list.get(i).getKey() + "\t" + list.get(i).getValue() + "\n";
				writer.write(line);
			}
			
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				writer.close();
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
	
	
	
	
	
	
}
