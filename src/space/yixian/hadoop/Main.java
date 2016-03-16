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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;


public class Main {
	
	public final static Integer SUPPORT_DEGREE = 50;
	
	public static void main(String[] args) throws Exception {
			
			String HDFSAddr = "hdfs://localhost:8020/";
			String folderAddr = HDFSAddr + "FP/";
			
//			String[] address = {HDFSAddr+"u.data",folderAddr+"job1"};		
//			int res = ToolRunner.run(new Configuration(), new CountDriver(), address);
//			System.exit(res);
//			
			//presenceSort(folderAddr+"job1/par*");

			String[] address = {HDFSAddr+"u.data",folderAddr+"job2"};		
			int res = ToolRunner.run(new Configuration(), new TransactionsDriver(), address);

			  
	}
	
	
	
	
		
	static void presenceSort(String inputAddr) throws IOException {
		
		Configuration configuration = new Configuration();
		configuration.setBoolean("dfs.support.append", true);
		configuration.set("fs.defalutFS","hdfs://localhost:8020");
		FileSystem fileSystem = FileSystem.get(configuration);

		BufferedReader reader = null;
		BufferedWriter writer = null;
		
		FileStatus[] status = fileSystem.globStatus(new Path(inputAddr));
		HashMap< String, Integer > map = new HashMap<>();
		
		
		for(FileStatus fileStatus : status){
	
			try {
				
				FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath());
				reader = new BufferedReader(new InputStreamReader(inputStream));									 
			
				String aLine;
				while(( aLine = reader.readLine()) != null){
					
					String[] split = aLine.split("\t");
					map.put(split[0], Integer.valueOf(split[1]));

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
		
		
		
		if(map!=null){
			ArrayList< Map.Entry<String, Integer> > list = new ArrayList< Map.Entry<String, Integer>>(map.entrySet());
			
			Collections.sort( list,new Comparator<Map.Entry<String, Integer>>() {

				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					return o1.getValue().equals(o2.getValue()) ? 0 : ( o1.getValue() < o2.getValue() ? 1 : -1 );
				}
			} );
			
			Path appFilePath = new Path("hdfs://localhost:8020/FP/sort");
			FSDataOutputStream outputStream = null;
			try {
					if(fileSystem.exists(appFilePath)) {
						outputStream = fileSystem.append(appFilePath);
					}else{
						outputStream = fileSystem.create(appFilePath);
					}
				writer = new BufferedWriter(new OutputStreamWriter(outputStream));
				for(Integer i = 0 ; i < list.size(); i++){
					String line = i.toString() + "\t" + list.get(i).getKey() + "\t" + list.get(i).getValue() + "\n";
					writer.write(line);
				}
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				
				try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	
	
	
	
	
}
