package space.yixian.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class TransactionsDriver extends Configured implements Tool{
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("mapreduce.job.jar","FP.jar");
		
		Job job = Job.getInstance(configuration);
		
		job.setMapperClass(TransactionsMapper2.class);
		job.setReducerClass(TransactionsReducer2.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		return job.waitForCompletion(true) ? 0 : 1 ;
	}
}
