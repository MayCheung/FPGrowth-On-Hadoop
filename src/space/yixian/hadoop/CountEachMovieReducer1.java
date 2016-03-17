package space.yixian.hadoop;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * count the frequency of movies
 * 
 * @author may
 * 
 * e.g:
 * SUPPORT_DEGREE = 3
 * 
 * Reducer1 input:
 * <f,1> <a,1> <c,1> <d,1> <g,1> <i,1> <m,1> <p,1> <a,1> <b,1>... 
 * 
 * Reducer1 output: <item, frequency>
 * Eliminate the item whose frequency smaller than SUPPORT_DEGREE(3)
 * <f,4> <a,3> <c,4> <m,3> <p,3> <b,3> (other items' frequency smaller than 3)
 * 
 * 
 * 
 * 
 * <f,4>: (in Mapper1 input)
 * <f> a c d g i m p
 * a b c <f> l m o
 * b <f> h j o
 * b c k s p
 * a <f> c e l p m n
 *
 */
public class CountEachMovieReducer1 extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		Integer count = 0;
		for(Text value : values){
			count++;
		}
		
		if(count >= Main.SUPPORT_DEGREE ){
			context.write(key, new Text(count.toString()));
		}
		
	}
	
}
