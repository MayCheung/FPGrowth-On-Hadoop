package space.yixian.hadoop;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * count the frequency of movies
 * @author may
 * 
 * e.g:
 * 
 * f a c d g i m p -- user1
 * a b c f l m o -- user2
 * b f h j o -- user3
 * b c k s p -- user4
 * a f c e l p m n -- user5
 * 
 * 
 * Mapper1 input: (transactions)
 * user1 f 
 * user1 a
 * user2 a
 * ......
 * 
 * Mapper1 output:
 * <f,1> <a,1> <c,1> <d,1> <g,1> <i,1> <m,1> <p,1> <a,1> <b,1>... 
 *
 */
public class CountEachMovieMapper1 extends Mapper<Object, Object, Text, Text> {
	
	//input : userid movieid rate time
	@Override
	protected void map(Object key, Object value, Mapper<Object, Object, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(" |\t");
		Text movieId = new Text(split[1]);
		
		context.write(movieId, new Text("1"));
		
	}
}
