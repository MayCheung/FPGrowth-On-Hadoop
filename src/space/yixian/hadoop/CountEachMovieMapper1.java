package space.yixian.hadoop;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * count the frequence of movie
 * @author may
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
