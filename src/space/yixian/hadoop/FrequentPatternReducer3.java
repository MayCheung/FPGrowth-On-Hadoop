package space.yixian.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * get all the conditional transactions of one item 
 * to generate this item's conditional FP-tree 
 * @author may
 *
 *e.g:
 * 
 * f a c d g i m p -- user1
 * a b c f l m o -- user2
 * b f h j o -- user3
 * b c k s p -- user4
 * a f c e l p m n -- user5
 * 
 * according to: <f,4> <a,3> <c,4> <m,3> <p,3> <b,3>
 * sorted transactions:
 * f c a m p 
 * f c a b m 
 * f b 
 * c b p
 * f c a m p
 * 
 * MapReduce2:
 * m's conditional transactions:(the item's frequency > m's)
 * f c a m p >> m: f,c,a
 * f c a b m >> m: f,c,a,b
 * f b >> 
 * c b p >> 
 * f c a m p >> m: f,c,a
 * 
 * Reducer3:
 * m: <f,c,a><f,c,a,b><f,c,a>
 * count the frequency of every item in value list
 * m: <f,3> <c,3> <a,3> <b,1>
 * Eliminate the item whose frequency smaller than SUPPORT_DEGREE(3)
 * m's conditional FP-tree: 
 * m: <f,3> <c,3> <a,3> 
 * 
 */
public class FrequentPatternReducer3 extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		HashMap<String, Integer> map = new HashMap<String,Integer>();
		
		for(Text value : values){
			
			String[] allItem = value.toString().split(",");
			
			for(String aItem : allItem){
				if(map.containsKey(aItem)){
					map.put(aItem, (map.get(aItem)+1) );
				}else{
					map.put(aItem,1);
				}
			}
			
			
		}
		
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			
			//get key's conditional FP-trees
			if(entry.getValue() >= Main.SUPPORT_DEGREE){ 
				context.write(new Text(key), new Text(entry.getKey())); //<KEY , entry.getKey1> is frequent pattern
				//all the combination of entry.getKey1/2/3...(anyKey > SUPPORT_DEGREE) + KEY is the frequent pattern 
			}
		}
		
		
		
	}
}
