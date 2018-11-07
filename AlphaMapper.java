/**
 * import needed packages
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @author skonduri
 *
 */
public class AlphaMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context)	throws IOException,InterruptedException {
		
		String line = value.toString();
		String[] words = line.split("\\s");
		
		for (String word: words) {					
			context.write(new IntWritable(word.length()), new IntWritable(1));
		}			
		
	}

}
