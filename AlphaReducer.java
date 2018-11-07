/**
 * 
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author skonduri
 *
 */
public class AlphaReducer extends Reducer<IntWritable, IntWritable, IntWritable, LongWritable> {
	public void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context)
			throws IOException,InterruptedException {
		long sum=0;
				
		for(IntWritable value: values)
		{
			sum++;
		}
		context.write(key, new LongWritable(sum));
		

	}
}
