import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by zjh on 14-9-29.
 * to generate the positive train data
 */
public class GetTrainPositiveMapper extends Mapper<Object, Text, Text, LongWritable>{
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");
        String userid = tokens[0];
        String newsid = tokens[1];
        long visit_timestamp = Long.parseLong(tokens[2]);
        Text outputKey = new Text(userid+":"+newsid);
        LongWritable outputValue = new LongWritable(visit_timestamp);
        context.write(outputKey, outputValue);
    }
}
