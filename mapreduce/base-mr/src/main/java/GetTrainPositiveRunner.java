import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by zjh on 14-9-29.
 */
public class GetTrainPositiveRunner {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length != 2){
            System.err.println("please type the in and out file");
            System.exit(2);
        }

        Job job = new Job(configuration, "give");
        job.setJarByClass(GetTrainPositiveRunner.class);
        job.setMapperClass(GetTrainPositiveMapper.class);
        job.setCombinerClass(GetTrainPositiveReducer.class);
        job.setReducerClass(GetTrainPositiveReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
