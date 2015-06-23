import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by zjh on 14-9-29.
 */
public class GetTrainPositiveReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    private List<Long> visitTimeList = new ArrayList<Long>();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable value : values){
            visitTimeList.add(value.get());
        }

        Collections.sort(visitTimeList);
        //Penultimate day
        long targetVisitTime = visitTimeList.get(visitTimeList.size()-2);
        String[] keyArray = key.toString().split(":");
        String userid = keyArray[0];
        String newsid = keyArray[0];
        Text outputKey = new Text(userid + " " + newsid);
        LongWritable outputValue = new LongWritable(targetVisitTime);
        context.write(outputKey, outputValue);
    }
}
