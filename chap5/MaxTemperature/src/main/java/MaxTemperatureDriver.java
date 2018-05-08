import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

public class MaxTemperatureDriver extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = getConf();
        conf.setBoolean(JobContext.TASK_PROFILE, true);
        conf.set(JobContext.TASK_PROFILE_PARAMS, "-agentlib:hprof=cpu=samples,heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s");
        conf.set(JobContext.NUM_MAP_PROFILES, "0-2");
        conf.set(JobContext.NUM_REDUCE_PROFILES, "");

        Job job = new Job(conf, "Max temperature");
        job.setJarByClass(getClass());

//        for(Map.Entry<String, String> entry: conf){
//            System.out.printf("%s = %s\n", entry.getKey(), entry.getValue());
//        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
        System.exit(exitCode);
    }
}
