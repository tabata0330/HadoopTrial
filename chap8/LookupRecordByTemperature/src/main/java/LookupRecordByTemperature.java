import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LookupRecordByTemperature extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 2){
            JobBuilder.printUsage(this, "<path> <key>");
            return -1;
        }
        Path path = new Path(args[0]);
        IntWritable key = new IntWritable(Integer.parseInt(args[1]));

        MapFile.Reader[] readers = MapFileOutputFormat.getReaders(path, getConf());
        Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
        Text val = new Text();
//        Writable entry = MapFileOutputFormat.getEntry(reader, partitioner, key, val);
        MapFile.Reader reader = readers[partitioner.getPartition(key, val, readers.length)];
        Writable entry = reader.get(key, val);
        if(entry == null) {
            System.err.println("Key not found: " + key);
            return -1;
        }

        NcdcRecordParser parser = new NcdcRecordParser();
        IntWritable nextKey = new IntWritable();
        do{
            parser.parse(val.toString());
            System.out.printf("%s\t%s\n", parser.getStationId(), parser.getYear());
        }while(reader.next(nextKey, val) && key.equals(nextKey));

        return 0;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new LookupRecordByTemperature(), args);
        System.exit(exitCode);
    }
}
