import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;


public class MapFileReadDemoMain {

    public static void main(String args[]) throws IOException{
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        MapFile.Reader reader = null;
        Path path = new Path(uri);

        try{
            reader = new MapFile.Reader(path, conf);
            WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while(reader.next(key, value)){
                System.out.printf("%s\t%s\n", key,value);
            }
//            reader.get(new IntWritable(128), value);
//            System.out.println(value.toString());
        }finally {
            IOUtils.closeStream(reader);
        }

    }
}
