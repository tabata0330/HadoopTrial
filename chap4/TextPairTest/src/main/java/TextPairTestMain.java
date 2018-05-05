import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;

public class TextPairTestMain {

    public static void main(String args[]) throws IOException{
//        Text t1 = new Text("Hello");
//        Text t2 = new Text("World");
        TextPair tp1 = new TextPair();
//        tp1.set(t1, t2);
//        DataOutputStream out = new DataOutputStream(System.out);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        DataInputStream in = null;
        DataOutputStream out = fs.create(new Path(args[0]));
        try{
            in = fs.open(new Path(args[0]));
//            in = new DataInputStream(System.in);
            tp1.readFields(in);
            System.out.println("first: "+tp1.getFirst());
            System.out.println("second: "+tp1.getSecond());
            tp1.write(out);
        }finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
