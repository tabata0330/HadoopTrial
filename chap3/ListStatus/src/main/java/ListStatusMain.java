import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class ListStatusMain {

    public static void main(String args[]) throws Exception{
        String uri = args[0];
        String regex = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

//        Path[] paths = new Path[args.length];
//        for(int i = 0; i < paths.length; i++){
//            paths[i] = new Path(args[i]);
//        }

//        Path path = new Path(args[0]);

        FileStatus[] status = fs.globStatus(new Path(uri), new RegexExcludePathFilter(regex));
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for(Path p: listedPaths){
            System.out.println(p);
        }
    }
}
