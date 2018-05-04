import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class FileDecompressorMain {
    //outにつなげば圧縮されて、inにつなげば解凍するのかな…？
    public static void main(String args[]) throws Exception{
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        System.out.println("Factory: "+factory.toString());
        CompressionCodec codec = factory.getCodec(inputPath);
        System.out.println("codec: "+codec.toString());
        if(codec == null){
            System.err.println("No codec found for "+uri);
            System.exit(1);
        }

        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        System.out.println("outputUri: "+outputUri);

        InputStream in = null;
        OutputStream out = null;
        try{
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, conf);
        }finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
