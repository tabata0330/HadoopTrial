import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

import java.net.URI;


public class PooledStreamCompressorMain {

    public static void main(String args[]) throws Exception{
        String codecClassname = args[0];
        Class<?> codecClass = Class.forName(codecClassname);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        Compressor compressor = null;

        FileSystem fs = FileSystem.get(URI.create(args[1]), conf);
        try{
            compressor = CodecPool.getCompressor(codec);
            CompressionOutputStream out = codec.createOutputStream(fs.create(new Path(args[1])), compressor);
            IOUtils.copyBytes(System.in, out, 4096, false);
            out.finish();
        }finally {
            CodecPool.returnCompressor(compressor);
        }
    }
}
