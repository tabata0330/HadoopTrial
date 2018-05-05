import org.apache.hadoop.io.Text;
import java.nio.ByteBuffer;

public class TextIteratorMain {

    public static void main(String args[]){
        Text t = new Text("ABCD");

        ByteBuffer buf = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
        int cp;
        while(buf.hasRemaining() && (cp = Text.bytesToCodePoint(buf)) != -1){
            System.out.println(Integer.toHexString(cp));
        }
    }
}
