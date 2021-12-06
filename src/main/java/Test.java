import java.text.SimpleDateFormat;
import util.Utils;

public class Test {
    public static void main(String[] args) {
//        String content = "12345678";
//        byte[] digest = Utils.getSHA256(content);
//        System.out.println(Utils.getSHA256String(content));
//        System.out.println(digest[digest.length-1] % 2);

        long timeStamp = System.currentTimeMillis();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(timeStamp);
        System.out.println(dateString);
    }
}
