import java.text.SimpleDateFormat;
import java.util.Scanner;

import util.Utils;

public class Test {
    public static void main(String[] args) {
//        String content = "12345678";
//        byte[] digest = Utils.getSHA256(content);
//        System.out.println(Utils.getSHA256String(content));
//        System.out.println(digest[digest.length-1] % 2);

//        long timeStamp = System.currentTimeMillis();
//        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String dateString = formatter.format(timeStamp);
//        System.out.println(dateString);

        double a, b, c, d, e, mean;
        Scanner in = new Scanner(System.in);
        while(true) {
            a = in.nextDouble();
            b = in.nextDouble();
            c = in.nextDouble();
            d = in.nextDouble();
            e = in.nextDouble();
            mean = (a+b+c+d+e)/5.0;
            System.out.println(mean);
        }


    }
}
