import util.Utils;

public class Test {
    public static void main(String[] args) {
        String content = "12345678";
        byte[] digest = Utils.getSHA256(content);
        System.out.println(Utils.getSHA256String(content));
        System.out.println(digest[digest.length-1] % 2);
    }
}
