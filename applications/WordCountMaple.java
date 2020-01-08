import java.util.StringTokenizer;

public class WordCountMaple {
    public static void main(String[] args) throws Exception {
        StringTokenizer itr = new StringTokenizer(args[0]);
        while (itr.hasMoreTokens()) {
            System.out.println(itr.nextToken() + ",1");
        }
    }
}
