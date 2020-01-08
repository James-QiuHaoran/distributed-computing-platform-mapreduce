import java.util.StringTokenizer;

public class ReverseWebLinkMaple {
    public static void main(String[] args) throws Exception {
        StringTokenizer itr = new StringTokenizer(args[0], "\n");
        while (itr.hasMoreTokens()) {
            System.out.println(itr.nextToken().split("\\s+")[1] + itr.nextToken().split("\\s+")[0]);
        }
    }
}
