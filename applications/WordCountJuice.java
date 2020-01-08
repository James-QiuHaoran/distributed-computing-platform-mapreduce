public class WordCountJuice {
    public static void main(String[] args) throws Exception {
        String key = args[0];
        String[] values = args[1].split("\n");
        System.out.println(key + "," + values.length);
    }
}
