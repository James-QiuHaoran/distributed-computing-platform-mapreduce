public class ReverseWebLink_Juice {
    public static void main(String[] args) throws Exception {
        String key = args[0];
        String[] values = args[1].split("\n");
        String result = "";
        for(String val : values){
            result += val + " ";
        }
        System.out.println(key + "," + result);
    }
}
