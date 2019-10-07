public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("Indexer")){
            if (args.length != 2) {
                System.exit(1);
            } else {
                String temp[] = new String[] {args[1], "idf_output" };
                Indexer.main(temp);
            }
        }  else {
            System.exit(1);
        }
    }
}
