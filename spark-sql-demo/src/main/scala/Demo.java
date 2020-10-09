import java.io.File;

public class Demo {
    public static void main(String[] args) {
        File file = new File("D:\\ReceiveFromFeiq\\13_Spark\\4.视频\\day10");
        File[] files = file.listFiles();
        for (File file1 : files) {
            String path = file1.getAbsolutePath();
            String newN = path.replaceAll("尚硅谷-Spark-Spark\\w+（", "").replace("）", "");
            String newN1 = newN.replaceAll(" +", "_");
            file1.renameTo(new File(newN1));
        }
    }
}
