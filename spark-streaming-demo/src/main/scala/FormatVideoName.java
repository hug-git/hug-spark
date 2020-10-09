import java.io.File;

public class FormatVideoName {
    public static void main(String[] args) {
//        File file = new File("D:\\ReceiveFromFeiq\\12_尚硅谷大数据项目之电商数仓\\4.视频\\day03");
        File file = new File(args[0]);
        File[] files = file.listFiles();
        for (File file1 : files) {
            String path = file1.getAbsolutePath();
            String newN = path.replaceAll("尚硅谷-\\D+（", "").replace("）", "");
            String newN1 = newN.replaceAll(" +", "_");
            file1.renameTo(new File(newN1));
        }
    }
}
