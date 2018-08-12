package testing;

import app.Driver;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class TestDriver {
    public static void main(String[] args) throws IOException{
        String testOutput1 = "/tmp/1", testOutput2 = "/tmp/2", testOutput3 = "/tmp/3";
        String[] testArgs = {args[0], testOutput1, testOutput2, testOutput3};
        Driver.main(testArgs);

        if (!equal(args[1],testOutput1)){
            System.out.println("Test 1 failed");
        }
        if (!equal(args[2],testOutput2)){
            System.out.println("Test 2 failed");
        }
        if (!equal(args[3],testOutput3)){
            System.out.println("Test 3 failed");
        }
        System.out.println("Test Complete");
    }

    public static boolean equal(String basePath, String toTest) throws IOException {
        File dir = new File(toTest);
        File[] listOfFiles = dir.listFiles();

        for(File f:listOfFiles){
            if (f.getName().startsWith("part")){
                System.out.println(f.getName());
                byte[] testContent = Files.readAllBytes(Paths.get(f.getAbsolutePath()));
                byte[] baseContent = Files.readAllBytes(Paths.get(new File(basePath).getAbsolutePath()));
                return Arrays.equals(baseContent, testContent);
            }
        }
        return false;
    }
}
