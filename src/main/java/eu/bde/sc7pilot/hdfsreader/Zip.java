package eu.bde.sc7pilot.hdfsreader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Zip {
    private List<String> fileList = new ArrayList<>();

//    public static void main(String[] args) {
//        String dir = "/media/indiana/data/imgs/imgSaoPaolo/S1A_S6_GRDH_1SDV_20160815T214331_20160815T214400_012616_013C9D_2495.SAFE";
//        String zipFile = "/media/indiana/data/imgs/imgSaoPaolo/S1A_S6_GRDH_1SDV_20160815T214331_20160815T214400_012616_013C9D_2495.zip";

//        Zip zip = new Zip();
//        zip.compressDirectory(dir, zipFile);
//    }

    public void compressDirectory(String dir, String zipFile) {
        File directory = new File(dir);
        getFileList(directory);

        try {
            FileOutputStream fos = new FileOutputStream(zipFile);
            ZipOutputStream zos = new ZipOutputStream(fos);

            for (String filePath : fileList) {
                //System.out.println("Compressing: " + filePath);

                String name = filePath.substring(
                        directory.getAbsolutePath().length() + 1, 
                        filePath.length());

                File file = new File(filePath);
                if (file.isFile()) {
                    // Creates a zip entry.
                    ZipEntry zipEntry = new ZipEntry(name);
                    zos.putNextEntry(zipEntry);

                    // Read file content and write to zip output stream.
                    FileInputStream fis = new FileInputStream(filePath);
                    byte[] buffer = new byte[1024];
                    int length;
                    while ((length = fis.read(buffer)) > 0) {
                        zos.write(buffer, 0, length);
                    }

                    // Close the zip entry and the file input stream.
                    zos.closeEntry();
                    fis.close();
                } else if (file.isDirectory()) {
                    // Creates a zip entry for empty directory. The name
                    // must ends with "/"
                    ZipEntry zipEntry = new ZipEntry(name + "/");
                    zos.putNextEntry(zipEntry);
                    zos.closeEntry();
                }
            }

            // Close zip output stream and file output stream. This will
            // complete the compression process.
            zos.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get files list from the directory recursive to the sub directory.
     */
    private void getFileList(File directory) {
        File[] files = directory.listFiles();
        if (files != null && files.length > 0) {
            for (File file : files) {
                if (file.isFile()) {
                    fileList.add(file.getAbsolutePath());
                } else {
                    fileList.add(file.getAbsolutePath());
                    getFileList(file);
                }
            }
        }

    }
}
