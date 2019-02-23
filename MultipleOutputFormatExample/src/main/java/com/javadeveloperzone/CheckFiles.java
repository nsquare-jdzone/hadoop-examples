package com.javadeveloperzone;

import java.io.*;

public class CheckFiles {

    public void readContent(File file){

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line=bufferedReader.readLine())!=null){
                if(line.contains("http://www.tenlister.me/")){
                    System.out.println(file.getPath());
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void readFIle(File file){
        if(file.isFile()){
            readContent(file);
        }else{
            File file1[] = file.listFiles();
            if(file1==null)
                return;
            for(File file2 : file1){
                if(file2.isFile()){
                    readContent(file2);
                }else{
                    readFIle(file2);
                }
            }
        }
    }
    public static void main(String[] args) {
        CheckFiles checkFiles = new CheckFiles();
        File file = new File("C:\\Users\\Nitin\\Downloads\\Impreza\\Impreza");
        checkFiles.readFIle(file);
    }
}
