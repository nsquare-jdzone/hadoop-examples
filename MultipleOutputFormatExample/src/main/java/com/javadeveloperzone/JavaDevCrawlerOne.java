package com.javadeveloperzone;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class JavaDevCrawlerOne {

    public void process(){

        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(
                    new File("H:\\Work\\broken_links_alexa.txt")
            ));
            String line = null;
            int cnt=0;
            while ((line=bufferedReader.readLine())!=null){
                if(line.trim().length()==0)
                    continue;
                String data [] = line.split(",");
                if(data!=null && data.length==2){
                    sendGET(data[0].replace("-SNAPSHOT",""),data[1]);
                }else{
                    System.out.println("Incorrect data:"+line);
                }
                cnt++;

                if(cnt%10==0){
                    System.out.println("Processed : "+cnt);
                }
//                break;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                bufferedReader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void sendGET(String broenLink ,String url)  {
        HttpURLConnection con = null;
        try{
            URL obj = new URL(url);
            con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = con.getResponseCode();
            //System.out.println("GET Response Code :: " + responseCode);
            if (responseCode == HttpURLConnection.HTTP_OK) { // success
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                // print result
                //System.out.println(response.toString());
                if(response.toString().contains(broenLink)){
                    System.out.println("Broken Link : "+broenLink+","+url);
                }
            } else {
                System.out.println("GET request not worked"+broenLink+","+url);
            }
        }catch (Exception e){
            System.out.println("Error : "+broenLink+","+url);
        }


    }

    public static void main(String[] args) {
        JavaDevCrawlerOne javaDevCrawler = new JavaDevCrawlerOne();
        javaDevCrawler.process();
    }
}
