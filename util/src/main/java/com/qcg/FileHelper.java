package com.qcg;

import java.io.*;

/**
 * Created by chenguang2 on 2014/11/28.
 */
public class FileHelper {
    //第一种获取文件内容方式
    public byte[] getContent(String filePath) throws IOException {
        File file = new File(filePath);
        long fileSize = file.length();
        if (fileSize > Integer.MAX_VALUE) {
            System.out.println("file too big...");
            return null;
        }
        FileInputStream fi = new FileInputStream(file);
        byte[] buffer = new byte[(int) fileSize];
        int offset = 0;
        int numRead = 0;
        while (offset < buffer.length
                && (numRead = fi.read(buffer, offset, buffer.length - offset)) >= 0) {
            offset += numRead;
        }
        // 确保所有数据均被读取
        if (offset != buffer.length) {
            throw new IOException("Could not completely read file "
                    + file.getName());
        }
        fi.close();
        return buffer;
    }
    //第二种获取文件内容方式
    public byte[] getContent2(String filePath) throws IOException
    {
        FileInputStream in=new FileInputStream(filePath);
        ByteArrayOutputStream out=new ByteArrayOutputStream(1024);
        System.out.println("bytes available:"+in.available());
        byte[] temp=new byte[1024];
        int size=0;
        while((size=in.read(temp))!=-1)
        {
            out.write(temp,0,size);
        }
        in.close();
        byte[] bytes=out.toByteArray();
        System.out.println("bytes size got is:"+bytes.length);
        return bytes;
    }
    //将byte数组写入文件
    public void createFile(String path, byte[] content) throws IOException {
        FileOutputStream fos = new FileOutputStream(path);
        fos.write(content);
        fos.close();
    }
    /**
     * 写入文件
     *
     * @param filePathAndName
     *            String 如 c:\\1.txt 绝对路径
     */
    public static void writeFile(String filePathAndName, String fileContent) {
        try {
            File f = new File(filePathAndName);
            if (!f.exists()) {
                f.createNewFile();
            }
            //OutputStreamWriter write = new OutputStreamWriter(new FileOutputStream(f),"UTF-8");
            //OutputStreamWriter write = new OutputStreamWriter(new FileOutputStream(f),"GBK");
            //OutputStreamWriter write = new OutputStreamWriter(new FileOutputStream(f),"GB2312");
            OutputStreamWriter write = new OutputStreamWriter(new FileOutputStream(f),"GB2312");
            BufferedWriter Writer=new BufferedWriter(write);
            Writer.write(fileContent);
            Writer.close();
        } catch (Exception e) {
            //System.out.println("写文件内容操作出错");
            e.printStackTrace();
        }
    }

}