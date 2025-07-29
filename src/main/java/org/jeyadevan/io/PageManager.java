package org.jeyadevan.io;

import org.jeyadevan.storage.Constants;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

public class PageManager implements IPageManager {
    private final RandomAccessFile file;
    private final AtomicInteger nextFreePage;
    private final File metaFile;

    public PageManager(File dataFile) throws IOException{
        this.file = new RandomAccessFile(dataFile, "rw");
        this.metaFile = new File(dataFile.getName() + ".meta");

        if (!metaFile.exists()){
            long fileLength = file.length();
            int initialPage = (int) (fileLength / Constants.PAGE_SIZE);
            writeMeta(initialPage);
            this.nextFreePage = new AtomicInteger(initialPage);
        }
        else{
            System.out.println(readMeta() + 1);
            this.nextFreePage = new AtomicInteger(readMeta() + 1);
        }
    }

    private int readMeta() throws IOException{
        try(RandomAccessFile meta = new RandomAccessFile(metaFile, "r")){
            return meta.readInt();
        }
    }

    private void writeMeta(int pageNo) throws IOException{
        try(RandomAccessFile meta = new RandomAccessFile(metaFile, "rw")){
            meta.seek(0);
            meta.writeInt(pageNo);
        }
    }

    @Override
    public int allocatePage(){
        int pageNo = nextFreePage.getAndIncrement();
        try{
            writeMeta(pageNo);
        }
        catch (IOException e){
            throw new RuntimeException("Unable to persist meta info i.e page." + e);
        }
        return pageNo;
    }

    @Override
    public void writePage(int pageNo, byte[] data){
        // Write a page at a time.
        try{
            if (data.length > Constants.PAGE_SIZE){
                throw new IllegalArgumentException("invalid data length");
            }
            byte[] padded = new byte[Constants.PAGE_SIZE];
            System.arraycopy(data, 0, padded, 0, data.length);

            System.out.println("Padded length "+ padded.length);

            System.out.println("SEEKING: " + (long) pageNo * Constants.PAGE_SIZE);

            file.seek((long) pageNo * Constants.PAGE_SIZE);
            file.write(padded);
        }
        catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] readPage(int pageNo, int length){
        try{
            file.seek((long) pageNo * Constants.PAGE_SIZE);
            byte[] buffer = new byte[length];
            file.readFully(buffer);
            return buffer;
        }
        catch (IOException e){
            throw new RuntimeException("Error reading page " + e);
        }
    }
}
