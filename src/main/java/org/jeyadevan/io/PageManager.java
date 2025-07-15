package org.jeyadevan.io;

import org.jeyadevan.storage.Constants;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

public class PageManager implements IPageManager {
    private final RandomAccessFile file;
    private final AtomicInteger nextFreePage;

    public PageManager(File dataFile) throws IOException{
        this.file = new RandomAccessFile(dataFile, "rw");
        long fileLength = file.length();
        this.nextFreePage = new AtomicInteger((int) (fileLength / Constants.PAGE_SIZE));
    }

    @Override
    public int allocatePage(){
        return nextFreePage.getAndIncrement();
    }

    @Override
    public void writePage(int pageNo, byte[] data){
        // Write a page at a time.
        try{
            if (data.length > Constants.PAGE_SIZE){
                throw new IllegalArgumentException("invalid data length");
            }
            file.seek((long) pageNo * Constants.PAGE_SIZE);
            file.write(data);
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
