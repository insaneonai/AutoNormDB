package org.jeyadevan.io;

public interface IPageManager {
    int allocatePage();
    void writePage(int pageNo, byte[] data);
    byte[] readPage(int pageNo, int length);
}
