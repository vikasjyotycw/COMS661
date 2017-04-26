/*
 * GetTupleFromRelatioIterator
 * By Rakesh Kumar Shah and Vikas Jyoti
 */


package Demo;

import StorageManager.Storage;
import PTCFramework.ProducerIterator;


public class GetTupleFromRelationIterator implements ProducerIterator<byte[]> {
	
	Storage storage;
	
	int pageSize;
	int currentPage = 0;
    int nextPage = -1;
    
    //variables related to tuples 
    int tLen;
    int tRead = 0;
    int maxTuples;
    
    byte[] currentBuffer;
    String fName; //File Name

    //default constructor to set all the tuples, page and storage
    public GetTupleFromRelationIterator(String string, int n, int n2) throws Exception 
    {
    	this.tLen = n;
    	this.fName = string;
        this.storage = new Storage();
        this.storage.LoadStorage(string);
        this.pageSize = this.storage.pageSize;
        this.currentPage = n2;
    }

    private int toInt(byte[] arrby, int n) 
    {
        int num = 0;
        for (int i = 0; i < 4; ++i) 
        {
            num <<= 8;
            num |= arrby[n + i] & 255;
        }
        return num;
    }
    
    public boolean hasNext() 
    {
        if (this.nextPage == -1 && this.tRead == this.maxTuples) 
        {
            return false;
        }
        return true;
    }
    public byte[] next() 
    {
        if (this.tRead == this.maxTuples)
        {
            if (this.nextPage == 0) 
            {
                return null;
            }
            this.currentBuffer = new byte[this.pageSize];
            try {
                this.storage.ReadPage((long)this.nextPage, this.currentBuffer);
                this.currentPage = this.nextPage;
                this.nextPage = this.toInt(this.currentBuffer, 4);
                this.maxTuples = this.toInt(this.currentBuffer, 0);
                this.tRead = 0;
                byte[] arrby = new byte[this.tLen];
                for (int i = 0; i < this.tLen; ++i) 
                {
                    arrby[i] = this.currentBuffer[8 + this.tLen * this.tRead + i];
                }
                ++this.tRead;
                return arrby;
            }
            catch (Exception vara) {
                vara.printStackTrace();
            }
        } else {
            byte[] arrby = new byte[this.tLen];
            for (int i = 0; i < this.tLen; ++i)
            {
                arrby[i] = this.currentBuffer[8 + this.tLen * this.tRead + i];
            }
            ++this.tRead;
            return arrby;
        }
        return null;
    }

    public void open() throws Exception 
    {
        this.currentBuffer = new byte[1024];
        this.storage.ReadPage((long)this.currentPage, this.currentBuffer);
        this.nextPage = this.toInt(this.currentBuffer, 4);
        this.maxTuples = this.toInt(this.currentBuffer, 0);
        this.tRead = 0;
    }
    
    public void remove() {}
    public void close() throws Exception {}
}