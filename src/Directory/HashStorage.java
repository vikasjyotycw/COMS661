package Directory;

import java.io.IOException;
import java.io.RandomAccessFile;

public class HashStorage {
	private static String fileName;
	private static long fileSize;
	public static RandomAccessFile file;
	public static int pageSize;
	private static int headerSize = 16;
	private static int tupleSize = 4;
	private static int writeableAreaSize;
	//private int freeSize = 32;


	private static int bitMapSize;
	public static int numPages;
	private static int numRead;
	private static int numWritten;




	public static void CreateStorage(String fileName,int pageSize, int fileSize) throws Exception{
		numPages=1;
		writeableAreaSize = pageSize-headerSize;

		file= new RandomAccessFile(fileName, "rw");
		file.setLength(fileSize);
		writeHeader(pageSize, numPages);
		file.seek(headerSize);
		//Writing 0s to the randomaccessfile so that we physically claim the memory required for the storage.
		//first writing for the bitmap
		for(int i=headerSize;i<pageSize;i++){
			file.write((byte) 0);
		}
	}

	public static void writeHeader(int pageSize, int numPages) throws IOException{
		file.seek(0);
		file.writeInt(numWritten);
		file.seek(4);
		file.writeInt(pageSize);
		file.seek(8);
		file.writeInt(numPages);
	}

	public static void LoadStorage(String fileName) throws Exception{
		file= new RandomAccessFile(fileName, "rw");
		fileSize=file.length();
		//Read bytes 4 to 7 which we used to store the number of pages
		file.seek(4);
		numPages= file.readInt();
		//Read the first 4 bytes of the file which we used to store the page size while creating the storage.
		file.seek(0);
		pageSize = file.readInt();
	}

	public static int ReadPage(int bucketNum) throws Exception{
		long offset= headerSize+bucketNum*4;
		file.seek(offset);
		return file.readInt();
	}

	public static void WritePage(int bucketNum) throws Exception{
		file.seek(0);
		int offset = file.readInt();
		file.seek(offset*4);
		file.writeInt(bucketNum);
		file.seek(0);
		file.writeInt(++offset);
	}

	public static int getHashValue(int numToHash) {
		int hashValue = 0;
		hashValue = numToHash % 5;
		return hashValue;
	}
	
	public static void main(String[] args){
		//int[] inputValues = {3,12,45,33,6,89,4,97,118,9,14};
		int[] inputValues = {3,13,41,33,6,85,44,97,118,91,14};
		try {
			CreateStorage("HashStorage",1024,1024);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			LoadStorage("HashStorage");
		} catch (Exception e) {
			e.printStackTrace();
		}
		for(int i=0; i<inputValues.length; i++){
			try {
				WritePage(getHashValue(inputValues[i]));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		for(int i=0; i<inputValues.length; i++){
			try {
				System.out.println(ReadPage(getHashValue(inputValues[i])));;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}


