package Directory;

import java.io.IOException;
import java.io.RandomAccessFile;

public class SimpleHashStorage {
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
		file.writeInt(pageSize);
		file.seek(4);
		file.writeInt(numPages);
	}

	public static void LoadStorage(String fileName) throws Exception{
		file= new RandomAccessFile(fileName, "rw");
		fileSize=file.length();
		//Read bytes 4 to 7 which we used to store the number of pages
		
	}

	public static int ReadPage(int bucketNum) throws Exception{
		file.seek(8+(bucketNum)*4);
		return file.readInt();
	}

	public static void WritePage(int value, int bucketNum) throws Exception{
		file.seek(8+(bucketNum)*4);
		file.writeInt(value);
	}

	public static int getHashValue(int numToHash) {
		int hashValue = 0;
		hashValue = numToHash % 5;
		return hashValue;
	}
	
	public static String readHeader() throws IOException{
		String header = "";
		file.seek(0);
		int pageSize = file.readInt();
		file.seek(4);
		int numPages = file.readInt();
		header = Integer.toString(pageSize) + " - " + Integer.toString(numPages);
		return header;
	}
	
	public static void main(String[] args){
		//int[] inputValues = {3,12,45,33,6,89,4,97,118,9,14};
		int[] inputValues = {3,11,4,55,32};
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
		try {
			/*
			WritePage(6,getHashValue(6));
			System.out.println(ReadPage(1));
			System.out.println(readHeader());
			*/
			for(int i=0; i<inputValues.length; i++){
				WritePage(inputValues[i],getHashValue(inputValues[i]));
			}
			for(int i=0; i<inputValues.length; i++){
				System.out.println(ReadPage(getHashValue(inputValues[i])));
			}
			System.out.println("-----");
			for(int i=0; i<28; i=i+4){
				file.seek(i);
				System.out.println(file.readInt());
			}
			System.out.println("-----");
			System.out.println(readHeader());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

/*
 2
4
1
2
4
3
3
4
2
3
3
 
 */
