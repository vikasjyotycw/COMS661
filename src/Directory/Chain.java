package Directory;

import java.io.IOException;
import java.nio.ByteBuffer;


public class Chain {
	private static Storage storage;
	
	public static void main(String[] args){
		//int inputArr[] = {208, 123, 318, 526, 232, 215, 400, 412, 285, 345};
		//int[] inputArr = {6, 16, 14, 10, 4, 22, 28, 37, 40, 61, 64, 19, 13, 7, 25, 31, 34, 67, 91, 94, 97, 100, 103, 106, 109, 112, 115, 118, 121, 124, 127};
		//2, 3, 0, 1, 4
		//int[] inputArr = {208, 123, 318, 526, 232, 215, 400, 412, 285, 345};
		int[] inputArr = {208, 123, 318, 526, 232, 215, 400, 412};
		storage = new Storage();
		try {
			//storage.CreateStorage("HashStorage", 64, 8192, 4);
			storage.CreateStorage("HashStorage", 32, 8192, 4);
			//storage.printHeader();
			for(int i=0; i<inputArr.length; i++){
				byte[] buffer = ByteBuffer.allocate(4).putInt(inputArr[i]).array();
				storage.writeValue(getHash(inputArr[i]), buffer);
			}
			storage.printHeader();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static int getHash(int num) throws IOException{
		int M = storage.getM();
		int m = num%M;
		int Sp = storage.getSp();
		if(m<Sp){
			System.out.println(num+" is hashed with 2M = "+(2*M));
			m = num%(2*M);
		}
		return m;
	}
	
}
