import java.io.IOException;
import java.nio.ByteBuffer;

public class Chain {
	private static Storage storage;
	
	public static void main(String[] args){
		//int inputArr[] = {208, 123, 318, 526, 232, 215, 400, 412, 285, 345};
		int[] inputArr = {6, 16, 14};
		//2, 3, 0, 1, 4
		storage = new Storage();
		try {
			storage.CreateStorage("HashStorage", 64, 8192, 4);
			storage.printHeader();
			for(int i=0; i<inputArr.length; i++){
				storage.writeValue(getHash(inputArr[i]), ByteBuffer.allocate(4).putInt(inputArr[i]).array());
			}
			storage.printValInLoc();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static double getLambdaAvg() throws IOException{
		return (double)storage.getN()/(double)(storage.getM()+storage.getSp());
	}
	
	public static int getHash(int num) throws IOException{
		return num%storage.getM();
	}
	
}
