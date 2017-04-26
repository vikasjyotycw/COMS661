package Directory;

import Demo.GetTupleFromRelationIterator;
import Tuple.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class Chain {
	private static Storage storage;
	public static void main(String args[]) throws Exception{

		Tuple t = new Tuple();
		GetTupleFromRelationIterator getTupleFromRelationIterator= new GetTupleFromRelationIterator("myDisk1", 35, 0);
		getTupleFromRelationIterator.open();
		while(getTupleFromRelationIterator.hasNext()){
			byte [] tuple = getTupleFromRelationIterator.next();
			byte [] keyPart = t.generateKey(tuple);
			System.out.println(toInt(keyPart,0));
			List<Integer> inputArr = new ArrayList<Integer>();
			inputArr.add(toInt(keyPart,0));
			storage = new Storage();
			try {
				storage.CreateStorage("HashStorage", 32, 8192, 4);
				for(int i=0; i<inputArr.size(); i++){
					byte[] buffer = ByteBuffer.allocate(4).putInt(inputArr.get(i)).array();
					storage.writeValue(getHash(inputArr.get(i)), buffer);
				}
				storage.printHeader();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
	private static int toInt(byte[] bytes, int offset) {
		int ret = 0;
		for (int i=0; i<4; i++) {
			ret <<= 8;
			ret |= (int)bytes[offset+i] & 0xFF;
		}
		return ret;
	}
}