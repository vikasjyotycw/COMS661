
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class Storage{
	private String fileName;
	private long fileSize;
	public RandomAccessFile file;
	public int pageSize;
	public int tupleSize;
	public int M;
	public int N;
	public int Sp;
	public final double lowerLim = 1.5;
	public final double upperLim = 2.0;
	public int m;
	public long nxtPtr;
	public long pos;
	private int bitMapSize;
	public int numPages;
	
	private int numAllocated;
	private int numDeallocated;
	private int numRead;
	private int numWritten;
	
	public void CreateStorage(String fileName,int pageSize, int fileSize, int tupleSize) throws Exception{
		this.M = 3;
		this.N = 0;
		this.Sp = 0;
		this.fileName=fileName;
		this.fileSize= fileSize;
		this.pageSize=pageSize;
		this.tupleSize = tupleSize;
		this.numPages=(int) (this.fileSize/this.pageSize);
		
		this.bitMapSize =(int) Math.ceil(this.numPages/8.0);

		if(this.bitMapSize%16!=0){
			this.bitMapSize = (this.bitMapSize/16+1)*16;
		}
		//Allocating 16 extra bytes in the beginning for storage of parameters such as pagesize.
		this.bitMapSize=this.bitMapSize+16;
		this.file= new RandomAccessFile(this.fileName, "rw");
		
		file.seek(0);
		//Write the pagesize to the first 4 bytes in the file.
		file.writeInt(pageSize);
		
		//Write number of pages to the next 4 bytes in the file
		file.seek(4);
		file.writeInt(this.numPages);
		
		//Write tuple size to the next 4 bytes in the file
		file.seek(8);
		file.writeInt(this.tupleSize);
		
		//Write M to the next 4 bytes in the file
		file.seek(12);
		file.writeInt(this.M);
		
		//Write N to the next 4 bytes in the file
		file.seek(16);
		file.writeInt(this.N);
		
		//Write Sp to the next 4 bytes in the file
		file.seek(20);
		file.writeInt(this.Sp);
		
		file.seek(0);
		
		this.fileSize=this.fileSize+this.bitMapSize;
		file.setLength(fileSize);
		file.seek(16);
		//Writing 0s to the randomaccessfile so that we physically claim the memory required for the storage.
		//first writing for the bitmap
		for(int i=16;i<this.bitMapSize;i++){
			this.file.write((byte) 0);
		}
		//Writing the file contents will 0s
		for(int i=this.bitMapSize;i<this.fileSize;i++){
			file.write((byte) 0);
		}
		
		for(int i=0; i<M; i++){
			long startPos = this.bitMapSize+i*this.pageSize;
			file.seek(startPos);
			file.write(i);
			file.seek(startPos+4);
			file.write(0);
			file.seek(startPos+8);
			file.write(0);
		}
		
	}
	
	public void LoadStorage(String fileName) throws Exception{
		this.file= new RandomAccessFile(fileName, "rw");
		
		this.fileSize=file.length();
		
		//Read bytes 4 to 7 which we used to store the number of pages
		file.seek(4);
		this.numPages= file.readInt();
		this.fileName=fileName;
		
		//Read the first 4 bytes of the file which we used to store the page size while creating the storage.
		file.seek(0);
		this.pageSize = file.readInt();
		
		
		this.bitMapSize =(int) Math.ceil(this.numPages/8.0);

		if(this.bitMapSize%16!=0){
			this.bitMapSize = (this.bitMapSize/16+1)*16;
		}
		this.bitMapSize=this.bitMapSize+16;
		
		
		this.numAllocated=0;
		this.numDeallocated=0;
		this.numRead=0;
		this.numWritten=0;
	}
	
	public void UnloadStorage() {
		this.file=null;
	}
	
	
	public void ReadPage(long n, byte [] buffer) throws Exception{
		//Go to the offset.
		long offset= n*this.pageSize+this.bitMapSize;
		file.seek(offset);
		
		//read the page in buffer.
		file.read(buffer);
		this.numRead++;
	}
	
	
	public void WritePage(long n, byte[] buffer) throws Exception{
		//Go to the required offset
		long offset= n*this.pageSize+this.bitMapSize;
		file.seek(offset);
		
		//Write the buffer to the file.
		file.write(buffer);
		this.numWritten++;
	}
	
	//This function changes a bit in a byte and returns the int value of the new byte.
	private int WriteBitInAByte(int offset, int byteRead, int bitToBeWritten){
		String binaryString= String.format("%8s", Integer.toBinaryString(byteRead & 0xFF)).replace(' ', '0');
		binaryString = binaryString.substring(0,offset)+bitToBeWritten+binaryString.substring(offset+1);
		int byteWrite= Integer.parseInt(binaryString,2);
		return byteWrite;
	}
	
	
	public long AllocatePage() throws Exception{
		file.seek(16);
		//We use bits to keep track of allocated pages. The RandomAccessFile supports only byte operations.
		//Thus, to allocate, we pick up bytes from the RandomAccessFile and then look in the bits in the byte to see 
		//if any of them is 0 or not.
		for(long i=16;i<this.bitMapSize;i++){
			int byteread;
			byteread=file.read();
			//If the byte which is read has all 1's, then all the pages are allocated. Don't look in that byte.
			if(byteread<255){
				file.seek(i);
				
				//Convert the byte into a binary string.
				String binaryString= String.format("%8s", Integer.toBinaryString(byteread & 0xFF)).replace(' ', '0');
				//Look in the string to find the first 0 bit and set it to 1. Return that page number
				for(int j=0;j<8;j++){
					if(binaryString.charAt(j)=='0'){
						binaryString=binaryString.substring(0,j)+"1"+binaryString.substring(j+1);
						file.write(Integer.parseInt(binaryString,2));
						numAllocated++;
						
						//Return the page number only if the number of pages is more than the page we are returning.
						if((i-16)*8+j<this.numPages)
							return ((i-16)*8+j);
						else {

							System.out.println("Error in allocating a page");
							return -1;
						}
						
					}
				}
			}
		}
		System.out.println("Error in allocating a page");
		return -1;
	}
	
	
	//To deallocate a page n, we pick up the n/8th byte from the RandomAccessFile and then change the corresponding bit in that byte to 0.
	public void DeAllocatePage(long n) throws Exception{
		file.seek(n/8);
		int byteRead= file.read();
		int byteToBeWritten = WriteBitInAByte((int) (n%8), byteRead, 0);
		file.seek(n/8);
		file.write(byteToBeWritten);
		numDeallocated++;
	}
	
	public void printStats(){
		System.out.println("Number of pages Read:"+numRead + " "+ "; Written:"+numWritten+" "+"; Allocated: "+numAllocated+" "+"; Deallocated: "+numDeallocated);
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getTupleSize() {
		return tupleSize;
	}

	public void setTupleSize(int tupleSize) {
		this.tupleSize = tupleSize;
	}

	public int getM() throws IOException {
		file.seek(12);
		return file.readInt();
	}

	public void setM(int M) throws IOException {
		file.seek(12);
		file.writeInt(M);
	}

	public int getN() {
		return N;
	}

	public void setN(int n) {
		N = n;
	}

	public int getSp() {
		return Sp;
	}

	public void setSp(int sp) {
		Sp = sp;
	}

	public long getNxtPtr() {
		return nxtPtr;
	}

	public void setNxtPtr(long nxtPtr) {
		this.nxtPtr = nxtPtr;
	}

	public long getPos() {
		return pos;
	}

	public void setPos(long pos) {
		this.pos = pos;
	}

	public int getNumPages() {
		return numPages;
	}

	public void setNumPages(int numPages) {
		this.numPages = numPages;
	}

	public int getNumAllocated() {
		return numAllocated;
	}

	public void setNumAllocated(int numAllocated) {
		this.numAllocated = numAllocated;
	}

	public int getNumDeallocated() {
		return numDeallocated;
	}

	public void setNumDeallocated(int numDeallocated) {
		this.numDeallocated = numDeallocated;
	}

	public int getNumRead() {
		return numRead;
	}

	public void setNumRead(int numRead) {
		this.numRead = numRead;
	}

	public int getNumWritten() {
		return numWritten;
	}

	public void setNumWritten(int numWritten) {
		this.numWritten = numWritten;
	}

	public double getLowerLim() {
		return lowerLim;
	}

	public double getUpperLim() {
		return upperLim;
	}
	
	public void writeValue(int m, byte[] buffer) throws IOException{
		//byte[] buffer = ByteBuffer.allocate(4).putInt(m).array();
		long offset= m*this.pageSize+this.bitMapSize+12;
		file.seek(pos);
		file.write(buffer);
	}
}
