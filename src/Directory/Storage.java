
import java.io.IOException;
import java.io.RandomAccessFile;

public class Storage{
	private String fileName;
	private long fileSize;
	public RandomAccessFile file;
	public int pageSize;
	public int tupleSize;
	public int M;
	private int Mstart;
	public int N;
	public int Sp;
	public final double lowerLim = 0.5;
	public final double upperLim = 1.5;
	public int m;
	private int bitMapSize;
	public int numPages;
	private int maxTuplesInPage;

	private int numAllocated;
	private int numDeallocated;
	private int numRead;
	private int numWritten;

	public void CreateStorage(String fileName,int pageSize, int fileSize, int tupleSize) throws Exception{
		this.M = 3;
		this.Mstart = this.M;
		this.N = M;
		this.Sp = 0;
		this.fileName=fileName;
		this.fileSize= fileSize;
		this.pageSize=pageSize;
		this.tupleSize = tupleSize;
		this.numPages=(int) (this.fileSize/this.pageSize);
		this.maxTuplesInPage = (this.pageSize-24)/tupleSize;
		System.out.println("maxTuplesInPage:"+maxTuplesInPage+" numPages:"+numPages);

		this.bitMapSize =(int) Math.ceil(this.numPages/8.0);

		if(this.bitMapSize%16!=0){
			this.bitMapSize = (this.bitMapSize/16+1)*16;
		}
		//Allocating 28 extra bytes in the beginning for storage of parameters such as pagesize.
		this.bitMapSize=this.bitMapSize+28;
		this.file= new RandomAccessFile(this.fileName, "rw");
		System.out.println("BitMapSize:"+this.bitMapSize);
		//System.out.println(this.pageSize+" "+this.numPages+" "+this.tupleSize+" "+this.M+" "+this.N+" "+this.Sp);
		file.seek(0);
		//Write the pagesize to the first 4 bytes in the file.
		file.writeInt(this.pageSize);

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
		
		//Write current max chain number to the next 4 bytes
		file.seek(24);
		file.writeInt(this.M-1);

		file.seek(0);

		this.fileSize=this.fileSize+this.bitMapSize;
		file.setLength(fileSize);
		file.seek(28);
		//Writing 0s to the randomaccessfile so that we physically claim the memory required for the storage.
		//first writing for the bitmap
		for(int i=28;i<this.bitMapSize;i++){
			this.file.write((byte) 0);
		}
		//Writing the file contents will 0s
		for(int i=this.bitMapSize;i<this.fileSize;i++){
			file.write((byte) 0);
		}

		for(int i=0; i<M; i++){
			//long startPos = this.bitMapSize+i*this.pageSize;
			int pageNum = AllocatePage();
			int startPos = this.bitMapSize+(pageNum*this.pageSize);
			//System.out.println("Bucket#"+i+" starts at "+startPos);
			file.seek(startPos);
			file.writeInt(i);
			file.seek(startPos+4);
			file.writeInt(0);
			file.seek(startPos+8);
			file.writeInt(0);
			file.seek(startPos+12);
			file.writeInt(0);
			file.seek(startPos+16);
			file.writeInt(M);
			file.seek(startPos+20);
			if(i<M-1){
				file.writeInt(this.bitMapSize+((pageNum+1)*this.pageSize));
			}else{
				file.writeInt(0);
			}
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


	public int AllocatePage() throws Exception{
		file.seek(28);
		//We use bits to keep track of allocated pages. The RandomAccessFile supports only byte operations.
		//Thus, to allocate, we pick up bytes from the RandomAccessFile and then look in the bits in the byte to see 
		//if any of them is 0 or not.
		for(int i=28;i<this.bitMapSize;i++){
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
						if((i-28)*8+j<this.numPages){
							return ((i-28)*8+j);
						} else {
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
	public void DeAllocatePage(int n) throws Exception{
		file.seek(n/8);
		int byteRead= file.read();
		int byteToBeWritten = WriteBitInAByte((int) (n%8), byteRead, 0);
		file.seek(n/8);
		file.write(byteToBeWritten);
		numDeallocated++;
		
		int pageLoc = this.bitMapSize+(n*this.pageSize);
		for(int i=pageLoc; i<pageLoc+this.pageSize; i++){
			file.seek(i);
			file.write((byte)0);
		}
		System.out.println(pageLoc+" to "+(pageLoc+this.pageSize-1)+" filled with 0");
	}

	public void printStats(){
		System.out.println("Number of pages Read:"+numRead + " "+ "; Written:"+numWritten+" "+"; Allocated: "+numAllocated+" "+"; Deallocated: "+numDeallocated);
	}

	public void writeValue(int m, byte[] buffer) throws Exception{
		System.out.println("################# INSERTING "+java.nio.ByteBuffer.wrap(buffer).getInt()+" #################");
		//Going to the start of the chain
		int offset= getChainAddress(m);
		System.out.println("Start address of chain: "+offset);
		//Reading the nextPtr value
		file.seek(offset+4);
		int currPtr = offset;
		int nextPtr = file.readInt();
		System.out.println("Next pointer value in 1st page: "+nextPtr);
		//Moving to the last page of the chain
		while(nextPtr!=0){
			System.out.println(currPtr+","+nextPtr);
			file.seek(nextPtr+4);
			currPtr = nextPtr;
			nextPtr = file.readInt();
			System.out.println("Page at address "+currPtr+" is full so moving to next page");
			System.out.println(currPtr+","+nextPtr);
		}
		//If the page does not have space, allocate a new page
		file.seek(currPtr+8);
		int tuplesInPage = file.readInt();
		System.out.println("There are "+tuplesInPage+" tuples in this page. Max tuples allowed here is "+maxTuplesInPage);
		if(tuplesInPage==maxTuplesInPage){
			int pageNum = AllocatePage();
			nextPtr = this.bitMapSize+(pageNum*this.pageSize);
			System.out.println("New page allocated with address "+nextPtr+". Control is still at address "+currPtr);
			file.seek(currPtr+4);
			file.writeInt(nextPtr);
			file.seek(currPtr+4);
			System.out.println("Written nextPtr="+file.readInt()+"at address "+(currPtr+4));
			configureNewPage(nextPtr, currPtr, m, M, false);
			currPtr = nextPtr;
			nextPtr = 0;
			tuplesInPage = 0;
		}
		//Write value and update tuples-in-page count
		System.out.println("Value will be written at location "+(currPtr+24+tuplesInPage*tupleSize));
		file.seek(currPtr+24+tuplesInPage*tupleSize);
		file.write(buffer);
		file.seek(currPtr+8);
		System.out.println("Tuples in page count updated at location "+(currPtr+8));
		file.writeInt(++tuplesInPage);
		System.out.println("Value written and now this page has "+tuplesInPage+" tuples");
		double lambda = getLambdaAvg();
		//int z=0;
		while(lambda>upperLim){
			System.out.println("We need to split chain "+getSp());
			splitChain();
			//if(++z==10) System.exit(0);
			lambda = getLambdaAvg();
		}
	}
	
	public void writeValueInSplit(int m, byte[] buffer, boolean internalCall) throws Exception{
		System.out.println("################# INSERTING "+java.nio.ByteBuffer.wrap(buffer).getInt()+" DURING SPLIT #################");
		//Going to the start of the chain
		int offset=0;
		if(internalCall){
			offset=m;
		} else {
			offset= getChainAddress(m);
		}
		System.out.println("Start address of chain: "+offset);
		//Reading the nextPtr value
		file.seek(offset+4);
		int currPtr = offset;
		int nextPtr = file.readInt();
		System.out.println("Next pointer value in 1st page: "+nextPtr);
		//Moving to the last page of the chain
		while(nextPtr!=0){
			System.out.println(currPtr+","+nextPtr);
			file.seek(nextPtr+4);
			currPtr = nextPtr;
			nextPtr = file.readInt();
			System.out.println("Page at address "+currPtr+" is full so moving to next page");
			System.out.println(currPtr+","+nextPtr);
		}
		//If the page does not have space, allocate a new page
		file.seek(currPtr+8);
		int tuplesInPage = file.readInt();
		System.out.println("There are "+tuplesInPage+" tuples in this page. Max tuples allowed here is "+maxTuplesInPage);
		if(tuplesInPage==maxTuplesInPage){
			int pageNum = AllocatePage();
			nextPtr = this.bitMapSize+(pageNum*this.pageSize);
			System.out.println("New page allocated with address "+nextPtr+". Control is still at address "+currPtr);
			file.seek(currPtr+4);
			file.writeInt(nextPtr);
			file.seek(currPtr+4);
			System.out.println("Written nextPtr="+file.readInt()+"at address "+(currPtr+4));
			configureNewPage(nextPtr, currPtr, m, M, false);
			currPtr = nextPtr;
			nextPtr = 0;
			tuplesInPage = 0;
		}
		//Write value and update tuples-in-page count
		System.out.println("Value will be written at location "+(currPtr+24+tuplesInPage*tupleSize));
		file.seek(currPtr+24+tuplesInPage*tupleSize);
		file.write(buffer);
		file.seek(currPtr+8);
		System.out.println("Tuples in page count updated at location "+(currPtr+8));
		file.writeInt(++tuplesInPage);
		System.out.println("Value written and now this page has "+tuplesInPage+" tuples");
	}
	
	public void configureNewPage(int newPageLoc, int prevPageLoc, int chainNum, int hashedWith, boolean newChainFlg) throws IOException{
		System.out.println("Configuring new page staring at "+newPageLoc);
		file.seek(newPageLoc);
		file.writeInt(chainNum);
		file.seek(newPageLoc+4);
		file.writeInt(0);
		file.seek(newPageLoc+8);
		file.writeInt(0);
		file.seek(newPageLoc+12);
		file.writeInt(prevPageLoc);
		file.seek(newPageLoc+16);
		file.writeInt(hashedWith);
		if(newChainFlg){
			file.seek(newPageLoc+20);
			file.writeInt(0);
			file.seek(getChainAddress(chainNum-1)+20);
			file.writeInt(newPageLoc);
			System.out.println("Writing 0 at "+(newPageLoc+20)+" and "+(newPageLoc)+" at "+(getChainAddress(chainNum-1)+20));
		}else{
			file.seek(prevPageLoc+20);
			int nextChainPtr = file.readInt();
			file.seek(newPageLoc+20);
			file.writeInt(nextChainPtr);
		}
		int N = getN();
		setN(++N);
	}
	
	public void splitChain() throws Exception{
		int flg = 0;
		int Sp = getSp();
		int M = getM();
		int startOfPage = getChainAddress(Sp);
		do{
			System.out.println("Splitting chain - now at page starting at "+startOfPage);
			file.seek(startOfPage+8);
			int tuplesInPage = file.readInt();
			if(tuplesInPage==0) break;
			for(int i=0; i<tuplesInPage; i++){
				int posOfTuple = startOfPage+24+i*tupleSize;
				file.seek(posOfTuple);
				int newChainNum = file.readInt()%(2*M);
				file.seek(startOfPage);
				int currChainNum = file.readInt();
				if(newChainNum==currChainNum){
					continue;
				} else {
					boolean internalCallFlg = false;
					int newPageLoc = 0;
					if(newChainNum>getCurrentChainCount()){
						int pageNum = AllocatePage();
						newPageLoc = this.bitMapSize+(pageNum*this.pageSize);
						configureNewPage(newPageLoc, 0, newChainNum, 2*M, true);
						setCurrentChainCount(getCurrentChainCount()+1);
						if(getCurrentChainCount()==this.Mstart){
							file.seek(getChainAddress(this.Mstart-1)+20);
							file.writeInt(newPageLoc);
						}
						internalCallFlg = true;
						System.out.println("Added new chain "+newChainNum+" at address "+newPageLoc);
					} 
					byte[] buffer = new byte[tupleSize];
					file.seek(posOfTuple);
					file.read(buffer);
					flg = removeTupleFromPage(startOfPage, posOfTuple, tuplesInPage);
					i--;
					tuplesInPage--;
					if(internalCallFlg)
						writeValueInSplit(newPageLoc, buffer, internalCallFlg);
					else
						writeValueInSplit(newChainNum, buffer, internalCallFlg);
				}
			}
			file.seek(startOfPage+16);
			file.writeInt(2*M);
			if(flg==0){
				file.seek(startOfPage+4);
				startOfPage = file.readInt();
			} else if (flg==-1){
				continue;
			} else {
				startOfPage = flg;
			}
		} while (startOfPage!=0);
		if(Sp==M-1){
			setSp(0);
			setM(2*M);
			System.out.println("Doubled M");
		} else {
			setSp(++Sp);
			System.out.println("Incremented Sp to "+getSp());
		}
		System.out.println("Exiting splitChain");
	}
	
	public int removeTupleFromPage(int pageLoc, int posOfTuple, int tuplesInPage) throws Exception{
		System.out.println("Need to delete tuple from pos "+posOfTuple+" in page starting at "+pageLoc+" which has "+tuplesInPage+" tuples");
		for(int i=posOfTuple; i<posOfTuple+tupleSize; i++){
			file.seek(i);
			file.write((byte)0);
			System.out.println("Deleted from pos "+i);
		}
		int tupleNum = (posOfTuple-pageLoc-24)/tupleSize;
		for(int i=tupleNum; i<tuplesInPage-1; i++) {
			byte[] buffer = new byte[tupleSize];
			file.seek(pageLoc+24+(i+1)*tupleSize);
			file.read(buffer);
			file.seek(pageLoc+24+i*tupleSize);
			file.write(buffer);
			System.out.println("While deleting tuple# "+tupleNum+" at "+posOfTuple+" moving tuple from "+(pageLoc+24+(i+1)*tupleSize)+" to "+(pageLoc+24+i*tupleSize));
		}
		file.seek(pageLoc+8);
		file.writeInt(--tuplesInPage);
		System.out.println("This page now has "+tuplesInPage+" tuples");
		if(tuplesInPage==0){
			int currentN = getN();
			file.seek(pageLoc+4);
			int nextPtr = file.readInt();
			file.seek(pageLoc+12);
			int prevPtr = file.readInt();
			if(prevPtr==0 && nextPtr==0){
				return 0;
			} else if(prevPtr!=0 && nextPtr==0){
				DeAllocatePage((pageLoc-this.bitMapSize)/this.pageSize);
				file.seek(prevPtr+4);
				file.writeInt(0);
				setN(--currentN);
				System.out.println("Deallocating page at "+pageLoc);
			} else if(prevPtr==0 && nextPtr!=0){
				int srcPage = nextPtr;
				int destPage = pageLoc;
				copyPageContents(srcPage, destPage);
				DeAllocatePage((nextPtr-this.bitMapSize)/this.pageSize);
				file.seek(pageLoc+12);
				//DeAllocatePage((pageLoc-this.bitMapSize)/this.pageSize);
				//file.seek(nextPtr+12);
				file.writeInt(0);
				setN(--currentN);
				//System.out.println("Deallocating page at "+pageLoc);
				System.out.println("Deallocating page at "+nextPtr);
				return -1;
			} else {
				DeAllocatePage((pageLoc-this.bitMapSize)/this.pageSize);
				file.seek(prevPtr+4);
				file.writeInt(nextPtr);
				file.seek(nextPtr+12);
				file.writeInt(prevPtr);
				setN(--currentN);
				System.out.println("Deallocating page at "+pageLoc);
				return nextPtr;
			}
		}
		return 0;
	}
	
	public void copyPageContents(int srcPage, int destPage) throws IOException{
		file.seek(srcPage);
		int m = file.readInt();
		file.seek(destPage);
		file.writeInt(m);
		file.seek(srcPage+4);
		int nextPtr = file.readInt();
		file.seek(destPage+4);
		file.writeInt(nextPtr);
		file.seek(srcPage+8);
		int tuplesInPage = file.readInt();
		file.seek(destPage+8);
		file.writeInt(tuplesInPage);
		file.seek(srcPage+12);
		int prevPtr = file.readInt();
		file.seek(destPage+12);
		file.writeInt(prevPtr);
		file.seek(srcPage+16);
		int h = file.readInt();
		file.seek(destPage+16);
		file.writeInt(h);
		file.seek(srcPage+20);
		int nextChain = file.readInt();
		file.seek(destPage+20);
		file.writeInt(nextChain);
		for(int i=0; i<tuplesInPage; i++){
			byte[] buffer = new byte[tupleSize];
			file.seek(srcPage+24+i*tupleSize);
			file.read(buffer);
			file.seek(destPage+24+i*tupleSize);
			file.write(buffer);
		}
	}
	
	public double getLambdaAvg() throws IOException{
		int N = getN();
		int M = getM();
		int Sp = getSp();
		System.out.println("M:"+M+" N:"+N+" Sp:"+Sp);
		double lambda = (double)N/(double)(M+Sp);
		System.out.println("Lambda is "+lambda);
		return Math.round (lambda * 100.0) / 100.0;
	}
	
	public int getChainAddress(int m) throws IOException{
		int chainAddress = 0;
		if(m<this.Mstart){
			chainAddress = m*this.pageSize+this.bitMapSize;
		}else{
			chainAddress = this.bitMapSize;
			for(int i=0; i<m; i++){
				file.seek(chainAddress+20);
				int x = chainAddress+20;
				chainAddress = file.readInt();
				System.out.println("While finding next chain address of chain#"+i+" at "+x+" we found value "+chainAddress);
			}
		}
		System.out.println("Address of chain "+m+" as computed by the method is "+chainAddress);
		return chainAddress;
	}
	
	public byte[] searchValue(int val) throws IOException{
		byte[] buffer = new byte[this.pageSize];
		int pageAccessCount = 0;
		int m = val%this.M;
		int chainAddress = getChainAddress(m);
		file.seek(chainAddress+16);
		int hashedWith = file.readInt();
		pageAccessCount++;
		if(this.M==hashedWith){
			file.seek(chainAddress);
			file.read(buffer);
			System.out.println("Number of pages accessed - "+pageAccessCount);
			return buffer;
		}
		m = val%hashedWith;
		chainAddress = getChainAddress(m);
		file.seek(chainAddress);
		file.read(buffer);
		pageAccessCount++;
		return buffer;
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

	public int getN() throws IOException {
		file.seek(16);
		return file.readInt();
	}

	public void setN(int n) throws IOException {
		file.seek(16);
		file.writeInt(n);
	}

	public int getSp() throws IOException {
		file.seek(20);
		return file.readInt();
	}

	public void setSp(int sp) throws IOException {
		file.seek(20);
		file.writeInt(sp);
	}
	
	public int getCurrentChainCount() throws IOException{
		file.seek(24);
		return file.readInt();
	}
	
	public void setCurrentChainCount(int n) throws IOException{
		file.seek(24);
		file.writeInt(n);
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

	public void printHeader(){
		try{
			System.out.println("\n***********************************");
			System.out.println("pagesize "+getPageSize());
			System.out.println("number of pages "+getNumPages());
			System.out.println("tuple size "+getTupleSize());
			System.out.println("M "+getM());
			System.out.println("N "+getN());
			System.out.println("Sp "+getSp());
			System.out.println("Num of chains "+(getCurrentChainCount()+1));
			System.out.println("***********************************");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
