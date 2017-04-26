/*
 * Created by Rakesh Kumar Shah and Vikas Jyoti
 * 
 * 
 */


package Demo;


import PTCFramework.ConsumerIterator;
import PTCFramework.PTCFramework;
import PTCFramework.ProducerIterator;

public class RelationToRelationPTC1 extends PTCFramework<byte [], byte []> {

	public RelationToRelationPTC1(ProducerIterator<byte []> pIterator, ConsumerIterator<byte []> cIterator) {
		super(pIterator, cIterator);
	}
	
	public void run(){
		try{
			this.producerIterator.open();
			this.consumerIterator.open();
			while(this.producerIterator.hasNext())
			{
				byte [] bytes= new byte[31];
				byte [] producerElement= producerIterator.next();
				
				if(toInt(producerElement,31) >= 50000)
				{
					for(int i = 0; i < 27; i++)
						bytes[i]=producerElement[i];
					for(int i=27;i<31;i++){
						bytes[i]=producerElement[i+4];
					}
				//Send the transformed tuple to the Consumer Iterator
				consumerIterator.next(bytes);
				}
			}
			this.producerIterator.close();
			this.consumerIterator.close();
		} catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	

	
	private static int toInt(byte[] bytes, int offset) 
	{
		  int ret = 0;
		  for (int i=0; i<4; i++) 
		  {
		    ret <<= 8;
		    ret |= (int)bytes[offset+i] & 0xFF;
		  }
		  return ret;
	}
}