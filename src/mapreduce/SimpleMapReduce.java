package mapreduce;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

public class SimpleMapReduce {

	final static String FILE_NAME = "output.txt";
	final static Charset ENCODING = StandardCharsets.UTF_8;
	public static Node[] nodes;
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws MessagingException 
	 * @throws AddressException 
	 */
	public static void main(String[] args) throws IOException, AddressException, MessagingException {
		try {
		nodes = new Node[685230];
		
		Util.Init();
		
		} catch (Exception e) {}
		Util.email();
	}
}
