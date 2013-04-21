package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class Util {
	public static long[] blocks = {10328, 20373, 30629, 40645, 50462, 60841, 70591, 80118, 90497, 100501, 110567, 120945, 130999, 140574, 150953, 161332, 171154, 181514, 191625, 202004, 212383, 222762, 232593, 242878, 252938, 263149, 273210, 283473, 293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929, 374236, 384554, 394929, 404712, 414617, 424747, 434707, 444489, 454285, 464398, 474196, 484050, 493968, 503752, 514131, 524510, 534709, 545088, 555467, 565846, 576225, 586604, 596585, 606367, 616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230};

	/**
	 * Get the input and put into the array of nodes
	 */
	public static void Init() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(SimpleMapReduce.FILE_NAME));
		String line;
		while ((line = br.readLine()) != null) {
			long from = Long.parseLong(line.substring(0, 6));
			long to = Long.parseLong(line.substring(7, 13));
			long prob = Long.parseLong(line.substring(15));

			Edge e = new Edge(from, to, prob);
			if (SimpleMapReduce.nodes[(int) from] == null) {
				SimpleMapReduce.nodes[(int) from] = new Node(from);
			} 
			(SimpleMapReduce.nodes[(int) from]).addOutEdge(e);
			if (SimpleMapReduce.nodes[(int) to] == null) {
				SimpleMapReduce.nodes[(int) to] = new Node(to);
			}
			(SimpleMapReduce.nodes[(int) to]).addInEdge(e);
		}      
		br.close();
	}

	/**
	 * @param nodeID
	 * @return
	 */
	long blockIDofNode(long nodeID) {
		int lower = (int) (nodeID / 10328); // Get the most likely block
		if (nodeID <= 10328) { // After this point, the block number needs to decrease lower than our expectation
			lower = 0;
		}
		while (blocks[lower] < nodeID) {
			lower++;
		}
		return lower;
	}

	/**
	 * Send email using GMail SMTP server.
	 *
	 * @param username GMail username
	 * @param password GMail password
	 * @param recipientEmail TO recipient
	 * @param ccEmail CC recipient. Can be empty if there is no CC recipient
	 * @param title title of the message
	 * @param message message to be sent
	 * @throws AddressException if the email address parse failed
	 * @throws MessagingException if the connection is dead or not in the connected state or if the message is not a MimeMessage
	 */
	public static void email() 
			throws AddressException, MessagingException {
		String host = "smtp.gmail.com";
		String from = "cs5300proj2alerts";
		String pass = "largescale";
		Properties props = System.getProperties();
		props.put("mail.smtp.starttls.enable", "true"); // added this line
		props.put("mail.smtp.host", host);
		props.put("mail.smtp.user", from);
		props.put("mail.smtp.password", pass);
		props.put("mail.smtp.port", "587");
		props.put("mail.smtp.auth", "true");

		String[] to = {"cs5300proj2alerts@gmail.com"}; // added this line

		Session session = Session.getDefaultInstance(props, null);
		MimeMessage message = new MimeMessage(session);
		message.setFrom(new InternetAddress(from));

		InternetAddress[] toAddress = new InternetAddress[to.length];

		// To get the array of addresses
		for( int i=0; i < to.length; i++ ) { // changed from a while loop
			toAddress[i] = new InternetAddress(to[i]);
		}
		System.out.println(Message.RecipientType.TO);

		for( int i=0; i < toAddress.length; i++) { // changed from a while loop
			message.addRecipient(Message.RecipientType.TO, toAddress[i]);
		}
		message.setSubject("CS5300 Alert");
		message.setText("");
		Transport transport = session.getTransport("smtp");
		transport.connect(host, from, pass);
		transport.sendMessage(message, message.getAllRecipients());
		transport.close();
	}
}
