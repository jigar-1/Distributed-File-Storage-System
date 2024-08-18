package gash.grpc.route.server;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import route.Route;

public class Worker extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("worker");
	private boolean forever = true;
	String storageLocation = Engine.getConf().getProperty("storageLocation");

	public Worker() {
	}

	public void shutdown() {
		logger.info("shutting down worker " + this.getId());
		forever = true;
	}

	private void doWork(Work w) throws IOException, ClassNotFoundException, ParseException {

		if (w == null)
			return;

		
			try {
				
//				logger.info("*** doWork() " + w + " ***");
//				byte[] payload  = w.request.getPayload().toByteArray();
//				String s = new String(payload, StandardCharsets.UTF_8);
//				InputStream dataInputStream = new ByteArrayInputStream(payload); 
//				logger.info("Incoming request data" + s);
				byte[] payload  = w.request.getPayload().toByteArray();
				ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            	ObjectInputStream ois = new ObjectInputStream(bais);
            	ArrayList<String> arrayList = ( ArrayList<String>) ois.readObject();
            	
				Writer writer = new Writer(arrayList, storageLocation);
				writer.processData();
				// simulates latency
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
//		else {
//			// forward the message to Link instances
//			// for (var link : Engine.getInstance().links) {
//			// 	if (link.getServerID() == w.request.getDestination()) {
//					
//					String linkIp = (String) Engine.getConf().getProperty("server.connects.ip");
//					int linkPort = Integer.parseInt(Engine.getConf().getProperty("server.connects.port"));
//
//					Sender sender = new Sender(linkIp, linkPort);
//					Route request = w.request;
//					
//					sender.sendRequest(request.getOrigin(), request.getDestination(), request.getId(), request.getPath(), request.getPayload());
//					
//					System.out.println("Request forwaded to: " + request.getDestination());
//					logger.info("TODO: send message");
//				// }
//			// }
//
//			// if no direct destination exists, forward to all
//			// links or the next link?
//		}
	}

	@Override
	public void run() {
		// TODO not a good idea to spin on work --> wastes CPU cycles
		while (forever) {
			try {
				if (logger.isDebugEnabled())
					logger.debug("run() work qsize = " + Engine.getInstance().workQueue.size());
				
				var w = Engine.getInstance().workQueue.poll(2, TimeUnit.SECONDS);
				doWork(w);
			} catch (Exception e) {
				logger.error("worker failure",e);
			}
		}

		// TODO any shutdown steps
	}
}
