package gash.grpc.route.server;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import route.Route;

public class QueryWorker extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("worker");
	private boolean forever = true;
	String storageLocation = Engine.getConf().getProperty("storageLocation");

	public QueryWorker() {
	}

	public void shutdown() {
		logger.info("shutting down worker " + this.getId());
		forever = true;
	}

	private void doWork(Work w) throws IOException, ClassNotFoundException, ParseException {

		if (w == null)
			return;
		try {
				System.out.println("Inside Query worker !!!");
				String payload  = w.request.getPayload().toStringUtf8();
				System.out.println(payload);
				Reader reader = new Reader(payload, storageLocation);
				reader.processData();
				System.out.println("Data has been processed!!");
				String path = "/queryResponse";
				long originId = Engine.getInstance().serverID; 
				long msgId= w.request.getId();
				String filePath = storageLocation + "/queryOutput.csv";
				ArrayList<String> arr = reader.convertCSVtoArray(filePath);
				
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
            	ObjectOutputStream oos = new ObjectOutputStream(baos);
            	oos.writeObject(arr);
            	
            	byte[] payloadResponse = baos.toByteArray();
            	
            	String linkIp = (String) Engine.getConf().getProperty("server.connects.ip");
    			int linkPort = Integer.parseInt(Engine.getConf().getProperty("server.connects.port"));
    			
				Sender sender = new Sender(linkIp, linkPort);
				sender.responseQuery(msgId, originId, Engine.getInstance().leaderServerID, path, ByteString.copyFrom(payloadResponse));
				
				File file = new File(filePath);
				if(file.delete())
				{
					System.out.println("Deleted the combined file!!!");
				}
				
				// simulates latency
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	@Override
	public void run() {
		// TODO not a good idea to spin on work --> wastes CPU cycles
		while (forever) {
			try {
				if (logger.isDebugEnabled())
					logger.debug("run() work qsize = " + Engine.getInstance().queryQueue.size());
				
				var w = Engine.getInstance().queryQueue.poll(2, TimeUnit.SECONDS);
				doWork(w);
			} catch (Exception e) {
				logger.error("worker failure",e);
			}
		}

		// TODO any shutdown steps
	}
}
