package gash.grpc.route.server;
import org.apache.zookeeper.*;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import route.Route;
import route.RouteServiceGrpc;
import route.RouteServiceGrpc.RouteServiceImplBase;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

public class MainServer extends RouteServiceImplBase implements Watcher  {

	private Server svr;
	
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 5000;
    private static final int CHUNK_SIZE = 5000;
    private static final long MAIN_SERVER_ID = 8000;
    private ZooKeeper zooKeeper;
    private int counter = 0;
    private static int totalMasterServer = 0;
    private static List<String> serverPaths = new ArrayList<String>();
    
    public static void main(String[] args) throws Exception {
    	MainServer mainServer = new MainServer();
    	try
    	{
    		
	    	mainServer.connectToZooKeeper();

	        // Add the address of all the master servers
	        
	        serverPaths.add(mainServer.getServerAddress("/masterServer/1"));;
	        serverPaths.add(mainServer.getServerAddress("/masterServer/2"));
	        serverPaths.add(mainServer.getServerAddress("/masterServer/3"));
	        
	        totalMasterServer = serverPaths.size();
	        final MainServer impl = new MainServer();
			impl.start();
			impl.blockUntilShutdown();
			
    	}
    	// Close the connection to ZooKeeper
    	catch(InterruptedException ie) {
    		try {
				mainServer.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
        
        
    }
    
    private static final Route constructMessageByte(int mID, long toID, String path, byte[] payload) {
    	Route.Builder bld = Route.newBuilder();
    	bld.setId(mID);
    	bld.setDestination(toID);
    	bld.setOrigin(MAIN_SERVER_ID);
    	bld.setPath(path);
    	bld.setPayload(ByteString.copyFrom(payload));

    	return bld.build();
    }
    private void start() throws Exception {
		svr = ServerBuilder.forPort(Engine.getInstance().getServerPort()).addService(new RouteServerImpl()).build();

		Engine.logger.info("starting server");
		svr.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				MainServer.this.stop();
			}
		});
	}

	protected void stop() {
		svr.shutdown();
	}
	
	private void blockUntilShutdown() throws Exception {
		/* TODO what clean up is required? */
		svr.awaitTermination();

	}

    private void connectToZooKeeper() throws Exception {
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);

        // Wait until the connection is established
        while (zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
            Thread.sleep(100);
        }

        System.out.println("Connected to ZooKeeper!");
    }

    private String getServerAddress(String serverPath) throws KeeperException, InterruptedException {
        byte[] serverAddressBytes = zooKeeper.getData(serverPath, false, null);
        String serverAddress = new String(serverAddressBytes);

        System.out.println("Server address of target server: " + serverAddress);

        return serverAddress;
    }

    private void close() throws InterruptedException {
        zooKeeper.close();
        System.out.println("Connection to ZooKeeper closed.");
    }

    @Override
    public void process(WatchedEvent event) {
        // Handle ZooKeeper events, if required
    }
    
    
    @Override
	public void query(route.Route request, StreamObserver<route.Route> responseObserver) {
    	
    	for(String s: serverPaths)
    	{
    		String[] serverPath = s.split(":");
    		ManagedChannel ch = ManagedChannelBuilder.forAddress(serverPath[0], Integer.parseInt(serverPath[1])).usePlaintext().build();
    		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
    		
    		Route.Builder bld = Route.newBuilder();
    		bld.setId(request.getId());
    		bld.setDestination(counter%totalMasterServer);
    		bld.setOrigin(MAIN_SERVER_ID);
    		bld.setPath("/query");
    		bld.setPayload(request.getPayload());

    		var r = stub.query(bld.build());
    		
    		while(r.hasNext())
    		{
    			responseObserver.onNext(r.next());
    		}
    		ch.shutdown();
    		
    	}
    	System.out.println("Sent all the data back to client");
    	responseObserver.onCompleted();
    }
    
    @Override
	public void request(route.Route request, StreamObserver<route.Route> responseObserver) {
    	
		try {
	         
			ByteArrayInputStream bais = new ByteArrayInputStream(request.getPayload().toByteArray());
	    	ObjectInputStream ois;
	    	ArrayList<String> arrayList = new ArrayList<>();
	    	
			try {
				ois = new ObjectInputStream(bais);
				arrayList = ( ArrayList<String>) ois.readObject();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
	         String firstLine = arrayList.get(0);
	         
	         int lineNumber = 0;
	         int counter = 0;
	         List<String> firstChunk = new ArrayList<>();
	         firstChunk.add(firstLine);
	         String line = "";
	         
	         for(int i=0; i < arrayList.size(); i++) {
	        	 
	        	 if (lineNumber < CHUNK_SIZE) {
	                    firstChunk.add(line);
	                } 
	        	 else {
	        		 
	                	
	                	try
	                	{	
	                		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		                	ObjectOutputStream oos = new ObjectOutputStream(baos);
		                	oos.writeObject(firstChunk);
		                	var msg = constructMessageByte(counter++, MAIN_SERVER_ID , "/upload", baos.toByteArray());
		                	
		                	String[] serverPath = serverPaths.get(counter%totalMasterServer).split(":");
		        	    	
		        	    	ManagedChannel ch = ManagedChannelBuilder.forAddress(serverPath[0], Integer.parseInt(serverPath[1])).usePlaintext().build();
		        			RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
		        			
		        			var r = stub.request(request);
		        			System.out.println("reply: " + r.getId() + ", from: " + r.getOrigin());
		        			ch.shutdown();
		        			counter++;
		                	
		                	lineNumber = 0;
		                	firstChunk = new ArrayList<>();
		                	firstChunk.add(firstLine);
		                	firstChunk.add(line);
		                	ch.shutdown();

	                	}
	                	catch(Exception e)
	                	{
	                		e.printStackTrace();
	                	}
	                	
	                	
	                }
	                lineNumber++;
	         }
	         if (firstChunk.size() > 0)
	            {
	            	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	            	ObjectOutputStream oos = new ObjectOutputStream(baos);
	            	oos.writeObject(firstChunk);
	            	String[] serverPath = serverPaths.get(counter%totalMasterServer).split(":");
        	    	
        	    	ManagedChannel ch = ManagedChannelBuilder.forAddress(serverPath[0], Integer.parseInt(serverPath[1])).usePlaintext().build();
        			RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
        			
	            	var msg = constructMessageByte(counter++, MAIN_SERVER_ID, "/upload", baos.toByteArray());
	            	var r = stub.request(msg);
	            	
	            	counter++;
	            }
	         
	         } catch(IOException ioe) {
	            ioe.printStackTrace();
	         }
	
    }
}
