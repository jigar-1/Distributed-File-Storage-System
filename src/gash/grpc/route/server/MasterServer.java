package gash.grpc.route.server;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.zookeeper.*;
import gash.grpc.route.server.Engine;
import gash.grpc.route.server.RouteServerImpl;

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import route.Route;
import route.RouteServiceGrpc;
import route.RouteServiceGrpc.RouteServiceImplBase;

public class MasterServer extends RouteServiceImplBase implements Watcher{

	private Server svr;
    private ZooKeeper zooKeeper;

// Zookeeper config
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 5000;
    private static final String ZNODE_PATH = "/leader";
    
 // when using server.conf
    private static long clientID = 501;
    private static int leaderPort = 8000;
    private static String leaderIP = "";
    private static int leaderId = 0;
    private final static int CHUNK_SIZE = 10000;
    private static int ROUND_ROBIN_COUNTER = 0;
    static final int numberOfServers = 4;
	private static final long MASTER_SERVER_ID = 3000;
    static int counter = 0;



private static Properties getConfiguration(final File path) throws IOException {
	if (!path.exists())
		throw new IOException("missing file");

	Properties rtn = new Properties();
	FileInputStream fis = null;
	try {
		fis = new FileInputStream(path);
		rtn.load(fis);
	} finally {
		if (fis != null) {
			try {
				fis.close();
			} catch (IOException e) {
				// ignore
			}
		}
	}

	return rtn;
}

private static final Route constructMessage(int mID, int toID, String path, String payload) {
	Route.Builder bld = Route.newBuilder();
	bld.setId(mID);
	bld.setDestination(toID);
	bld.setOrigin(MasterServer.clientID);
	bld.setPath(path);

	byte[] hello = payload.getBytes();
	bld.setPayload(ByteString.copyFrom(hello));

	return bld.build();
}

private static final Route constructMessageByte(int mID, long toID, String path, byte[] payload) {
	Route.Builder bld = Route.newBuilder();
	bld.setId(mID);
	bld.setDestination(toID);
	bld.setOrigin(MasterServer.clientID);
	bld.setPath(path);
	bld.setPayload(ByteString.copyFrom(payload));

	return bld.build();
}

private static final Route constructQueryMessage(int mID, int toID, String path, String payload) {
	Route.Builder bld = Route.newBuilder();
	bld.setId(mID);
	bld.setDestination(toID);
	bld.setOrigin(MasterServer.clientID);
	bld.setPath(path);

	byte[] hello = payload.getBytes();
	bld.setPayload(ByteString.copyFrom(hello));

	
	return bld.build();
}

private static final void response(Route reply) {
	// TODO handle the reply/response from the server
	var payload = new String(reply.getPayload().toByteArray());
	System.out.println("reply: " + reply.getId() + ", from: " + reply.getOrigin());
}

public void sendRequest(String ip, int destPort, int destId, int msgId, String path, String payload)
{
	ManagedChannel ch = ManagedChannelBuilder.forAddress(ip, destPort).usePlaintext().build();
	RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
	
	var msg = MasterServer.constructMessage(msgId, destId, path, payload);
	var r = stub.request(msg);
	response(r);
}

private static void readFileToBytes(String filePath) throws IOException {
	ManagedChannel ch = ManagedChannelBuilder.forAddress(MasterServer.leaderIP, MasterServer.leaderPort).usePlaintext().build();
	RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
	try {
         File file = new File(filePath);
         FileReader fr = new FileReader(file);
         BufferedReader br = new BufferedReader(fr);
         String firstLine = br.readLine();
         int lineNumber = 0;
         int counter = 0;
         List<String> firstChunk = new ArrayList<>();
         firstChunk.add(firstLine);
         String line = "";
         while((line = br.readLine()) != null) {
        	 
        	 if (lineNumber < MasterServer.CHUNK_SIZE) {
                    firstChunk.add(line);
                } else {
                	System.out.println("Chunk over: line 132");
                	try
                	{	if(ROUND_ROBIN_COUNTER== MasterServer.leaderId) {
                			ROUND_ROBIN_COUNTER=(ROUND_ROBIN_COUNTER+1)%numberOfServers;
                			}
                		ByteArrayOutputStream baos = new ByteArrayOutputStream();
	                	ObjectOutputStream oos = new ObjectOutputStream(baos);
	                	oos.writeObject(firstChunk);
	                	var msg = MasterServer.constructMessageByte(counter++, ROUND_ROBIN_COUNTER++, "/upload", baos.toByteArray());
//	                	System.out.println("Before stub: line 139");
	                	var r = stub.request(msg);
	                	System.out.println("Chunk sent with id: " + (counter-1));
	                	response(r);
	                	
	                	lineNumber = 0;
	                	firstChunk = new ArrayList<>();
	                	firstChunk.add(firstLine);
	                	firstChunk.add(line);
	                	ROUND_ROBIN_COUNTER%=numberOfServers;

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
            	var msg = MasterServer.constructMessageByte(counter++, ROUND_ROBIN_COUNTER++, "/to/somewhere", baos.toByteArray());
            	var r = stub.request(msg);
            	response(r);
            	ROUND_ROBIN_COUNTER%=numberOfServers;
            }
         br.close();
         } catch(IOException ioe) {
            ioe.printStackTrace();
         }
	finally {
		ch.shutdown();
	}
}



private static void sendQuery() {
	System.out.println("Sending query");
	ManagedChannel ch = ManagedChannelBuilder.forAddress(MasterServer.leaderIP, MasterServer.leaderPort).usePlaintext().build();
	RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
	
	//format - Startdate:Endate(optional):ViolationCode(optional)
	String queryPayload = "18/1/1:18/1/15:71";
	int queryId = 1;
	int messageId = 1;
	var msg = MasterServer.constructQueryMessage(queryId,0,"/query",queryPayload);
	Iterator<Route> r ;
	
	try {
		r = stub.query(msg);
		while(r.hasNext()) {
			Route res = r.next();
			byte[] byteArray = res.getPayload().toByteArray();
			System.out.println(byteArray.length);
		}
		
	}catch(Exception e) {
		e.printStackTrace();
	}
	
	
	
	ch.shutdown();
}

public static void main(String[] args) throws Exception {
	String path = args[0];
	String csvFilepath = "";
	try {
		Properties conf = MasterServer.getConfiguration(new File(path));
		MasterServer.leaderIP = conf.getProperty("server.connects.ip");
		MasterServer.leaderPort = Integer.parseInt(conf.getProperty("server.connects.port"));
		MasterServer.leaderId = Integer.parseInt(conf.getProperty("server.connects.id"));
		
		MasterServer masterServer = new MasterServer();
        masterServer.connectToZooKeeper();

        masterServer.start();
        masterServer.blockUntilShutdown();
        
        // Register this master server with ZooKeeper
        String thisServerPath = masterServer.registerWithZooKeeper();

        // Get the address of another master server
        String leaderServerPath = "/leader"; // Example: Getting address of the leader server
        String[] leaderServerAddress = masterServer.getServerAddress(leaderServerPath).split(":");
		MasterServer.leaderIP = leaderServerAddress[0];
		MasterServer.leaderPort = Integer.parseInt(leaderServerAddress[1]);
		
	} catch (IOException e) {
		// TODO better error message
		e.printStackTrace();
	}
}

	private void start() throws Exception {
		svr = ServerBuilder.forPort(Engine.getInstance().getServerPort()).addService(new MasterServer()).build();

		Engine.logger.info("starting server");
		svr.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				MasterServer.this.stop();
			}
		});
	}

	protected void stop() {
		svr.shutdown();
	}

	private void blockUntilShutdown() throws Exception {
		/* TODO what clean up is required? */
		svr.awaitTermination();
        zooKeeper.close();
	}

    private void connectToZooKeeper() throws Exception {
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);

        // Wait until the connection is established
        while (zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
            Thread.sleep(100);
        }

        System.out.println("Connected to ZooKeeper!");
    }

    private String registerWithZooKeeper() throws KeeperException, InterruptedException {
        // Create an ephemeral sequential znode under /masters
        String thisServerPath = zooKeeper.create(ZNODE_PATH + "/master_", null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("Registered with ZooKeeper. Server path: " + thisServerPath);

        return thisServerPath;
    }

    private String getServerAddress(String serverPath) throws KeeperException, InterruptedException {
        byte[] serverAddressBytes = zooKeeper.getData(serverPath, false, null);
        String serverAddress = new String(serverAddressBytes);

        System.out.println("Server address of target server: " + serverAddress);

        return serverAddress;
    }

    @Override
    public void process(WatchedEvent event) {
        // Handle ZooKeeper events, if required
    }
    
    
    @Override
	public void request(route.Route request, StreamObserver<route.Route> responseObserver) {
    	ManagedChannel ch = ManagedChannelBuilder.forAddress(MasterServer.leaderIP, MasterServer.leaderPort).usePlaintext().build();
    	RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
    	
    	Route.Builder bld = Route.newBuilder();
		bld.setId(counter++);
		bld.setDestination(ROUND_ROBIN_COUNTER++);
		bld.setOrigin(MASTER_SERVER_ID);
		bld.setPath("/upload");
		bld.setPayload(request.getPayload());
		
    	var r = stub.request(bld.build());
    	
    	ROUND_ROBIN_COUNTER%=numberOfServers;
    }
    
    @Override
	public void query(route.Route request, StreamObserver<route.Route> responseObserver) {
    	ManagedChannel ch = ManagedChannelBuilder.forAddress(MasterServer.leaderIP, Integer.parseInt(MasterServer.leaderIP)).usePlaintext().build();
		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
		
		Route.Builder bld = Route.newBuilder();
		bld.setId(request.getId());
		bld.setDestination(111);
		bld.setOrigin(MASTER_SERVER_ID);
		bld.setPath("/query");
		bld.setPayload(request.getPayload());

		var r = stub.query(bld.build());
		
		while(r.hasNext())
		{
			responseObserver.onNext(r.next());
		}
		ch.shutdown();
    }
}


