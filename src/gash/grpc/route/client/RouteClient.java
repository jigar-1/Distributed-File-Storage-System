	package gash.grpc.route.client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import route.Route;
import route.RouteServiceGrpc;

/**
 * copyright 2023, gash
 *
 * Gash licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class RouteClient {
	
	// when using server.conf
	private static long clientID = 501;
	private static int port = 8000;
	private static String ip = "";
	private static int CHUNK_SIZE = 50;
	static final int numberOfServers = 4;
	private static final int MAIN_SERVER_ID = 8000;
	
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
		bld.setOrigin(RouteClient.clientID);
		bld.setPath(path);

		byte[] hello = payload.getBytes();
		bld.setPayload(ByteString.copyFrom(hello));

		return bld.build();
	}

	private static final Route constructMessageByte(int mID, int toID, String path, byte[] payload) {
		Route.Builder bld = Route.newBuilder();
		bld.setId(mID);
		bld.setDestination(toID);
		bld.setOrigin(RouteClient.clientID);
		bld.setPath(path);
		bld.setPayload(ByteString.copyFrom(payload));

		return bld.build();
	}
	
	private static final Route constructQueryMessage(int mID, int toID, String path, String payload) {
		Route.Builder bld = Route.newBuilder();
		bld.setId(mID);
		bld.setDestination(toID);
		bld.setOrigin(RouteClient.clientID);
		bld.setPath(path);

		byte[] hello = payload.getBytes();
		bld.setPayload(ByteString.copyFrom(hello));

		
		return bld.build();
	}

	private static final void response(Route reply) {
		// TODO handle the reply/response from the server
		System.out.println("reply: " + reply.getId() + ", from: " + reply.getOrigin());
	}

	public void sendRequest(String ip, int destPort, int destId, int msgId, String path, String payload)
	{
		ManagedChannel ch = ManagedChannelBuilder.forAddress(ip, destPort).usePlaintext().build();
		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
		
		var msg = RouteClient.constructMessage(msgId, destId, path, payload);
		var r = stub.request(msg);
		response(r);
	}
	
	//Client code to send a csv file in chunk of bytes
	private static void readFileToBytes(String filePath) throws IOException {
		ManagedChannel ch = ManagedChannelBuilder.forAddress(RouteClient.ip, RouteClient.port).usePlaintext().build();
		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
		try {
	         File file = new File(filePath);
	         FileReader fr = new FileReader(file);
	         BufferedReader br = new BufferedReader(fr);
	         String firstLine = br.readLine();
	         int lineNumber = 0;
	         int counter = 0;
	         List<String> chunk = new ArrayList<>();
	         
	         String line = "";
	         while((line = br.readLine()) != null) {
	        	 chunk.add(line);
	        	
	         }
	         
	        ByteArrayOutputStream baos = new ByteArrayOutputStream();
          	ObjectOutputStream oos = new ObjectOutputStream(baos);
          	oos.writeObject(chunk);
          	var msg = RouteClient.constructMessageByte(counter++, MAIN_SERVER_ID, "/upload", baos.toByteArray());
//          	System.out.println("Before stub: line 139");
          	var r = stub.request(msg);
          	System.out.println("Chunk sent with id: " + (counter-1));
          	response(r);
          	
	         br.close();
	         } catch(IOException ioe) {
	            ioe.printStackTrace();
	         }
		finally {
			ch.shutdown();
		}
	}
	
	private static void sendLeaderInitiation() {
		ManagedChannel ch = ManagedChannelBuilder.forAddress(RouteClient.ip, RouteClient.port).usePlaintext().build();
		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
		String path = "/initiateLeaderElection";
		var msg = RouteClient.constructMessageByte(0, 3, path, new byte[0]);
		var r = stub.request(msg);
		
		response(r);
		
	}
	
	private static void sendQuery() {
		System.out.println("Sending query");
		ManagedChannel ch = ManagedChannelBuilder.forAddress(RouteClient.ip, RouteClient.port).usePlaintext().build();
		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
		
		//format - Startdate:Endate(optional):ViolationCode(optional)
		String queryPayload = "18/1/1:18/1/15:71";
		int queryId = 1;
		var msg = RouteClient.constructQueryMessage(queryId,0,"/query",queryPayload);
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

	public static void main(String[] args) throws IOException, InterruptedException {
		String path = args[0];
		String csvFilepath = "";
		try {
			Properties conf = RouteClient.getConfiguration(new File(path));
			csvFilepath = conf.getProperty("csvFile");
			RouteClient.ip = conf.getProperty("server.connects.ip");
			RouteClient.port = Integer.parseInt(conf.getProperty("server.connects.port"));
		} catch (IOException e) {
			// TODO better error message
			e.printStackTrace();
		}

		sendLeaderInitiation();
		
		//Data Injection
		readFileToBytes(csvFilepath);
		
		//Thread to sleep to enable data injection
		Thread.sleep(60000);
		sendQuery();
	}
}
