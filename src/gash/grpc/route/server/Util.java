package gash.grpc.route.server;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

public class Util {
	
	public static void byteArrayToCSV(byte[] payload, String filePath ) { 
		
		ByteArrayInputStream bais = new ByteArrayInputStream(payload);
    	ObjectInputStream ois;
    	ArrayList<String> arrayList = new ArrayList<>();
    	
		try {
			ois = new ObjectInputStream(bais);
			arrayList = ( ArrayList<String>) ois.readObject();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	FileWriter fw;
		try {
			fw = new FileWriter(filePath, true);
			for(String line: arrayList)
	    	{
	    		fw.write(line);
				fw.write("\n");
				fw.flush();
	    	}
	    	fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	
	}
	
	
	
}
