package gash.grpc.route.server;


import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;

public class Writer {
	private List<String> arr;
	protected static Logger logger = LoggerFactory.getLogger("worker");
	private String storageLocation;
	private HashMap<String, String> map = Engine.getInstance().getHashMap();
	
	public Writer(List<String> arr, String storageLocation)
	{
		this.arr = arr;
		this.storageLocation = storageLocation;
	}
	
	public void processData() throws ParseException, IOException {
		int size = arr.size();
		if(size == 0)
		{
			return;
		}
		
		Iterator<String> iter = arr.iterator();
		String headerStr = iter.next();
		String[] header = headerStr.split(",");
		
		boolean hashOut = true;
		
		
		
		int[] index = new int[2];
		
		for(int i=0; i<header.length; i++)
		{
			if(header[i].toLowerCase().contains("issue date"))
			{
				index[0] = i;
			}
			else if(header[i].toLowerCase().contains("violation code"))
			{
				index[1] = i;
			}
				
		}
		
		while(iter.hasNext())
		{
			String lineStr = iter.next();
			String[] line = lineStr.split(",");
			
			String date_str = line[index[0]];
			
			String vc = line[index[1]];
			
			SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy");
			SimpleDateFormat df2 = new SimpleDateFormat("yy/MM/dd");
			String date = df2.format(df.parse(date_str));
			
			
			String key = "/" + date + "/" + vc;
			
			if(map.containsKey(key))
			{
				String filePath = map.get(key);
				FileWriter fw = new FileWriter(filePath, true);				
				fw.write(lineStr);
				fw.write("\n");
				fw.flush();
				fw.close();
			}
			
			//Write to a csv file
			
			else
			{
				logger.info("New key found: " + key);
//				String filePath = "/Users/vatsalbhanderi/eclipse-workspace/CSVParse/csvSamples/18/01/01"+key+".csv";
				String filePath = storageLocation + key + ".csv";
				File file = new File(filePath);
				file.getParentFile().mkdirs();
				map.put(key, filePath);
				FileWriter fw = new FileWriter(file,true);
				fw.write(headerStr);
				fw.write("\n");
				fw.write(lineStr);
				fw.write("\n");
				fw.flush();
				fw.close();
			}
			//Append to a csv file
			
			//Search in hashMap and then append	
		}
		
	}
	
	

//	public static void main(String[] args) throws FileNotFoundException, ClassNotFoundException, ParseException {
//		String filePath = "/Users/vatsalbhanderi/Downloads/parkingviolations/Parking_Violations_Issued_-_Fiscal_Year_2018.csv";
//		File file = new File(filePath);
//		FileReader fr = new FileReader(file);
//		BufferedReader br = new BufferedReader(fr);
//		int lineNumber = 0;
//		int counter = 0;
//		List<String> firstChunk = new ArrayList<>();
//		String line = "";
//		try {
//			while((line = br.readLine()) != null) {
//				if (lineNumber < 10) {
//					firstChunk.add(line);
//				} else {
//					break;
//				}
//				lineNumber++;
//			}
//			int i=0;
//			
////			for(String s: firstChunk)
////			{
////				System.out.println("Line number " + i + " ;String val: " + s);
////				i++;
////			}
//			System.out.println("Total length of the chunk: " + firstChunk.size());
//			
//			ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        	ObjectOutputStream oos = new ObjectOutputStream(baos);
//        	oos.writeObject(firstChunk);
//        	
//        	byte[] byteArr = baos.toByteArray();
//        	
//        	ByteArrayInputStream bais = new ByteArrayInputStream(byteArr);
//        	ObjectInputStream ois = new ObjectInputStream(bais);
//        	ArrayList<String> arrayList = ( ArrayList<String>) ois.readObject();
//        	
//        	System.out.println("Size of parsed arraylist: " + arrayList.size());
//        	System.out.println("Last element of the csv: " + arrayList.get(0));
//        	
//        	processData(arrayList);
//        	
//        	
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}

	
}
