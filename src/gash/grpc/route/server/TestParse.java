package gash.grpc.route.server;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Date;
import java.util.HashMap;

import com.google.common.primitives.Bytes;


public class TestParse {
	
	static HashMap<String, String> map = new HashMap<>();
	
	public static void processData() throws ParseException, IOException {
		
		String storageLocation = "/Users/vatsalbhanderi/eclipse-workspace/CSVParse/csvSamples";
		String query = "18/01/01:18/01/05:71";
		String filePath = storageLocation + "/queryOutput.csv";
		FileWriter fw = new FileWriter(filePath, true);	
		File queryOutputFile = new File(filePath);
		boolean isHeader = false;
		String[] queryParams = query.split(":",-1);
		String startDate = queryParams[0];
		String endDate = queryParams[1];
		String vc = queryParams[2];
		
		if(endDate.isEmpty()) {
			endDate = startDate;
		}
		
		SimpleDateFormat df = new SimpleDateFormat("yy/MM/dd");
		Date dt = df.parse(startDate);
		Date dte = df.parse(endDate);
		Calendar c = Calendar.getInstance();
		
		while(true) {
			
			String dts = "/" + df.format(dt) +"/"+ vc;
			System.out.println("dts" + dts);
			Set<String> set = map.keySet().stream().filter(s -> s.startsWith(dts)).collect(Collectors.toSet());
			
			if(!set.isEmpty()){
				String row;
				for(String loc : set) {
					System.out.println("loc:" + loc);
					BufferedReader csvReader = new BufferedReader(new FileReader(storageLocation + loc + ".csv"));
					if(isHeader) csvReader.readLine();
					while ((row = csvReader.readLine()) != null) {
									
						fw.write(row);
						fw.write("\n");
						fw.flush();
						
					}
					isHeader = true;
					csvReader.close();
				}
			}
			
			if(dt.compareTo(dte)==0) {
				break;
			}
			
			c.setTime(dt);
			c.add(Calendar.DATE, 1);
			dt = df.parse(df.format(c.getTime()));		
		}
		
		fw.close();
		
	}
	
	public static void processData(List<String> arr) throws ParseException, IOException {
		int size = arr.size();
		if(size == 0)
		{
			return;
		}
		
		Iterator<String> iter = arr.iterator();
		String headerStr = iter.next();
		String[] header = headerStr.split(",");
		
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
		
		System.out.println(header[index[0]] + "-" + header[index[1]]);
		
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
				System.out.println("new key found" + key);
				String filePath = "/Users/vatsalbhanderi/eclipse-workspace/CSVParse/csvSamples"+key+".csv";
				File file = new File(filePath);
				file.getParentFile().mkdirs();
				map.put(key, filePath);
				FileWriter fw = new FileWriter(file);
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
	
	public static void writeToCSV(String filePath, boolean appendFlag, String line) throws IOException {
		
		
		
	}

	public static void main(String[] args) throws FileNotFoundException, ClassNotFoundException, ParseException {
		String filePath = "/Users/vatsalbhanderi/Downloads/parkingviolations/Parking_Violations_Issued_-_Fiscal_Year_2018.csv";
		File file = new File(filePath);
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		int lineNumber = 0;
		int counter = 0;
		List<String> firstChunk = new ArrayList<>();
		List<String> secondChunk = new ArrayList<>();
		String line = "";
		try {
//			while((line = br.readLine()) != null) {
//				if (lineNumber < 5000) {
//					firstChunk.add(line);
//				} 
//				else if(lineNumber < 10000 || lineNumber>5000 ) {
//					secondChunk.add(line);
//				}
//				else {
//					break;
//				}
//				lineNumber++;
//			}
//			
//			
////			for(String s: firstChunk)
////			{
////				System.out.println("Line number " + i + " ;String val: " + s);
////				i++;
////			}
//			System.out.println("Total length of the chunk: " + firstChunk.size());
			
			
			
			List<String> first = new ArrayList<>();
			List<String> second = new ArrayList<>();
			
			first.add("f");
			first.add("i");
			first.add("r");
			first.add("s");
			first.add("t");
			
			second.add("s");
			second.add("e");
			second.add("c");
			second.add("o");
			second.add("n");
			second.add("d");
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
        	ObjectOutputStream oos = new ObjectOutputStream(baos);
        	oos.writeObject(first);
        	byte[] response = new byte[0];
        	
        	byte[] byteArrFirst = baos.toByteArray();
        	oos.close();
        	
        	ByteArrayOutputStream baps = new ByteArrayOutputStream();
        	ObjectOutputStream oops = new ObjectOutputStream(baps);
        	oops.writeObject(second);
        	
        	
        	
        	byte[] byteArrSecond = baps.toByteArray();
        	
        	response = Bytes.concat(response, byteArrFirst);
        	response = Bytes.concat(response,byteArrSecond);
        	
        	
        	
        	
        	System.out.println("response length" + response.length);
        	
        	
        	
        	
        	ByteArrayInputStream bais = new ByteArrayInputStream(response);
        	ObjectInputStream ois = new ObjectInputStream(bais);
        	ArrayList<String> arrayList = ( ArrayList<String>) ois.readObject();
        	
        	System.out.println("array size" + arrayList.toString());
//        	
//        	System.out.println("response: " + arrayList.size());
//        	
//        	System.out.println("Size of parsed arraylist: " + arrayList.size());
//        	System.out.println("Last element of the csv: " + arrayList.get(0));
//        	
//        	for(String s: arrayList)
//        	{
//        		FileWriter fw = new FileWriter("/Users/vatsalbhanderi/eclipse-workspace/CSVParse/csvSamples/test.csv", true);	
//        		fw.write(s);
//				fw.write("\n");
//				fw.flush();
//				fw.close();
//        	}
//        	
//        	
//        	processData(arrayList);
//        	processData();
        	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
}
