package gash.grpc.route.server;


import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;

public class Reader {
	private String query;
	protected static Logger logger = LoggerFactory.getLogger("worker");
	private String storageLocation;
	private HashMap<String, String> map = Engine.getInstance().getHashMap();
	
	public Reader(String query, String storageLocation)
	{
		this.query = query;
		this.storageLocation = storageLocation;
	}
	
	public void processData() throws ParseException, IOException {
		
		String filePath = storageLocation + "/queryOutput.csv";
		File queryOutputFile = new File(filePath);
		boolean isHeader = false;
		String[] queryParams = query.split(":",-1);
		FileWriter fw = new FileWriter(filePath, true);	
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
			
			Set<String> set = map.keySet()
	                .stream()
	                .filter(s -> s.startsWith(dts))
	                .collect(Collectors.toSet());
			
			if(!set.isEmpty()){
				String row;
				for(String loc : set) {
					BufferedReader csvReader = new BufferedReader(new FileReader(map.get(loc)));
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
	
	public ArrayList<String> convertCSVtoArray(String filePath) throws IOException
	{
		File file = new File(filePath);
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        
        String line = "";
        ArrayList<String> chunk = new ArrayList<>();
        
        while((line = br.readLine()) != null) {
        	chunk.add(line);
        }
        
        return chunk;
    	
	}
}
