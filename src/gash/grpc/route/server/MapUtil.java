package gash.grpc.route.server;
import java.util.*;
import java.io.*;

  public class MapUtil {
	  static String filePath="";
	 @SuppressWarnings("unchecked")
	    public static HashMap<String, String> read(String file) {
		 MapUtil.filePath = file;
	        HashMap<String, String> hmap = new HashMap<String, String>();

	        try (FileInputStream fis = new FileInputStream(file)) {

	            try (ObjectInputStream ois = new ObjectInputStream(fis)) {

	                hmap=(HashMap<String,String>)ois.readObject();

	            } catch (Exception e) {
	                e.printStackTrace();
	            }
	        } catch (Exception e) {
	            e.printStackTrace();
	        }

	        return hmap;
	    }

	    public static void write(HashMap<String, String> hmap) {
	        
	        try (FileOutputStream fos = new FileOutputStream(MapUtil.filePath)) {

	            try (ObjectOutputStream oos = new ObjectOutputStream(fos)) {
	                oos.writeObject(hmap);
	            }

	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }
}
