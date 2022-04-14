package com.wugui.datax.admin.core.util;

import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.commons.io.FilenameUtils;

@Service
public class RunUtil {
	public static final org.slf4j.Logger LOGGER =org.slf4j.LoggerFactory.getLogger(RunUtil.class);

	public static void main(String[] args) {
		System.out.println(System.getProperty("user.dir"));
		System.out.println(RunUtil.taskRunning("77171"));
		System.out.println(RunUtil.Exec("jps", true));
	}
	public static String Exec(String name,boolean async){
		try {
			if (!async){
				Process p=Runtime.getRuntime().exec(name);
				p.waitFor();
				BufferedReader br=new BufferedReader(new InputStreamReader(p.getInputStream(), "UTF-8"));
			    StringBuffer sb=new StringBuffer();  
			    String inline;  
			    while(null!=(inline=br.readLine())){  
			      sb.append(inline).append("\n");
			    }
				Field field = p.getClass().getDeclaredField("pid");
				field.setAccessible(true);
				return String.valueOf(field.get(p));
			}else{
				Process p= Runtime.getRuntime().exec(name);
				Field field = p.getClass().getDeclaredField("pid");
				field.setAccessible(true);
				return String.valueOf(field.get(p));

			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static boolean taskRunning(String task){
		 Process proc;

		try {
			proc = Runtime.getRuntime().exec("jps");
			proc.waitFor();
			LOGGER.info(proc.toString());
			 BufferedReader br=new BufferedReader(new InputStreamReader(proc.getInputStream(), "UTF-8"));

	         String line=null;
	         while((line=br.readLine())!=null){
	             if(line.contains(task)){
	                 return true;
	             }
	         }
	         br.close();
	         return false;
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			return false;
		}   
	}
	public static boolean taskKill(String task){
		 Process proc;
		try {
			proc = Runtime.getRuntime().exec("jps");
			 BufferedReader br=new BufferedReader(new InputStreamReader(proc.getInputStream()));   
	         String line=null;
	         while((line=br.readLine())!=null){     
	             if(line.contains(task)){   
	            	 line = line.replaceAll("\\D", "");
					 String pid = line;
                      Runtime.getRuntime().exec("kill " + pid);
                      return true;
	             }   
	         }
	         return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}   
	}

	 public static InetAddress getInetAddress(){  		  
	        try{  
	            return InetAddress.getLocalHost();  
	        }catch(UnknownHostException e){  
	            System.out.println("unknown host!");  
	        }  
	        return null;  	  
	}
	public static String getHostIp(InetAddress netAddress){
		if(null == netAddress){
			return null;
		}
		String ip = netAddress.getHostAddress(); //get the ip address
		return ip;
	}
	public static String getHostIp(){
		InetAddress netAddress=getInetAddress();
		if(null == netAddress){
			return null;
		}
		String ip = netAddress.getHostAddress(); //get the ip address
		return ip;
	}

	public static String getHostName(InetAddress netAddress){
		if(null == netAddress){
			return null;
		}
		String name = netAddress.getHostName(); //get the host address
		return name;
	}
	public static String getHostName(){
		try{
			InetAddress netAddress = getInetAddress();
			if(null == netAddress){
				return null;
			}
			String name = netAddress.getHostName(); //get the host address
			return name;
		}catch(Exception e){
			return "";
		}
	}
	public static String getSystemOS(){
		 Properties props=System.getProperties();
		 String osName = props.getProperty("os.name");
		 return osName;
	}


}
