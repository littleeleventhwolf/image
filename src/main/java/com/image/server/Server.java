package com.image.server;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

/**
 * Send picture to Spark Streaming.
 */
public class Server {
	public ServerSocket getServerSocket(int port){
		ServerSocket server=null;
		try {
			server = new ServerSocket(port);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return server;
	}

	public void sendData(String path, ServerSocket server){
		OutputStream out=null;
		FileInputStream in=null;
		BufferedOutputStream bf =null;
		try {
			out = server.accept().getOutputStream();
			File file = new File(path);
			in = new FileInputStream(file);
			bf = new BufferedOutputStream(out);
			byte[] bt = new byte[(int)file.length()];
			in.read(bt);
			bf.write(bt);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(in!=null){
				try {
					in.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(bf!=null){
				try {
					bf.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(out!=null){
				try {
					out.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(!server.isClosed()){
				try {
					server.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
		if(args.length<4){
			System.err.println("Usage:Server <port> <file or dir> <send-times> <sleep-time(ms)>");
			System.exit(1);
		}

		Map<Integer, String> fileMap = null;

		Server s = new Server();
		for (int i = 0; i < Integer.parseInt(args[2]) ; i++) {
			ServerSocket server =null;
			while(server==null){
				server = s.getServerSocket(Integer.parseInt(args[0]));
				try {
					Thread.sleep(Integer.parseInt(args[3]));
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			while(!server.isBound()){
				try {
					server.bind(new InetSocketAddress(Integer.parseInt(args[0])));
					System.out.println("第"+(i+1)+"个服务端绑定成功");
					Thread.sleep(Integer.parseInt(args[3]));
				} catch (NumberFormatException | IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			fileMap = s.getFileMap(args[1]);

			System.out.println("fileMap.size="+fileMap.size());
			//System.out.println("fileMap="+fileMap);

			s.sendData(fileMap.get(s.getNum(0, fileMap.size()-1)), server);
			//s.sendData(args[1], server);
		}
	}


	public Map<Integer, String> getMap(String dir, Map<Integer, String> fileMap){
		File file = new File(dir);
		if(file.isFile()){
			if(file.getName().endsWith(".jpg")||file.getName().endsWith(".bmp")|file.getName().
	                                            endsWith(".JPG")||file.getName().endsWith(".BMP")){
				if(file.length()<1024*1024*2){
					fileMap.put(fileMap.size(),file.getAbsolutePath());
				}
			}else{
			}
		}
		if(file.isDirectory()){
			File[] files = file.listFiles();
			for (int j = 0; j < files.length; j++) {
				getMap(files[j].getAbsolutePath(), fileMap);
			}
		}
		return fileMap;
	}

	public Map<Integer, String> getFileMap(String dir){
		Map<Integer, String> fileMap = new HashMap<Integer, String>();
		return getMap(dir, fileMap);
	}

	public int getNum(int offset, int max){
		int i = offset+(int)(Math.random()*max);
		if(i>max){
			return i-offset;
		}else{
			return i;
		}
	}
}