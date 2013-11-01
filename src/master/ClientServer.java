package master;

import java.net.*;
import java.io.*;

public class ClientServer extends Thread {
	private Master master = null;
	private int port = -1;
	
	/**
	 * Constructor which sets the port field
	 * 
	 * @param m			the master object
	 * @param p			the port used by the clientserver
	 */
	ClientServer (Master m, int p) {
		master = m;
		port = p;
	}
	
	/**
	 * run() handles (multiple) clients that want to connect to the
	 * server by starting a ClientServerThread
	 */
	public void run() {
		try (ServerSocket serverSocket = new ServerSocket(port)) 
		{
            while (true) {
            	System.out.println("Server is listening...");
	            new ClientServerThread(serverSocket.accept(), master).start();
	        }
	    } catch (BindException e) {
            System.out.println(e.getMessage());
            System.out.println("Close existing running master and try again. Exiting");
            System.exit(-1);
        } catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}