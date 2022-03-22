import java.io.*;
import java.net.*;

import java.util.Hashtable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

public class Controller {

  private static final Logger logger = Logger.getLogger(Controller.class.getName());

  final int cport;
  int R;
  int timeout;
  int rebalancePeriod;
  Hashtable<InetAddress, Integer> dstores = new Hashtable<>();
  Hashtable<InetAddress, String> index = new Hashtable<>();

  public static void main(String[] args) {
    final int cport = Integer.parseInt(args[0]);
    int R = Integer.valueOf(args[1]);
    int timeout = Integer.valueOf(args[2]);
    int rebalancePeriod = Integer.valueOf(args[3]);
    Controller server = new Controller(cport, R, timeout, rebalancePeriod);
  }

  /**
   * Makes the connection server
   * @param cport The port to recieve on
   * @param R The number of Dstores require to rebalance
   * @param timeout The time in seconds to timeout
   * @param rebalancePeriod The duration in seconds to rebalance files
   */
  private Controller(int cport, int R, int timeout, int rebalancePeriod) {
    this.cport = cport;
    this.R = R;
    this.timeout = timeout;
    this.rebalancePeriod = rebalancePeriod;

    //Rebalance loop
    Timer timer = new Timer("ServerLoop");
    TimerTask task = new ServerTimerTask(this);
    try {
      timer.schedule(task, rebalancePeriod * 1000, rebalancePeriod * 1000);
    } catch (Exception e) {
      logger.info("Catching: " + e.getMessage());
    }

    //Receiver
    try{
      ServerSocket ss = new ServerSocket(cport);
      for(;;){
        try{
          final Socket client = ss.accept();
          new Thread(new Runnable(){
            public void run(){
              int port;
              InetAddress address = client.getInetAddress();
              try{
                BufferedReader in = new BufferedReader(
                new InputStreamReader(client.getInputStream()));
                String line;
                while((line = in.readLine()) != null) {
                  logger.info("Message recived: " + line);
                  String[] splitIn = line.split(" ");
                  if (splitIn[0].equals("JOIN")) {
                    port = Integer.parseInt(splitIn[1]);
                    dstores.put(address, port);
                  } else if (splitIn[0].equals("LIST") && dstores.containsKey(address)) {
                    String files = "";
                    try {
                      for (int i = 1 ; i < splitIn.length -1; i++ ) {
                        files += splitIn[i] + " ";
                      }
                    } catch (Exception e) {
                      files += splitIn[splitIn.length - 1];
                    }
                    try {
                    index.remove(address);
                    } catch (Exception e) {}

                    index.put(address, files);
                    logger.info("Files " + files + " added for " + address);
                  }
                }
                
                client.close();
              }catch(Exception e){
                try {
                  dstores.remove(address);
                  logger.info("Removed a Dstore");
                } catch (Exception ee) {}
              }
            }
          }).start();
        }catch(Exception e){logger.info("error "+e);}
      }
    }catch(Exception e){logger.info("error "+e);}
  }

  /**
   * Send a text message to the specified target
   * @param destinationAddress The target address
   * @param destinationPort The target port
   * @param msg The message to be sent
   */
  private void sendMsg(InetAddress destinationAddress, int destinationPort, String msg) {
    try{
      Socket socket = new Socket(destinationAddress, destinationPort);
      PrintWriter out = new PrintWriter(socket.getOutputStream());
      out.println(msg); out.flush();
      logger.info("TCP message "+msg+" sent");
      socket.close();

    }catch(Exception e){logger.info("error"+e);}
  }

  /**
   * Controlls the Dstores when rebalencing
   */
  public void rebalance() {
    if (dstores.size() >= R) {
      for (InetAddress address : dstores.keySet() ) {
        this.sendMsg(address, dstores.get(address), "LIST");
      }
    } else {
      logger.info("Not enougth Dstores");
    }
  }
}

class ServerTimerTask extends TimerTask {
  private Controller c;

  ServerTimerTask(Controller c) {
    this.c = c;
  }

  @Override
  public void run() {
    c.rebalance();
  }
  
}