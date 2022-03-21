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

  public static void main(String[] args) {
    final int cport = Integer.parseInt(args[0]);
    int R = Integer.valueOf(args[1]);
    int timeout = Integer.valueOf(args[2]);
    int rebalancePeriod = Integer.valueOf(args[3]);
    Controller server = new Controller(cport, R, timeout, rebalancePeriod);
  }

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
              InetAddress dstoreAddress = client.getInetAddress();
              try{
                BufferedReader in = new BufferedReader(
                new InputStreamReader(client.getInputStream()));
                String line;
                while((line = in.readLine()) != null) {
                  logger.info("Message recived: " + line);
                  String[] splitIn = line.split(" ");
                  if (splitIn[0].equals("JOIN")) {
                    port = Integer.parseInt(splitIn[1]);
                    dstores.put(dstoreAddress, port);
                  }
                }
                
                client.close();
              }catch(Exception e){
                try {
                  dstores.remove(dstoreAddress);
                  logger.info("Removed a Dstore");
                } catch (Exception ee) {}
              }
            }
          }).start();
        }catch(Exception e){logger.info("error "+e);}
      }
    }catch(Exception e){logger.info("error "+e);}
  }

  private void sendMsg(InetAddress destinationAddress, int destinationPort, String msg) {
    try{
      Socket socket = new Socket(destinationAddress, destinationPort);
      PrintWriter out = new PrintWriter(socket.getOutputStream());
      out.println(msg); out.flush();
      logger.info("TCP message "+msg+" sent");

    }catch(Exception e){logger.info("error"+e);}
  }

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