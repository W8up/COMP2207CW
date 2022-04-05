import java.io.*;
import java.net.*;
import java.util.ArrayList;
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
  Hashtable<Integer, Socket> dstores = new Hashtable<>();
  Hashtable<Integer, ArrayList<String>> fileLocations = new Hashtable<>();
  Hashtable<String, Boolean> index = new Hashtable<>();
  Hashtable<String, Integer> locks = new Hashtable<>();

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
          logger.info("New connection");
          new Thread(new Runnable(){
            public void run() {
              int port = 0;
              try{
                BufferedReader in = new BufferedReader(
                new InputStreamReader(client.getInputStream()));
                String line;
                while((line = in.readLine()) != null) {
                  logger.info("Message recived: " + line);
                  String[] splitIn = line.split(" ");
                  if (splitIn[0].equals("JOIN")) {
                    port = Integer.parseInt(splitIn[1]);
                    dstores.put(port, client);

                  //LIST from Dstore
                  } else if (splitIn[0].equals("LIST") && port != 0) {
                    ArrayList<String> files = new ArrayList<>();
                    for (int i = 1 ; i < splitIn.length; i++ ) {
                      Boolean p = false;
                      try{
                        p = index.get(splitIn[i]);
                      } catch (Exception e) {}
                      
                      index.put(splitIn[i], p);
                      files.add(splitIn[i]);
                    }
                    try {
                    fileLocations.remove(port);
                    } catch (Exception e) {}

                    fileLocations.put(port, files);
                    logger.info("Files " + files + " added for " + port);

                  //STORE from client
                  } else if (splitIn[0].equals("STORE")) {
                    String fileName = splitIn[1];
                    if (index.get(fileName) != null) {
                      sendMsg(client, "ERROR_FILE_ALREADY_EXISTS");
                    } else {
                      index.put(fileName, true);
                      float balanceNumber = (R * index.size())/dstores.size();
                      logger.info("Balance number " + balanceNumber);
                      String toSend = "STORE_TO";
                      ArrayList<Socket> ports = new ArrayList<>();

                      for (int c = 0; c < R; c++) {
                        for (Integer p : dstores.keySet()) {
                          try {
                            if (fileLocations.get(p).size() + 1 <= Math.ceil(balanceNumber) && fileLocations.get(p).size() < Math.floor(balanceNumber) && !fileLocations.get(p).contains(fileName)) {
                              fileLocations.get(p).add(fileName);
                              ports.add(dstores.get(p));
                              toSend += " " + p;
                              logger.info("File " + fileName + " added to " + p);
                              break;
                            }
                          } catch (Exception e) {
                            ArrayList<String> file = new ArrayList<>();
                            file.add(fileName);
                            fileLocations.put(p, file);
                            ports.add(dstores.get(p));
                            toSend += " " + p;
                            logger.info("File " + fileName + " added to " + p);
                            break;
                          }
                        }
                      }
                      sendMsg(client, toSend);
                      locks.put(fileName, 0);
                      logger.info("Thread paused");
                      while (true) {
                        if (locks.get(fileName).equals(R)){
                          break;
                        }
                      }
                      sendMsg(client, "STORE_COMPLETE");
                      index.put(fileName, false);
                      logger.info("Index updated for " + fileName);
                    }
                  //LIST from Client
                  } else if (splitIn[0].equals("LIST")) {
                    logger.info("LIST from Client");
                    String toSend = "LIST";
                    for (String i : index.keySet()) {
                      if (!index.get(i)) {
                        toSend += " " + i;
                      }
                    }
                    sendMsg(client, toSend);
                  } else if (splitIn[0].equals("STORE_ACK")) {
                    try {
                      synchronized (locks.get(splitIn[1])) {
                        locks.put(splitIn[1], locks.get(splitIn[1]) + 1);
                        logger.info("ACK incremented");
                      }
                    } catch (Exception e) {logger.info("error " + e.getMessage());}
                  }
                }
                
              }catch(Exception e){
                logger.info(e.getMessage());
                try {
                  if (port != 0) {
                    dstores.remove(port);
                    fileLocations.remove(port);
                    logger.info("Removed a Dstore");
                  } else {
                    logger.info("Connection closed");
                  }
                } catch (Exception ee) {}
              }
            }
          }).start();
        }catch(Exception e){logger.info("error "+e);}
      }
    }catch(Exception e){logger.info("error "+e);}
  }

  /**
   * Send a text message to the specified socket
   * @param socket
   * @param msg
   */
  private void sendMsg(Socket socket, String msg) {
    try{
      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      out.println(msg);
      logger.info("TCP message "+msg+" sent");

    }catch(Exception e){logger.info("error"+e);}
  }

  /**
   * Controlls the Dstores when rebalencing
   */
  public void rebalance() {
    if (dstores.size() >= R) {
      for (int port : dstores.keySet() ) {
        this.sendMsg(dstores.get(port), "LIST");
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