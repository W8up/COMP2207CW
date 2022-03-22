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
                    dstores.put(port, client);

                  //LIST from Dstore
                  } else if (splitIn[0].equals("LIST") && port != 0) {
                    ArrayList<String> files = new ArrayList<>();
                    try {
                      for (int i = 1 ; i < splitIn.length -1; i++ ) {
                        index.put(splitIn[i], false);
                        files.add(splitIn[i]);
                      }
                    } catch (Exception e) {
                      index.put(splitIn[splitIn.length - 1], false);
                      files.add(splitIn[splitIn.length - 1]);
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
                      while (index.get(fileName)) {}
                    }
                    index.put(fileName, true);
                    float balanceNumber = (R * index.size())/dstores.size();
                    String toSend = "STORE_TO";

                    for (int c = 0; c < R; c++) {
                      for (Integer p : dstores.keySet()) {
                        try {
                          if (fileLocations.get(p).size() < balanceNumber && !fileLocations.get(p).contains(fileName)) {
                            fileLocations.get(p).add(fileName);
                            toSend += " " + p;
                            logger.info("File " + fileName + " added to " + p);
                            break;
                          }
                        } catch (Exception e) {
                          ArrayList<String> file = new ArrayList<>();
                          file.add(fileName);
                          fileLocations.put(p, file);
                          toSend += " " + p;
                          logger.info("File " + fileName + " added to " + p);
                          break;
                        }
                      }
                    }
                    sendMsg(client, toSend);

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
                    logger.info("connection closed");
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
   * Send a text message to the specified target
   * @param destinationAddress The target address
   * @param destinationPort The target port
   * @param msg The message to be sent
   */
  private void sendMsg(Socket socket, String msg) {
    try{
      PrintWriter out = new PrintWriter(socket.getOutputStream());
      out.println(msg); out.flush();
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