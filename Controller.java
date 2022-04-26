import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
  Hashtable<String, CountDownLatch> locksS = new Hashtable<>();
  Hashtable<String, CountDownLatch> locksR = new Hashtable<>();
  Hashtable<String, String> fileSizes = new Hashtable<>();
  Object rebalence = new Object();

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
    final Controller main = this;
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
                  } else {
                    new Thread(new TextRunnable(port, line, main, client) {}).start();
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

  public void textProcessing(String line, int port, Socket client) {
    
    String[] splitIn = line.split(" ");
    String command = splitIn[0];
    switch (command) {
      case "LIST":
        if (port != 0) {
          ArrayList<String> files = new ArrayList<>();
          for (int i = 1 ; i < splitIn.length; i++ ) {
            Boolean p = false;
            try{
              p = index.get(splitIn[i]);
            } catch (Exception e) {}
            
            index.put(splitIn[i], p);
            if (!p) {files.add(splitIn[i]);}
          }
          try {
          fileLocations.remove(port);
          } catch (Exception e) {}

          fileLocations.put(port, files);
          logger.info("Files " + files + " added for " + port);
        } else {
          logger.info("LIST from Client");
          String toSend = "LIST";
          for (String i : index.keySet()) {
            if (!index.get(i)) {
              toSend += " " + i;
            }
          }
          sendMsg(client, toSend);
        }
        break;
      case "STORE":
        String fileName = splitIn[1];
        String fileSize = splitIn[2];
        if (index.get(fileName) != null) {
          sendMsg(client, "ERROR_FILE_ALREADY_EXISTS");
        } else if (dstores.size() < R) {
          sendMsg(client, "ERROR_NOT_ENOUGH_DSTORES");
        } else {
          index.put(fileName, true);
          fileSizes.put(fileName, fileSize);
          float balanceNumber = (R * index.size())/dstores.size();
          String toSend = "STORE_TO";
          ArrayList<Socket> ports = new ArrayList<>();

          for (int c = 0; c < R; c++) {
            for (Integer p : dstores.keySet()) {
              try {
                if (fileLocations.get(p).size() <= Math.ceil(balanceNumber) && fileLocations.get(p).size() <= Math.floor(balanceNumber) && !fileLocations.get(p).contains(fileName)) {
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

          CountDownLatch countDown = new CountDownLatch(R);
          locksS.put(fileName, countDown);
          sendMsg(client, toSend);
          logger.info("Thread paused");
          try {
            if (countDown.await(timeout, TimeUnit.MILLISECONDS)) {
              sendMsg(client, "STORE_COMPLETE");
              index.put(fileName, false);
              locksS.remove(fileName);
              logger.info("Index updated for " + fileName);
            } else {
              logger.info("STORE " + fileName +" timeout " + countDown.getCount());
              index.remove(fileName);
              locksS.remove(fileName);
            }
          } catch (Exception e) {logger.info("error " + e.getMessage());}
        }
        break;
      case "STORE_ACK":
        try {
          locksS.get(splitIn[1]).countDown();
          logger.info("ACK S " + splitIn[1] + " decremented");
        } catch (Exception e) {logger.info("error " + e.getMessage());}
        break;
      case "REMOVE_ACK":
        try {
          locksR.get(splitIn[1]).countDown();
          logger.info("ACK R " + splitIn[1] + " decremented");
        } catch (Exception e) {logger.info("error " + e.getMessage());}
        break;
      case "ERROR_FILE_DOES_NOT_EXIST":
        try {
          locksR.get(splitIn[1]).countDown();
          fileLocations.get(port).remove(splitIn[1]);
          logger.info("ACK R " + splitIn[1] + " decremented file does not exist");
        } catch (Exception e) {logger.info("error " + e.getMessage());}
        break;
      case "LOAD":
      case "RELOAD":
        String fileToLoad = splitIn[1];
        Boolean type = !splitIn[0].equals("RELOAD");
        if (dstores.size() >= R) {
          try {
            if (!index.get(fileToLoad)) {
              for (Integer store : fileLocations.keySet()) {
                if (fileLocations.get(store).contains(fileToLoad)) {
                  if (type) {
                    sendMsg(client, "LOAD_FROM " + store + " " + fileSizes.get(fileToLoad));
                    break;
                  } else {
                    type = true;
                    fileLocations.get(store).remove(fileToLoad);
                  }
                }
                sendMsg(client, "ERROR_LOAD");
              }
              
            } else {sendMsg(client, "FILE_DOES_NOT_EXIST");}
          } catch (NullPointerException e) {
            sendMsg(client, "FILE_DOES_NOT_EXIST");
          }
        } else {
          sendMsg(client, "ERROR_NOT_ENOUGH_DSTORES");
        }
        break;
      case "REMOVE":
        fileName = splitIn[1];
        if (dstores.size() >= R) {
          try {
            if (!index.get(fileName)) {
              CountDownLatch countDown = new CountDownLatch(R);
              locksR.put(fileName, countDown);
              for (Integer p : fileLocations.keySet()) {
                if (fileLocations.get(p).contains(fileName)) {
                  sendMsg(dstores.get(p), "REMOVE " + fileName);
                }
              }
              
              logger.info("Thread paused");
              try {
                if (countDown.await(timeout, TimeUnit.MILLISECONDS)) {
                  sendMsg(client, "REMOVE_COMPLETE");
                  index.remove(fileName);
                  locksR.remove(fileName);
                  logger.info("Index removed for " + fileName);
                } else {
                  logger.info("REMOVE " + fileName +" timeout " + countDown.getCount());
                  locksR.remove(fileName);
                }
              } catch (Exception e) {logger.info("error " + e.getMessage());}
            } else {
              sendMsg(client, "ERROR_FILE_DOES_NOT_EXIST");
            }
          } catch (NullPointerException e) {
            sendMsg(client, "ERROR_FILE_DOES_NOT_EXIST");
          }
        } else {
          sendMsg(client, "ERROR_NOT_ENOUGH_DSTORES");
        }
        break;
      default:
        logger.info("Malformated message");
    }
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
    synchronized (rebalence) {
      logger.info("has Lock");
      for (Integer s : dstores.keySet()) {
        sendMsg(dstores.get(s), "LIST");
      }
      rebalence.notifyAll();
      logger.info("lock done");
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

class TextRunnable implements Runnable {
  public int port;
  public String line;
  public Controller c;
  public Socket client;

  TextRunnable(int port, String line, Controller thread, Socket client) {
    this.port = port;
    this.line = line;
    this.c = thread;
    this.client = client;
  }

  @Override
  public void run() {
    c.textProcessing(line, port, client);
  }
}