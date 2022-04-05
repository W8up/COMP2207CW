import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.logging.Logger;

public class Dstore {

  private static final Logger logger = Logger.getLogger(Dstore.class.getName());

  final int port;
  final int cport;
  int timeout;
  String fileFolder;
  ArrayList<String> filesStored;
  Socket toServer;
  File dir;

  public static void main(String[] args) {
    final int port = Integer.valueOf(args[0]);
    final int cport = Integer.valueOf(args[1]);
    int timeout = Integer.valueOf(args[2]);
    String fileFolder = args[3];
    ArrayList<String> filesStored = new ArrayList<>();
    Dstore dstore = new Dstore(port, cport, timeout, fileFolder, filesStored);
  }

  public Dstore(int port, int cport, int timeout, String fileFolder, ArrayList<String> filesStored) {
    this.port = port;
    this.cport = cport;
    this.timeout = timeout;
    this.fileFolder = fileFolder;
    this.filesStored = filesStored;
    this.dir = new File(fileFolder);
    try {
      dir.mkdirs();
    } catch (Exception e) {logger.info("error " + e.getMessage());}

    new Thread(new Runnable(){
      public void run() {
        ServerComms();
      }
    }).start();

    try{
      ServerSocket ss = new ServerSocket(port);
      for(;;){
        try{
          final Socket client = ss.accept();
          logger.info("New connection");
          new Thread(new Runnable(){
            public void run() {
              try{
                BufferedReader in = new BufferedReader(
                new InputStreamReader(client.getInputStream()));
                String line;
                logger.info("msg");
                while((line = in.readLine()) != null) {
                  logger.info(line + " received");
                  String[] splitIn = line.split(" ");
                  if (splitIn[0].equals("STORE")) {
                    String fileName = splitIn[1];
                    int size = Integer.valueOf(splitIn[2]);
                    byte[] fileBuffer = new byte[size];
                    int buflen;
                    File outputFile = new File(dir, fileName);
                    FileOutputStream out = new FileOutputStream(outputFile);
                    InputStream fileInStream = client.getInputStream();
                    logger.info("filename: " + fileName);
                    sendMsg(client, "ACK");
                    while ((buflen=fileInStream.read(fileBuffer)) != -1){
                      System.out.print("*");
                      out.write(fileBuffer,0,buflen);
                    }
                    fileInStream.close();
                    out.close();
                    filesStored.add(fileName);
                    sendMsg(toServer, "STORE_ACK " + fileName);
                  }
                }
              } catch (Exception e) {
                logger.info("Exception caught " + e.getMessage());
              }
            }
          }).start();
        } catch (Exception e) {
          logger.info("Exception caught " + e.getMessage());
        }
      }
    } catch (Exception e) {
      logger.info("Exception caught " + e.getMessage());
    }
  }

  private void ServerComms() {

    try{
      //Sending
      toServer = new Socket(InetAddress.getLocalHost(),cport);
      sendMsg(toServer, "JOIN "+ port);

      //Receiving
      try{
        for(;;){
          try{
            BufferedReader in = new BufferedReader(
            new InputStreamReader(toServer.getInputStream()));
            String line;
            while((line = in.readLine()) != null) {
              logger.info(line+" received");
              String[] splitIn = line.split(" ");
              if (splitIn[0].equals("LIST")) {
                String msgToSend = "LIST";
                for (String file : filesStored) {
                  msgToSend += " " + file;
                }
                sendMsg(toServer, msgToSend);
              }
            }
          }catch(Exception e){logger.info("error "+e);}
        }
      }catch(Exception e){logger.info("error "+e);}
    }catch(Exception e){logger.info("error"+e);}
  }

  private void sendMsg(Socket socket, String msg) {
    try{
      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      out.println(msg);
      logger.info("TCP message "+msg+" sent");

    }catch(Exception e){logger.info("error"+e);}
  }
}