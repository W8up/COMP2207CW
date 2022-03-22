import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.logging.Logger;

public class Dstore {

  private static final Logger logger = Logger.getLogger(Dstore.class.getName());

  public static void main(String[] args) {
    final int port = Integer.valueOf(args[0]);
    final int cport = Integer.valueOf(args[1]);
    int timeout = Integer.valueOf(args[2]);
    String fileFolder = args[3];
    ArrayList<String> filesStored = new ArrayList<>();

    try{
      //Sending
      Socket socket = new Socket(InetAddress.getLocalHost(),cport);
      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      out.println("JOIN "+ port);
      logger.info("JOIN " + port + " sent");

      //Receiving
      try{
        ServerSocket ss = new ServerSocket(port);
        for(;;){
          try{
            Socket controller = ss.accept();
            BufferedReader in = new BufferedReader(
            new InputStreamReader(controller.getInputStream()));
            String line;
            while((line = in.readLine()) != null) {
              logger.info(line+" received");
              String[] splitIn = line.split(" ");
              if (splitIn[0].equals("LIST")) {
                String msgToSend = "LIST";
                for (String file : filesStored) {
                  msgToSend += " " + file;
                }
                out.println(msgToSend);
                logger.info(msgToSend + " sent");
              }
            }
            controller.close();
          }catch(Exception e){logger.info("error "+e);}
        }
      }catch(Exception e){logger.info("error "+e);}
      Thread.sleep(1000);
    }catch(Exception e){logger.info("error"+e);}
  }
}