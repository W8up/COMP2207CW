import java.io.*;
import java.net.*;
import java.util.logging.Logger;

public class Dstore {

  private static final Logger logger = Logger.getLogger(Dstore.class.getName());

  public static void main(String[] args) {
    final int port = Integer.valueOf(args[0]);
    final int cport = Integer.valueOf(args[1]);
    int timeout = Integer.valueOf(args[2]);
    String fileFolder = args[3];

    try{
      //Sending
      Socket socket = new Socket(InetAddress.getLocalHost(),cport);
      PrintWriter out = new PrintWriter(socket.getOutputStream());
      out.println("JOIN "+ port); out.flush();
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
            while((line = in.readLine()) != null)
            logger.info(line+" received");
            controller.close();
          }catch(Exception e){logger.info("error "+e);}
        }
      }catch(Exception e){logger.info("error "+e);}
      Thread.sleep(1000);
    }catch(Exception e){logger.info("error"+e);}
  }
}