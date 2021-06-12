import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class DBServer {
    public static DBMonitor monitor;
    public static void main(String[] args)  {
        try {
            monitor=new DBMonitor();
            System.out.println("Initiating RMI APIs...");
            LocateRegistry.createRegistry(1099);
            Registry registry=LocateRegistry.getRegistry();
            registry.bind("GetTable",monitor.RPCAPI);
            System.out.println("RMI APIs ready.");
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
