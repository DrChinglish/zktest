import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class DBServer {
    public static DBMonitor monitor;
    public static void main(String[] args)  {
        try {
            monitor=new DBMonitor();
            LocateRegistry.createRegistry(1099);
            Registry registry=LocateRegistry.getRegistry();
            registry.bind("GetTable",monitor.RPCAPI);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
