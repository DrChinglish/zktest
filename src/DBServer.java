import org.apache.zookeeper.ZooKeeper;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.logging.Logger;

public class DBServer {
    public static DBMonitor monitor;
    public static void main(String[] args) throws Exception {
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
