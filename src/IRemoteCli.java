import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IRemoteCli extends Remote {

    String getTable(String table_name, int method) throws RemoteException;//获取表所在的地址


}
