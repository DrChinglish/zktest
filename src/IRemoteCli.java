import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IRemoteCli extends Remote {

    String getTable(String table_name, int method) throws RemoteException;//获取表所在的地址

    String SyncTable(String table_name)throws RemoteException;//获取同步表需要的主副本地址

}
