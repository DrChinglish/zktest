import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class ClientList {
    private ReentrantReadWriteLock lock=new ReentrantReadWriteLock();
    private List<String> Clients;

    public ClientList(){
        Clients=new ArrayList<>();
    }

    public void Update(List<String> new_Clients){
        lock.writeLock().lock();
        Clients=new_Clients;
        lock.writeLock().unlock();
    }

    public List<String> GetList(){
        lock.readLock().lock();
        List<String> res= Clients;
        lock.readLock().unlock();
        return res;
    }

    public boolean deleteClient(String ClientName){
        if(Clients.contains(ClientName)){
            lock.writeLock().lock();
            Clients.remove(ClientName);
            lock.writeLock().unlock();
            return true;
        }else {
            return false;
        }
    }

    public String allocateDB(){
        Random r=new Random();
        if(!Clients.isEmpty())
            return Clients.get(r.nextInt(Clients.size()-1));
        else
            return null;
    }
}
