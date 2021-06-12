import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class ClientList {
    private ReentrantReadWriteLock lock=new ReentrantReadWriteLock();
    private List<String> Clients;
    private static final int MAX_COPIES = DBServer.MAX_COPIES;
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
        StringBuilder res=new StringBuilder();
        lock.readLock().lock();
        if(!Clients.isEmpty()) {
            Set<Integer> selected=new HashSet<>();
            while(selected.size()<Math.min(MAX_COPIES,Clients.size())){
                selected.add(r.nextInt(Clients.size()));
            }

            for (Integer i:selected
                 ) {
                if(res.length()!=0){
                    res.append(",");
                }
                res.append(Clients.get(i));
            }
            lock.readLock().lock();
            return res.toString();
        }
        else
            return null;
    }
}
