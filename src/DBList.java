import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBList {
    private ReentrantReadWriteLock lock=new ReentrantReadWriteLock();
    private Map<String, List<String>> DBMap;

    public  DBList(){
        DBMap=new HashMap<>();
    }

    public void Update(Map<String,List<String>> new_DBMap){
        lock.writeLock().lock();
        DBMap=new_DBMap;
        lock.writeLock().unlock();
    }
    public void Set(String db,List<String> tables){
        lock.writeLock().lock();
        DBMap.put(db,tables);
        lock.writeLock().unlock();
    }
    public Map<String,List<String>> GetList(){
        lock.readLock().lock();
        Map<String,List<String>> res= DBMap;
        lock.readLock().unlock();
        return res;
    }

    public List<String> GetTables(String ClientName) {
        List<String> res = new ArrayList<>();
        if (DBMap.containsKey(ClientName)) {
            lock.readLock().lock();
            res = DBMap.get(ClientName);
            lock.readLock().unlock();
        }
        return res;
    }

    public boolean deleteClient(String ClientName){
        if (DBMap.containsKey(ClientName)){
            lock.writeLock().lock();
            DBMap.remove(ClientName);
            lock.writeLock().unlock();
            return true;
        }else{
            return false;
        }
    }

    public String lookupTable(String table){
        Random r=new Random();
        List<String> clients= new ArrayList<>();
        lock.readLock().lock();
        for (Map.Entry<String,List<String>> client:DBMap.entrySet()
             ) {
                if(client.getValue().contains(table)){
                    clients.add(client.getKey());
                }
        }
        lock.readLock().unlock();
        if(clients.size() > 0)
            return clients.get(r.nextInt(clients.size()));
        else
            return null;
    }


}
