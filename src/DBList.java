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

    public Map<String, Integer> findWeakDB(){
        Map<String, Integer> weakDB=new HashMap<>();
        Map<String,Integer> TableCount= new HashMap<>();
        lock.readLock().lock();
        for(Map.Entry<String,List<String>> client:DBMap.entrySet()){
            for(String table:client.getValue()){
                if(TableCount.containsKey(table)){
                    TableCount.computeIfPresent(table,(key,y)->y+1);
                }else{
                    TableCount.put(table,1);
                }
            }
        }
        lock.readLock().unlock();
        for(Map.Entry<String,Integer> table:TableCount.entrySet()){
            if(table.getValue()<=Math.min(DBServer.MAX_COPIES,DBMap.size())){
                weakDB.put(table.getKey(),Math.min(DBServer.MAX_COPIES,DBMap.size())-table.getValue());
            }
        }
        return weakDB;
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

    public String lookupTable(String table){//randomly returns a DB for read
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

    public Set<String> allocateBackupDB(String table,int num){
        Random r=new Random();
        lock.readLock().lock();
        Set<String> selected= new HashSet<>();
        List<String> candidate=new ArrayList<>();
        for (Map.Entry<String,List<String>> client:DBMap.entrySet()
        ) {
            if(!client.getValue().contains(table)){
                candidate.add(client.getKey());
            }
        }
        while(selected.size()<num){
            selected.add(candidate.get(r.nextInt(candidate.size())));
        }
        lock.readLock().unlock();
        return selected;
    }

}
