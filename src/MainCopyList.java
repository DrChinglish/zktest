import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MainCopyList {
    private ReentrantReadWriteLock lock=new ReentrantReadWriteLock();
    private Map<String, String> MainCopies;

    public  MainCopyList(){
        MainCopies =new HashMap<>();
    }

    public void Update(Map<String, String> new_list){
        lock.writeLock().lock();
        MainCopies =new_list;
        lock.writeLock().unlock();
    }
    public void Set(String table,String host){
        lock.writeLock().lock();
        MainCopies.put(table,host);
        lock.writeLock().unlock();
    }
    public Map<String,String> GetList(){
        lock.readLock().lock();
        Map<String,String> res= MainCopies;
        lock.readLock().unlock();
        return res;
    }

    public Set<String> GetTables(){
        lock.readLock().lock();
        Set<String> res= MainCopies.keySet();
        lock.readLock().unlock();
        return res;
    }

    public boolean deleteClient(String table_name){
        if (MainCopies.containsKey(table_name)){
            lock.writeLock().lock();
            MainCopies.remove(table_name);
            lock.writeLock().unlock();
            return true;
        }else{
            return false;
        }
    }

    public String lookupMainCopy(String table){
        if (MainCopies.containsKey(table)){
            lock.readLock().lock();
            String res=MainCopies.get(table);
            System.out.println(res);
            lock.readLock().unlock();
            return res;
        }
            return null;
    }


}
