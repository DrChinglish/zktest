import java.nio.charset.StandardCharsets;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;



class DBMonitor implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zk = null;
    private static Stat stat = new Stat();
    //zookeeper配置数据存放路径
    private String DBPath = "/DBRoot";
    private String MainCopyPath="/MainCopy";
    private static DBList dbList=new DBList();
    private static MainCopyList MainCopies=new MainCopyList();
    boolean alive=true;
    private static  ClientList clients=new ClientList();
    private String master=null;
    public DBServerRPC RPCAPI;
    private final Watcher DBClientsWatcher =new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            System.out.printf("\nEvent Received: %s", event.toString());
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                try {
                    //Get current list of child znode,
                    //reset the watch
                    List<String> newList=zk.getChildren(DBPath, this);
                    List<String> added = new ArrayList<>(newList);
                    added.removeAll(clients.GetList());
                    for (String newCli:added
                         ) {
                        //set watcher for new client
                        String tables= Arrays.toString(zk.getData(DBPath +"/"+newCli, new DBClientWatcher(newCli), stat));
                        dbList.Set(newCli, Arrays.asList(tables.split(",")));
                    }

                    clients.Update(newList);
                    System.out.println("!!!Cluster Membership Change!!!");
                    System.out.println("Members: " + clients.GetList());
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    alive = false;
                    throw new RuntimeException(e);
                }
            }
        }
    };

    protected DBMonitor() throws Exception{
        RPCAPI=new DBServerRPC();
        //连接zookeeper并且注册一个默认的监听器
        zk = new ZooKeeper("localhost:2181", 5000, //
                this);
        //等待zk连接成功的通知
        connectedSemaphore.await();
        System.out.println("Zookeeper connected.");
        if(zk.exists(DBPath,false)==null){
            zk.create(DBPath,"DBRoot".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        //获取path目录节点的配置数据，并注册默认的监听器
        System.out.println(new String(zk.getData(DBPath, true, stat)));
        zk.exists(MainCopyPath,new DBMasterWatcher());
        clients.Update(zk.getChildren(DBPath, DBClientsWatcher));
        updateDBList();
        Thread.sleep(Integer.MAX_VALUE);
    }

    public void process(WatchedEvent event) {
        if (Event.KeeperState.SyncConnected == event.getState()) {  //zk连接成功通知事件
            if (Event.EventType.None == event.getType() && null == event.getPath()) {
                connectedSemaphore.countDown();
            } else if (event.getType() == Event.EventType.NodeDataChanged) {  //zk目录节点数据变化通知事件
                try {
                    System.out.println("配置已修改，新值为：" + new String(zk.getData(event.getPath(), true, stat)));
                } catch (Exception e) {
                }
            }
        }
    }

    private void updateDBList(){
        Map<String, List<String>> new_list= new HashMap<>() ;
        for (String client:clients.GetList()
             ) {
            try {
                String tables=new String(zk.getData(DBPath,false,new Stat()));
                new_list.replace(client, Arrays.asList(tables.split(",")));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        dbList.Update(new_list);
    }

    public class DBServerRPC extends UnicastRemoteObject implements IRemoteCli{
        protected DBServerRPC()throws RemoteException{

        }
        public String getTable(String table_name,int method) throws RemoteException {//0:read 1:modify 2:create 3:drop
            switch(method){
                case 0: return dbList.lookupTable(table_name);
                case 1: case 3: return master+","+dbList.lookupTable(table_name);
                case 2: return master+","+clients.allocateDB();
                default: return null;
            }
        }
    }



    private class  DBClientWatcher implements Watcher{//为每一个连接客户端创建的watcher
        private String ClientName;
        private Stat cliStat=new Stat();
        public DBClientWatcher(String ClientName){
            this.ClientName=ClientName;
        }
        @Override
        public void process(WatchedEvent event) {
            System.out.printf("\nEvent Received: %s", event.toString());
            if (event.getType() == Event.EventType.NodeDataChanged) {
                try {
                    //Get current tables of this DBClient,
                    //reset the watch
                    String tables= Arrays.toString(zk.getData(DBPath + "/" + ClientName, this, cliStat));
                    dbList.Set(ClientName, Arrays.asList(tables.split(",")));
                    System.out.println("!!!DBClient tables changed!!!");
                    System.out.println("Current table: " + dbList.GetTables(ClientName));
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    alive = false;
                    throw new RuntimeException(e);
                }
            }else if(event.getType() == Event.EventType.NodeDeleted){
                //Delete Client info
                //reset the watch
                if(!(dbList.deleteClient(ClientName)|| clients.deleteClient(ClientName))){
                    System.err.println("Cannot delete client:"+ClientName);
                }
                dbList.deleteClient(ClientName);
                clients.deleteClient(ClientName);
                System.out.println("!!!DBClient deleted!!!");
                System.out.println("Client quit: " + ClientName);
            }
        }
    }

    private class  DBMasterWatcher implements Watcher{//watcher for main copy path
        private Stat masterStat;
        public DBMasterWatcher(){
            masterStat=new Stat();
        }
        @Override
        public void process(WatchedEvent event) {
            System.out.printf("\nEvent Received: %s", event.toString());
            if (event.getType() == Event.EventType.NodeChildrenChanged){
                try {
                    List<String> newList=zk.getChildren(MainCopyPath, this);
                    List<String> added = new ArrayList<>(newList);
                    added.removeAll(MainCopies.GetTables());
                    for (String newCopy:added
                    ) {
                        //set watcher for new client
                        String hostname= Arrays.toString(zk.getData(MainCopyPath +"/"+newCopy, new DBMainCopyWatcher(newCopy), masterStat));
                        MainCopies.Set(newCopy, hostname);
                    }
                    if(!added.isEmpty()) {
                        System.out.println("!!!New MainCopy Assigned!!!");
                        System.out.println("New Tables: " + added);
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }else if (event.getType() == Event.EventType.NodeCreated){
                try {
                    master= Arrays.toString(zk.getData(MainCopyPath, this, masterStat));
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class  DBMainCopyWatcher implements Watcher{//watcher for each main copy
        private String table_name;
        public DBMainCopyWatcher(String table_name){
            this.table_name=table_name;
        }
        @Override
        public void process(WatchedEvent event) {
            System.out.printf("\nEvent Received: %s", event.toString());
            if (event.getType() == Event.EventType.NodeDeleted){
                    MainCopies.deleteClient(table_name);
            }
        }
    }

}

