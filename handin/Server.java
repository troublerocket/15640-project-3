import java.util.*;
import java.util.concurrent.*;
import java.rmi.*;
import java.rmi.server.*;

public class Server extends UnicastRemoteObject implements ServerIntf {
  
    private static final long serialVersionUID = 1L;
    public static String ip;
    public static int port;
    public static Server self;
    public static Info ServerInfo = new Info();
    public static ServerLib SL;
    public static Cloud.DatabaseOps cache;
    
    // master's server list
    public static ConcurrentHashMap<Integer,Boolean> ServerList;
    // master's request queue
    public static LinkedBlockingQueue<Cloud.FrontEndOps.Request> RequestQueue;
    // number of front servers
    public static int front_count = 0;
    // number of middle servers
    public static int mid_count = 0;
    // the tier of the current server
    public static boolean curr_tier = false;
    

    /* benchmark values for scaling in and scaling out */
    public static int initial_drop = 0;// drops during booting
    public static int request_drop = 0; // drops during running
    public static boolean booting = true;
    // front tier scale out paramater
    public final static int front_scale = 5;
    // middle tier scale out parameter
    public final static int mid_scale = 3;
    // middle tier scale out parameter during booting
    public final static int mid_initial = 5;
    // middle tier scale in parameter
    public final static int mid_idle = 3;
    public static int idle_count = 0;
    public final static int mid_shut = 600;
    public static ServerIntf master;
    
     
    protected Server() throws RemoteException {
        super();
    }

    /**
     * get the required server instance by the id from the RMI
     * 
     * @param ip the cloud ip
     * @param port the cloud port number
     * @param id the server id
     * @return the server interface from the RMI
     */
    
    public static ServerIntf getServer(String ip, int port, String id){

        try{
            ServerIntf server = (ServerIntf) Naming.lookup
            (String.format("//%s:%d/%s", ip, port, id));
            return server;

        }catch (Exception e){

            System.err.println(e);
            return null;
        }
    }
     

    /**
     * unregister a slave server from RMI when scaling in 
     * 
     * @param id the server id
     * @return true if succeeded, otherwise return false
     */
    
    public static boolean unregisterSlave(String id){
        try{

            Naming.unbind(String.format("//%s:%d/%s", ip, port, id));
            return true;

        }catch (Exception e){

            System.err.println(e);
            return false;
        }
    }
     

    /**
     * get the cache of the cloud database
     * 
     * @param ip the cloud ip
     * @param port the cloud port number
     * @return the cache instance
     */
    
    public static  Cloud.DatabaseOps getCache(String ip, int port){
        try{
            Cloud.DatabaseOps cache = (Cloud.DatabaseOps)Naming.lookup
                    (String.format("//%s:%d/%s", ip, port, "Ca"));
            return cache;

        }catch (Exception e){

            System.err.println(e);
            return null;
        }
    }
     

    /**
     * register the cache to RMI
     * 
     * @param ip the cloud ip
     * @param port the cloud port number
     * @param db the cloud database
     * @return true if succeeded, otherwise return false
     * @throws RemoteException
     */

    public static  boolean registerCache(String ip, 
        int port, Cloud.DatabaseOps db) throws RemoteException {
        try{

            cache = new Cache(db);    
            Naming.bind(String.format("//%s:%d/%s", ip, port, "Ca"), cache);        
            return true;

        }catch (Exception e){
            System.err.println(e);
            return false;
        } 
    }


    /**
     * register the master server to RMI
     * 
     * @param ip the cloud ip
     * @param port the cloud port number
     * @return true if succeeded, otherwise return false
     * @throws RemoteException
     */

    public static  boolean registerMaster(String ip, int port)
        throws RemoteException {
        try{

            Server master = new Server();    
            ServerList = new ConcurrentHashMap<Integer,Boolean>();
            RequestQueue = new LinkedBlockingQueue<Cloud.FrontEndOps.Request>();
            Naming.bind(String.format
                ("//%s:%d/%s", ip, port, "Ma"), master);
            return true;

        }catch (Exception e){
            return false;
        } 
    }
     

    /**
     * shut down the required server by the id
     * 
     * @param id the server id
     */

    public static void shutDown(int id){
        try{
            
            SL.interruptGetNext();
            // unregister from ServerLib if it is a front server
            if(ServerInfo.front_flag == true){
                SL.unregister_frontend();
            }
            
            // delete from master server's list
            master.deleteServer(id);
                
            // unregister from RMI
            unregisterSlave(Integer.toString(id));

            SL.shutDown();
            SL.endVM(id);

        }catch (Exception e){
            System.err.println(e);
        }
    }
     

    /**
     * register a slave server to RMI
     * 
     * @param ip the cloud ip
     * @param port the cloud port number
     * @param id the server id
     * @return true if succeeded, otherwise return false
     * @throws RemoteException
     */

    public static boolean registerSlave(String ip, int port, int id)
        throws RemoteException {
        try{

            Server slave = new Server();        
            Naming.bind(String.format("//%s:%d/%d", ip, port, id), slave);
            return true;

        }catch (Exception e){
            return false;
        } 
    }
     

    /**
     * remove the required server from master server's list by the id
     * 
     * @param id the server id
     * @return true if succeeded, otherwise return false
     */

    public boolean deleteServer(int id){
        
        if(ServerList.containsKey(id) == false){// wrong server id
            return false;
        }

        boolean front_flag = ServerList.get(id);
        ServerList.remove(id);
        
        if(front_flag) 
            front_count--;
        else 
            mid_count--;
        return true;
    }
     

    /**
     * add a request to master server's request queue
     * 
     * @param r the request instance
     */

    public  void addRequest(Cloud.FrontEndOps.Request r){
        RequestQueue.add(r);
    }
     

    /**
     * poll a request from master server's request queue
     * 
     * @return the request instance
     */

    public  Cloud.FrontEndOps.Request getRequest(){
        try{

            return RequestQueue.poll(mid_shut,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {

            e.printStackTrace();
        }
        return null;
    }
     

    /**
     * get the size of master server's request queue
     * 
     * @return the request queue length
     */

    public int requestLen(){
        return RequestQueue.size();
    }


    /**
     * count the number of servers in the front or middle tier
     * 
     * @param front_flag the tier to be counted
     * @return the number of servers in the required tier
     */

    public int countServer(boolean front_flag){

        if(front_flag) 
            return front_count;
        return mid_count;
    }
     

    /**
     * count the dropped requests and scale out if necessary
     * 
     * @param dropped if a request has been dropped
     */

    public synchronized void dropRequest(boolean dropped){
        if(dropped){
            request_drop++;
            if(request_drop == mid_scale){// if enough drops
                // scale out middle tier
                scaleout(false);
                // reset drop count
                request_drop = 0;
            }
        }else{
            request_drop = 0;
        }
    }
     

    /**
     * check which tier the required server belongs to  
     * 
     * @param id the server id
     * @return true if it is a front-tier server, otherwise return false
     */

    public boolean isFront(int id){
        return ServerList.get(id);
    }
     
     
    /**
     * master server adds a request to its request queue
     * 
     * @param r the request instance
     */

    public static  void masterAdd(Cloud.FrontEndOps.Request r){
        RequestQueue.add(r);
    }
    

    /**
     * add the required server to the master's server list
     * 
     * @param id the server id
     * @param front_flag the tier of the server
     * @return the server id if successful, return -1 if duplicated id
     */

    public static int addServer(int id, boolean front_flag){

        if(ServerList.containsKey(id) == false){
            ServerList.put(id, front_flag);
            return id;
        }
        return -1;
    }
     

    /**
     * scale out the required tier
     * 
     * @param front_flag the tier to be scaled out
     */

    public static synchronized void scaleout(boolean front_flag){

        curr_tier =  front_flag;
        // scale out one new server
        int id = SL.startVM();
        // add the new server to the master's list
        addServer(id,front_flag);
        // recount the server numbers
        countTier();
    }
     
    /**
     * count the number of servers seperately for each tier
     * 
     */

    public synchronized static void countTier(){
        
        if(curr_tier)
            front_count++;
        else 
            mid_count++;
    }

    public static void main (String args[]) throws Exception {
        
        if (args.length != 3) 
            throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");

        
        SL = new ServerLib(args[0], Integer.parseInt(args[1]));
        ip = args[0];
        port = Integer.parseInt(args[1]);
        ServerInfo.id = Integer.parseInt(args[2]);
                
        // reigister the master server
        if(registerMaster(ip, port) == true){
            
            // register cache
            Cloud.DatabaseOps db = SL.getDB();
            registerCache(ip, port, db);
            
            // register master
            ServerInfo.master_flag = true;                
            SL.register_frontend();
            addServer(1, true);
            front_count++;
            
            // initial scaling one middle server
            scaleout(false);
            // initial scaling one more front server
            scaleout(true);
        }

        // register a front/middle server
        else{

            ServerInfo.master_flag = false;
            master = getServer(ip, port, "Ma");
            ServerInfo.front_flag = master.isFront(ServerInfo.id);
            registerSlave(ip,port,ServerInfo.id);

            if(ServerInfo.front_flag) 
                SL.register_frontend();

            cache = getCache(ip, port);
        }

        while(true){
            try{
                /* master server operations */
                if(ServerInfo.master_flag == true){

                    if(booting){// booting time

                        if(SL.getStatusVM(2) 
                        != Cloud.CloudOps.VMStatus.Running){// still booting
                            
                            if(SL.getQueueLength() > 0) {
                                // drop requests while booting
                                SL.dropHead();
                                initial_drop++;
                                
                                // middle tier scale out
                                if(initial_drop % mid_initial == 0){
                                    scaleout(false);
                                }
                            }
                            continue;
                        }
                        // booting finishes
                        booting = false;

                    }else {// serving time
                                        
                        Cloud.FrontEndOps.Request r = SL.getNextRequest();
                        masterAdd(r);
                    }
                
                    int queuesize = SL.getQueueLength(); 
                    // front tier scale out                  
                    if(queuesize > front_scale * (front_count)){
                        scaleout(true);
                    }
                }

                /* front server operations */
                else if(ServerInfo.front_flag == true){

                    Cloud.FrontEndOps.Request r = SL.getNextRequest();
                    master.addRequest(r);                    
                    
                /* middle server operations */
                }else{
                    
                    int mid = master.countServer(false);
                    // middle tier scale in
                    if(idle_count >= mid_idle && mid > 2) {
                        shutDown(ServerInfo.id);
                    }
                    
                    int requestsize = master.requestLen();
                    Cloud.FrontEndOps.Request r = master.getRequest();

                    if(r != null){// middle server working
                        
                        if(requestsize > mid){// drop requests if busy

                            SL.drop(r);                        
                            master.dropRequest(true);

                        }else{// process requests if normal

                            SL.processRequest(r,cache);                            
                            master.dropRequest(false);
                        }
                        idle_count = 0; 

                    }else{// middle server idle
                        idle_count++;
                        }
                    }                
            }catch (Exception e){}
        }
    }
}

/* server informations */
class Info{
    // server id
    int id = 0;
    // master server flag
    boolean master_flag = false;
    // front server flag
    boolean front_flag = false;

}