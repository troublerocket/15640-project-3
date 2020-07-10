import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Cache extends UnicastRemoteObject 
        implements Cloud.DatabaseOps, java.io.Serializable {

    private static final long serialVersionUID = 1L;
    // item map
    private Map<String, String> items;
    // cloud database
    private Cloud.DatabaseOps db;
    
    protected Cache(Cloud.DatabaseOps cloud_db) 
            throws RemoteException {
        items = new ConcurrentHashMap<String,String>();
        db = cloud_db;
        
    }
    
    /* get item info from database */
    @Override
    public String get(String arg0) throws RemoteException {

        if(items.containsKey(arg0) == false){// cache miss
            String info = db.get(arg0);
            // store in the cache
            items.put(arg0, info);
            return info;
        }
        // cache hit
        return items.get(arg0);
    }
    
    /* set item info to database */
    @Override
    public boolean set(String arg0, String arg1, String arg2) 
            throws RemoteException {

        // pass the request to the database
        return db.set(arg0, arg1, arg2);
    }

    /* item purchase transaction */
    @Override
    public boolean transaction(String arg0, float arg1, int arg2) 
            throws RemoteException {

        // pass the request to the database
        return  db.transaction(arg0, arg1, arg2);
    }
}