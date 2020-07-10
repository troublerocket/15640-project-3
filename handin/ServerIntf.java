import java.util.*;
import java.util.concurrent.*;
import java.rmi.*;

public interface ServerIntf extends Remote {

    ServerLib  selfLib = null;
    Info ServerInfo = null;
    boolean curr_tier = false;
    int VMSecondnum = 0;
    ConcurrentHashMap<Integer,Boolean> VMs = null;

    /* functions for the master server to implement */    
    boolean isFront(int id) throws RemoteException;
    int countServer(boolean front_flag) throws RemoteException;
    Cloud.FrontEndOps.Request getRequest() throws RemoteException;
    void addRequest(Cloud.FrontEndOps.Request r) throws RemoteException;
    boolean deleteServer(int id) throws RemoteException;
    int requestLen() throws RemoteException;
    void dropRequest(boolean dropped) throws RemoteException;
}