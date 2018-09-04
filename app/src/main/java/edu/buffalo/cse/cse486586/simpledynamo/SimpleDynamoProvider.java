package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    BlockingQueue<String> stateblo=new ArrayBlockingQueue<String>(1);
    BlockingQueue<String> queblo=new ArrayBlockingQueue<String>(5);
    ArrayList<Ports> prtlst=new ArrayList<Ports>();
    ArrayList<Node> lst=new ArrayList<Node>();
    Uri uriobject=buildUri("content","edu.buffalo.cse.cse486586.simpledynamo.provider");
    String myPort;
    int createcontcount=0;
    int queindiloop=1;
    int quetransac=0;
    int transacsleep=0;
    int startcounter=0;
    int failcounter=0;
    int stateblofailcounter=0;
    int questarcounter=0;
    int delinfiloop=0;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        String comp=null;
        try {
            comp=genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Node nobj=null;
        Iterator iter=lst.iterator();//Finding node object where the key should be inserted
        while(iter.hasNext()){
            nobj= (Node) iter.next();
            if((nobj.hid.compareTo(lst.get(0).hid)==0 && (comp.compareTo(lst.get(0).hid)<=0 || comp.compareTo(lst.get(lst.size()-1).hid)>0)) || (comp.compareTo(nobj.hid)<=0 && comp.compareTo(nobj.pred1)>0)){
                break;
            }
        }
        Log.d("test","Delete: found node where the key resides, node.id: "+nobj.id+" node.hashid: "+nobj.hid);
        if(!(selection.compareTo("@")==0 || selection.compareTo("*")==0)) {
            if ((nobj.id.compareTo(myPort) == 0) || (nobj.suc1id.compareTo(myPort) == 0) || (nobj.suc2id.compareTo(myPort) == 0)) {
                if (nobj.id.compareTo(myPort) == 0) {
                    Log.d("test","Delete: This node is where the delete was called and key resides for key: "+selection);
                    boolean del = getContext().deleteFile(selection);
                    if (delinfiloop == 0) {//To get out of logical infinite loop
                        String port = nobj.suc1id + ";" + nobj.suc2id;
                        String msg = "delete;1;" + selection + "\n";
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);//Write to suc1 and suc2
                    }
                    delinfiloop = 0;
                } else if (nobj.suc1id.compareTo(myPort) == 0) {
                    Log.d("test","Delete: This node is suc1 where the delete was called and key resides for key: "+selection);
                    boolean del = getContext().deleteFile(selection);
                    if (delinfiloop == 0) {//To get out of logical infinite loop
                        String port = nobj.id + ";" + nobj.suc2id;
                        String msg = "delete;1;" + selection + "\n";
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);//Write to suc1 and suc2
                    }
                    delinfiloop = 0;
                } else if (nobj.suc2id.compareTo(myPort) == 0) {
                    Log.d("test","Delete: This node is suc2 where the delete was called and key resides for key: "+selection);
                    boolean del = getContext().deleteFile(selection);
                    if (delinfiloop == 0) {//To get out of logical infinite loop
                        String port = nobj.id + ";" + nobj.suc1id;
                        String msg = "delete;1;" + selection + "\n";
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);//Write to suc1 and suc2
                    }
                    delinfiloop = 0;
                }
            } else {
                Log.d("test","Delete operation called but this avd is not related, key: "+selection);
                String port = nobj.id + ";" + nobj.suc1id + ";" + nobj.suc2id;
                String msg = "delete;1;" + selection + "\n";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);//Write to original node, suc1 and suc2
            }
        }
        else if(selection.compareTo("@")==0){
            Log.d("test","Delete @ called in avd: "+myPort);
            String[] file = getContext().fileList();
            for (int i = 0; i < file.length; i++) {
                boolean del = getContext().deleteFile(file[i]);
            }
        }
        else if(selection.compareTo("*")==0){
            Log.d("test","Delete * called in avd: "+myPort);
            String port="11108;11112;11116;11120;11124";
            String msg="delete;@";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);//Write to all nodes
        }

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

        synchronized (this) {
            String key = values.getAsString("key");
            String value = values.getAsString("value");
            String[] val = value.split(";");

            if (val.length != 1) {
                try {
                    FileOutputStream out = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    SimpleDateFormat sdf = new SimpleDateFormat("HH:MM:ss:SSS");
                    Date now = new Date();
                    String str = sdf.format(now);
                    String towrite = val[0] + ";" + str;
                    out.write(towrite.getBytes());
                    out.close();
                    Log.d("test", "Insert: Written in current avd: " + myPort + " , key: " + values.getAsString("key"));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {

                String comp = null;
                try {
                    comp = genHash(values.getAsString("key"));
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }

                Node nobj = null;
                Iterator iter = lst.iterator();//Finding node object where the key should be inserted
                while (iter.hasNext()) {
                    nobj = (Node) iter.next();
                    if ((nobj.hid.compareTo(lst.get(0).hid) == 0 && (comp.compareTo(lst.get(0).hid) <= 0 || comp.compareTo(lst.get(lst.size() - 1).hid) > 0)) || (comp.compareTo(nobj.hid) <= 0 && comp.compareTo(nobj.pred1) > 0)) {
                        break;
                    }
                }
                Log.d("test", "Insert: found node where the key resides, node.id: " + nobj.id + " node.hashid: " + nobj.hid + " for key: " + values.getAsString("key"));
                if (nobj.id.compareTo(myPort) == 0 || nobj.suc1id.compareTo(myPort) == 0 || nobj.suc2id.compareTo(myPort) == 0) { //If current avd is either found node for suc1 or suc2 to it
                    Log.d("test", "Insert: Entering first if condition in insert with key: " + values.getAsString("key"));

                    if (nobj.id.compareTo(myPort) == 0) {//if current avd is node where it should be inserted
                        Log.d("test", "Insert: Inserting in nobj.id avd for key: " + values.getAsString("key"));
                        try {
                            FileOutputStream out = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            SimpleDateFormat sdf = new SimpleDateFormat("HH:MM:ss:SSS");
                            Date now = new Date();
                            String str = sdf.format(now);
                            String towrite = value + ";" + str;
                            out.write(towrite.getBytes());
                            out.close();
                            Log.d("test", "Insert: Written in current avd: " + myPort + " , key: " + values.getAsString("key"));
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        Log.d("test", "Insert: Entering if condition for future broadcast, key: " + values.getAsString("key"));
                        String port = nobj.suc1id + ";" + nobj.suc2id;
                        String msg = "insert;1;" + values.getAsString("key") + ";" + values.getAsString("value") + "\n";
                        Log.d("test", "Insert: Writing to ClientTask with: " + msg + " to port: " + port);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);//Write to suc1 and suc2
                        Log.d("test", "Insert: Successfully broadcasted");
                    } else if (nobj.suc1id.compareTo(myPort) == 0) {//if current avd is suc1 where it should be inserted
                        Log.d("test", "Insert: Inserting in nobj.suc1id avd for key: " + values.getAsString("key"));
                        try {
                            FileOutputStream out = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            SimpleDateFormat sdf = new SimpleDateFormat("HH:MM:ss:SSS");
                            Date now = new Date();
                            String str = sdf.format(now);
                            String towrite = value + ";" + str;
                            out.write(towrite.getBytes());
                            out.close();
                            Log.d("test", "Insert: Written in current avd: " + myPort + " , key: " + values.getAsString("key"));
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        Log.d("test", "Insert: Entering if condition for future broadcast, key: " + values.getAsString("key"));
                        String port = nobj.id + ";" + nobj.suc2id;
                        String msg = "insert;1;" + values.getAsString("key") + ";" + values.getAsString("value") + "\n";
                        Log.d("test", "Insert: Writing to ClientTask with: " + msg + " to port: " + port);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);//Write to original node and suc2
                        Log.d("test", "Insert: Successfully broadcasted");
                    } else if (nobj.suc2id.compareTo(myPort) == 0) {//if current avd is suc2 where it should be inserted
                        Log.d("test", "Insert: Inserting in nobj.suc2id avd for key: " + values.getAsString("key"));
                        try {
                            FileOutputStream out = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            SimpleDateFormat sdf = new SimpleDateFormat("HH:MM:ss:SSS");
                            Date now = new Date();
                            String str = sdf.format(now);
                            String towrite = value + ";" + str;
                            out.write(towrite.getBytes());
                            out.close();
                            Log.d("test", "Insert: Written in current avd: " + myPort + " , key: " + values.getAsString("key"));
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        Log.d("test", "Insert: Entering if condition for future broadcast, key: " + values.getAsString("key"));
                        String port = nobj.id + ";" + nobj.suc1id;
                        String msg = "insert;1;" + values.getAsString("key") + ";" + values.getAsString("value") + "\n";
                        Log.d("test", "Insert: Writing to ClientTask with: " + msg + " to port: " + port);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);//Write to original node and suc1
                        Log.d("test", "Insert: Successfully broadcasted");
                    }
                } else {//If current avd is not where it should be inserted at all
                    Log.d("test", "Insert: Broadcasting to all avds, key: " + values.getAsString("key"));
                    String port = nobj.id + ";" + nobj.suc1id + ";" + nobj.suc2id;
                    String msg = "insert;1;" + values.getAsString("key") + ";" + values.getAsString("value") + "\n";
                    Log.d("test", "Insert: Writing to ClientTask with: " + msg + " to port: " + port);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);//Write to original node, suc1 and suc2
                    Log.d("test", "Insert: Successfully broadcasted");
                }
            }
        }
        return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);//Calling ServerTask
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(int i=0;i<REMOTE_PORT.length;i++){
            Ports temp=new Ports();
            temp.prt=REMOTE_PORT[i];
            try {
                temp.hashprt = genHash(String.valueOf(Integer.parseInt(temp.prt)/2));//Calculation hashed is of portStr
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            prtlst.add(temp);//Add the avd i.e. node to port list

        }
        Collections.sort(prtlst);

        for(int i=0;i<5;i++){//Establishing links between nodes
            Node nobj=new Node();
            nobj.id= prtlst.get(i).prt;
            nobj.hid=prtlst.get(i).hashprt;
            if(i==0){
                nobj.pred1=prtlst.get(4).hashprt;
                nobj.pred2=prtlst.get(3).hashprt;
                nobj.pred1id=prtlst.get(4).prt;
                nobj.pred2id=prtlst.get(3).prt;
            }
            else if(i==1){
                nobj.pred1=prtlst.get(0).hashprt;
                nobj.pred2=prtlst.get(4).hashprt;
                nobj.pred1id=prtlst.get(0).prt;
                nobj.pred2id=prtlst.get(4).prt;
            }
            else{
                nobj.pred1=prtlst.get(i-1).hashprt;
                nobj.pred2=prtlst.get(i-2).hashprt;
                nobj.pred1id=prtlst.get(i-1).prt;
                nobj.pred2id=prtlst.get(i-2).prt;
            }
            if(i==3){
                nobj.suc1=prtlst.get(4).hashprt;
                nobj.suc2=prtlst.get(0).hashprt;
                nobj.suc1id=prtlst.get(4).prt;
                nobj.suc2id=prtlst.get(0).prt;
            }
            else if(i==4){
                nobj.suc1=prtlst.get(0).hashprt;
                nobj.suc2=prtlst.get(1).hashprt;
                nobj.suc1id=prtlst.get(0).prt;
                nobj.suc2id=prtlst.get(1).prt;
            }
            else{
                nobj.suc1=prtlst.get(i+1).hashprt;
                nobj.suc2=prtlst.get(i+2).hashprt;
                nobj.suc1id=prtlst.get(i+1).prt;
                nobj.suc2id=prtlst.get(i+2).prt;
            }
            lst.add(nobj);
        }

        for(int i=0;i<5;i++){
            Log.d("test","Prts list: "+i+" prt: "+prtlst.get(i).prt+" hashprt: "+prtlst.get(i).hashprt);
        }

        for(int i=0;i<5;i++){
            Log.d("test","ArrayList: "+i+" suc1: "+lst.get(i).suc1+" suc2: "+lst.get(i).suc2+" pred1: "+lst.get(i).pred1+" pred2: "+lst.get(i).pred2+" id: "+lst.get(i).id+" hid: "+lst.get(i).hid);
        }
        String msg="check;"+myPort+"\n";
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);//Checking for failure detection
        String further=null;
        try {
            further=stateblo.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if(further.compareTo("1")==0){//Failure detected
            Log.d("test","Blocking queue in OnCreate() (inside if - fault detected): "+further);
            delete(uriobject,"@",null);
            String temp="";
            for(int i=0;i<REMOTE_PORT.length;i++){
                if(REMOTE_PORT[i].compareTo(myPort)==0){
                    continue;
                }
                temp+=REMOTE_PORT[i]+";";
            }
            msg="duplicate;"+myPort+"\n";
            String port=temp.substring(0,temp.length()-1);
            Log.d("test","Oncreate: Writing to ClientTask in failure: "+msg+" to port: "+port);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
            while(createcontcount==0){
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        Log.d("test","Blocking queue in OnCreate() (inside else - general): "+further+" startcounter: "+startcounter);
        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
	    Log.d("test", "Query: entry");
	    // TODO Auto-generated method stub
        String[] split = selection.split(";");
        String[] head = {"key", "value"};
        MatrixCursor cur = new MatrixCursor(head);
        synchronized (this) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
            if (split.length == 2 && split[0].contains("check")) {//Failure detection check
                String[] file = getContext().fileList();
                Log.d("test", "Query: found file list size: " + file.length);
                String port = split[1];
                if (file.length == 0) {
                    String msg = "chfound;0" + "\n";
                    Log.d("test", "Query: before writing chfound to ClientTask, msg: " + msg + " port: " + port);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
                } else {
                    String msg = "chfound;1" + "\n";
                    Log.d("test", "Query: before writing chfound to ClientTask, msg: " + msg + " port: " + port);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
                }
                return null;
            } else if (split.length == 1 && !(split[0].compareTo("@") == 0 || split[0].compareTo("*") == 0)) {//Original avd where query was called
                Log.d("test", "Query: Original node where individual query was called: " + myPort + " for key: " + split[0]);
                String comp = null;
                try {
                    comp = genHash(split[0]);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                Node nobj = null;
                Iterator iter = lst.iterator();//Finding node where the query should transfer control to
                while (iter.hasNext()) {
                    nobj = (Node) iter.next();
                    if ((nobj.hid.compareTo(lst.get(0).hid) == 0 && (comp.compareTo(lst.get(0).hid) <= 0 || comp.compareTo(lst.get(lst.size() - 1).hid) > 0)) || (comp.compareTo(nobj.hid) <= 0 && comp.compareTo(nobj.pred1) > 0)) {
                        break;
                    }
                }
                Log.d("test", "Query: found node where the key resides, node.id: " + nobj.id + " node.hashid: " + nobj.hid);
                if ((nobj.id.compareTo(myPort) == 0) || (nobj.suc1id.compareTo(myPort) == 0) || (nobj.suc2id.compareTo(myPort) == 0)) {
                    FileInputStream in;
                    String input = "";
                    try {
                        StringBuilder bui = new StringBuilder();
                        in = getContext().openFileInput(split[0]);
                        int eof;
                        while ((eof = in.read()) != -1) {
                            bui.append((char) eof);
                        }
                        input = bui.toString();
                        in.close();
                    } catch (FileNotFoundException e) {
                        input = "0000;00:00:00.000";
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    transacsleep += 10;
                    while (quetransac == 1) {
                        try {
                            Thread.sleep(transacsleep);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    if (transacsleep > 500) {
                        transacsleep = 0;
                    }
                    if (nobj.id.compareTo(myPort) == 0) {
                        Log.d("test", "Query: Single query, key originally inserted in my avd");

                        String port = nobj.suc1id + ";" + nobj.suc2id;
                        String msg = "query;0;" + split[0] + ";" + myPort + "\n";
                        Log.d("test", "Query: Writing to ClientTask: " + msg + " to port: " + port);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);

                    } else if (nobj.suc1id.compareTo(myPort) == 0) {
                        Log.d("test", "Query: Single query, I am suc1 avd");
                        String port = nobj.id + ";" + nobj.suc2id;
                        String msg = "query;0;" + split[0] + ";" + myPort + "\n";
                        Log.d("test", "Query: Writing to ClientTask: " + msg + " to port: " + port);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
                    } else if (nobj.suc2id.compareTo(myPort) == 0) {
                        Log.d("test", "Query: Single query, I am suc2 avd");
                        String port = nobj.id + ";" + nobj.suc1id;
                        String msg = "query;0;" + split[0] + ";" + myPort + "\n";
                        Log.d("test", "Query: Writing to ClientTask: " + msg + " to port: " + port);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
                    }
                    String questamp = null;
                    String[][] values = new String[3][2];
                    values[0] = input.split(";");
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    int counter = 0;

                    for (int i = 0; i < 2; i++) {
                        try {
                            questamp = queblo.take();//Block till message is received
                            values[i + 1] = questamp.split(";");//Split the time stamp from the value
                            Log.d("test", "Query: take splitted value: " + values[i + 1][0] + " values[i+1][1]: " + values[i + 1][1]);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        counter++;
                        Log.d("test", "Query: Blocking que iter: " + i + " with message: " + questamp + " counter for next loop: " + counter + " for loop <: " + queindiloop);
                    }
                    queindiloop = 1;
                    for (int i = 0; i < 2; i++) {
                        Log.d("test", "Query: values[i][0]: " + values[i][0] + " values[i][1]: " + values[i][1]);
                    }
                    counter = 2;
                    String[][] finalvalue = new String[1][2];
                    for (int i = 0; i < counter; i++) {
                        if (i == 0) {
                            Log.d("test", "Query: Recent comparision iter: " + i);
                            if (values[0][1].compareTo(values[1][1]) >= 0) {
                                finalvalue[0][0] = values[0][0];
                                finalvalue[0][1] = values[0][1];
                            } else {
                                finalvalue[0][0] = values[1][0];
                                finalvalue[0][1] = values[1][1];
                            }
                            Log.d("test", "Query: After first comparision, value: " + finalvalue[0][0] + " between: " + values[0][0] + " and " + values[1][0]);
                        } else {
                            Log.d("test", "Query: Recent comparision iter: " + i);
                            if (finalvalue[0][1].compareTo(values[2][1]) >= 0) {
                                finalvalue[0][0] = finalvalue[0][0];
                                finalvalue[0][1] = finalvalue[0][1];
                            } else {
                                finalvalue[0][0] = values[2][0];
                                finalvalue[0][1] = values[2][1];
                            }
                            Log.d("test", "Query: After first comparision, value: " + finalvalue[0][0] + " between previous value and " + values[2][0]);
                        }
                    }
                    Log.d("test", "Query: Adding to matrix, key: " + split[0] + " value: " + finalvalue[0][0]);
                    cur.addRow(new Object[]{split[0], finalvalue[0][0]});
                } else {
                    Log.d("test", "Query: Single query, I am irrelevant avd");
                    String port = nobj.id + ";" + nobj.suc1id + ";" + nobj.suc2id;
                    String msg = "query;0;" + split[0] + ";" + myPort + "\n";
                    Log.d("test", "Query: Writing to ClientTask: " + msg + " to port: " + port);
                    while (quetransac == 1) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
                    String questamp = null;
                    String[][] values = new String[3][2];
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    int counter = 0;

                    for (int i = 0; i < 3; i++) {
                        try {
                            questamp = queblo.take();//Block till message is received
                            values[i] = questamp.split(";");//Split the time stamp from the value
                            Log.d("test", "Query: take splitted value: " + values[i][0] + " values[i+1][1]: " + values[i][1]);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        counter++;
                        Log.d("test", "Query: Blocking que iter: " + i + " with message: " + questamp + " counter for next loop: " + counter + " for loop <: " + queindiloop);
                        if (counter == 3) {
                            Log.d("test", "Query: counter == 3, break condition");
                            break;
                        }
                    }
                    Log.d("test", "Query: After blocking queue for loop");
                    queindiloop = 1;
                    Log.d("test", "Query: Before values[i] for loop");
                    for (int i = 0; i < 3; i++) {
                        Log.d("test", "Query: values[i][0]: " + values[i][0] + " values[i][1]: " + values[i][1]);
                    }
                    counter = 2;
                    String[][] finalvalue = new String[1][2];
                    for (int i = 0; i < counter; i++) {
                        if (i == 0) {
                            Log.d("test", "Query: Recent comparision iter: " + i);
                            if (values[0][1].compareTo(values[1][1]) >= 0) {
                                finalvalue[0][0] = values[0][0];
                                finalvalue[0][1] = values[0][1];
                            } else {
                                finalvalue[0][0] = values[1][0];
                                finalvalue[0][1] = values[1][1];
                            }
                            Log.d("test", "Query: After first comparision, value: " + finalvalue[0][0] + " between: " + values[0][0] + " and " + values[1][0]);
                        } else {
                            Log.d("test", "Query: Recent comparision iter: " + i);
                            if (finalvalue[0][1].compareTo(values[2][1]) >= 0) {
                                finalvalue[0][0] = finalvalue[0][0];
                                finalvalue[0][1] = finalvalue[0][1];
                            } else {
                                finalvalue[0][0] = values[2][0];
                                finalvalue[0][1] = values[2][1];
                            }
                            Log.d("test", "Query: After first comparision, value: " + finalvalue[0][0] + " between previous value and " + values[2][0]);
                        }
                    }
                    Log.d("test", "Query: Adding to matrix, key: " + split[0] + " value: " + finalvalue[0][0]);
                    cur.addRow(new Object[]{split[0], finalvalue[0][0]});
                }
            } else if (split.length == 1 && split[0].compareTo("@") == 0) {//When @ is called by grading script
                Log.d("test", "Query: Received for @ in current avd");
                FileInputStream in;
                String[] file = getContext().fileList();
                for (int i = 0; i < file.length; i++) {
                    String input = "";
                    try {
                        StringBuilder bui = new StringBuilder();
                        in = getContext().openFileInput(file[i]);
                        int eof;
                        while ((eof = in.read()) != -1) {
                            bui.append((char) eof);
                        }
                        input = bui.toString();
                        in.close();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    String[] val = input.split(";");
                    cur.addRow(new Object[]{file[i], val[0]});
                }
                Log.d("test", "Query: Added all files in current avd");
            } else if (split.length == 1 && split[0].compareTo("*") == 0) {//When * is called in current avd
                Log.d("test", "Query: Received for * in current avd");
                FileInputStream in;
                String[] file = getContext().fileList();
                for (int i = 0; i < file.length; i++) {//Adding all current avd key value pair in cursor object
                    String input = "";
                    try {
                        StringBuilder bui = new StringBuilder();
                        in = getContext().openFileInput(file[i]);
                        int eof;
                        while ((eof = in.read()) != -1) {
                            bui.append((char) eof);
                        }
                        input = bui.toString();
                        in.close();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    String[] val = input.split(";");
                    cur.addRow(new Object[]{file[i], val[0]});
                }
                Log.d("test", "Query: Added all current avd's files to cur");
                String port2 = "";
                for (int i = 0; i < 5; i++) {
                    if (REMOTE_PORT[i].compareTo(myPort) == 0) {
                        continue;
                    }
                    port2 += (REMOTE_PORT[i] + ";");
                }
                //port="11108;";
                String port = port2.substring(0, port2.length() - 1);
                String msg = "query;*;0;" + myPort + "\n";
                Log.d("test", "Query: Writing to ClientTask: " + msg + " to port: " + port);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);//Broadcasting to other available avd
                Log.d("test", "Query: questarcounter: " + questarcounter);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (int i = 0; i < questarcounter; i++) {//questarcounter is incremented only if a successful write happens to other avd
                    String questarstr = null;
                    try {
                        questarstr = queblo.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    Log.d("test", "Query: After take from queblo, counter total: " + questarcounter + " current iter: " + i + " take: " + questarstr);
                    String[] questaratrsplit = questarstr.split(";");
                    for (int j = 1; j < questaratrsplit.length; j += 2) {//Adding received key value pair to the cursor object
                        cur.addRow(new Object[]{questaratrsplit[j], questaratrsplit[j + 1]});
                    }
                    Log.d("test", "Query: Added to cur for iter: " + i);
                }
                questarcounter = 0;
                Log.d("test", "Query: Set value of questarcounter to 0: " + questarcounter);
            } else if (split.length == 3 && split[0].compareTo("*") == 0) {//When * is called in some other avd
                Log.d("test", "Query: received for * from avd: " + split[2]);
                FileInputStream in;
                String msg = "query;*;1";
                String[] file = getContext().fileList();
                for (int i = 0; i < file.length; i++) {
                    String input = "";
                    try {
                        StringBuilder bui = new StringBuilder();
                        in = getContext().openFileInput(file[i]);
                        int eof;
                        while ((eof = in.read()) != -1) {
                            bui.append((char) eof);
                        }
                        input = bui.toString();
                        in.close();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    String[] val = input.split(";");
                    msg = msg + ";" + file[i] + ";" + val[0];//Append key value pair to string to write to original avd where * was called
                }
                String port = split[2];
                msg = msg + "\n";
                Log.d("test", "Query: Writing to ClientTask: " + msg + " to port: " + port);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
                return null;
            } else if (split.length == 2 && split[0].compareTo("duplicate") == 0) {
                String callavd = split[1];
                Node nobj = null;
                Iterator iter = lst.iterator();
                while (iter.hasNext()) {
                    nobj = (Node) iter.next();
                    if (nobj.id.compareTo(callavd) == 0) {
                        break;
                    }
                }
                Node nobjmy = null;
                Iterator iter2 = lst.iterator();
                while (iter2.hasNext()) {
                    nobjmy = (Node) iter2.next();
                    if (nobjmy.id.compareTo(myPort) == 0) {
                        break;
                    }
                }
                String[] file = getContext().fileList();
                String[] fihash = new String[file.length];
                for (int i = 0; i < file.length; i++) {
                    try {
                        fihash[i] = genHash(file[i]);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
                if (nobj.pred1id.compareTo(myPort) == 0 || nobj.pred2id.compareTo(myPort) == 0) {
                    for (int i = 0; i < file.length; i++) {
                        if ((nobjmy.hid.compareTo(lst.get(0).hid) == 0 && (fihash[i].compareTo(lst.get(0).hid) <= 0 || fihash[i].compareTo(lst.get(lst.size() - 1).hid) > 0)) || (fihash[i].compareTo(nobjmy.hid) <= 0 && fihash[i].compareTo(nobjmy.pred1) > 0)) {
                            FileInputStream in;
                            String input = "";
                            try {
                                StringBuilder bui = new StringBuilder();
                                in = getContext().openFileInput(file[i]);
                                int eof;
                                while ((eof = in.read()) != -1) {
                                    bui.append((char) eof);
                                }
                                input = bui.toString();
                                in.close();
                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            Log.d("test", "Query: Adding to cursormatrix pred for failure, key: " + file[i] + " value: " + input);
                            cur.addRow(new Object[]{file[i], input});
                        }
                    }
                } else if (nobj.suc1id.compareTo(myPort) == 0 || nobj.suc2id.compareTo(myPort) == 0) {
                    for (int i = 0; i < file.length; i++) {
                        if ((nobj.hid.compareTo(lst.get(0).hid) == 0 && (fihash[i].compareTo(lst.get(0).hid) <= 0 || fihash[i].compareTo(lst.get(lst.size() - 1).hid) > 0)) || (fihash[i].compareTo(nobj.hid) <= 0 && fihash[i].compareTo(nobj.pred1) > 0)) {
                            FileInputStream in;
                            String input = "";
                            try {
                                StringBuilder bui = new StringBuilder();
                                in = getContext().openFileInput(file[i]);
                                int eof;
                                while ((eof = in.read()) != -1) {
                                    bui.append((char) eof);
                                }
                                input = bui.toString();
                                in.close();
                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            Log.d("test", "Query: Adding to cursormatrix suc for failure, key: " + file[i] + " value: " + input);
                            cur.addRow(new Object[]{file[i], input});
                        }
//
//
//
//                    if((nobjmy.hid.compareTo(lst.get(0).hid)==0 && (fihash[i].compareTo(lst.get(0).hid)<=0 || fihash[i].compareTo(lst.get(lst.size()-1).hid)>0)) || (fihash[i].compareTo(nobjmy.hid)<=0 && fihash[i].compareTo(nobjmy.pred1)>0)){
//                        Log.d("test","Query: Suc: iteration: "+i+" Skip in first if");
//                        continue;
//                    }
//                    else if(nobj.suc2id.compareTo(myPort)==0){
//                        if((nobjmy.pred1.compareTo(lst.get(0).hid)==0 && (fihash[i].compareTo(lst.get(0).hid)<=0 || fihash[i].compareTo(lst.get(lst.size()-1).hid)>0)) || (fihash[i].compareTo(nobjmy.pred1)<=0 && fihash[i].compareTo(nobjmy.pred2)>0)){
//                            Log.d("test","Query: Suc: iteration: "+i+" Skip in second if");
//                            continue;
//                        }
//                    }
//                    else{
//                        FileInputStream in;
//                        String input = "";
//                        try {
//                            StringBuilder bui = new StringBuilder();
//                            in = getContext().openFileInput(file[i]);
//                            int eof;
//                            while ((eof = in.read()) != -1) {
//                                bui.append((char) eof);
//                            }
//                            input = bui.toString();
//                            in.close();
//                        } catch (FileNotFoundException e) {
//                            e.printStackTrace();
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                        Log.d("test","Query: Adding to cursormatrix suc for failure, key: "+file[i]+" value: "+input);
//                        cur.addRow(new Object[]{file[i],input});
//                    }
                    }
                }
            }
//        else if(split.length==2 && !(split[0].contains("*") || split[0].contains("@"))){//Returning value to original avd where single query was made
//            Log.d("test","Query: Received for single query for key: "+split[0]+"from avd: "+split[1]);
//            FileInputStream in;
//            String input = "";
//            try {
//                StringBuilder bui = new StringBuilder();
//                in = getContext().openFileInput(split[0]);
//                int eof;
//                while ((eof = in.read()) != -1) {
//                    bui.append((char) eof);
//                }
//                input = bui.toString();
//                in.close();
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            cur.addRow(new Object[]{split[0],input});
//        }

        }
        return cur;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket accept = serverSocket.accept();
                    ObjectInputStream instream = new ObjectInputStream(accept.getInputStream());
                    String recmsg = (String) instream.readObject();
                    Log.d("test","Server: received message: "+recmsg);
                    ObjectOutputStream ostream=new ObjectOutputStream(accept.getOutputStream());

                    if(recmsg.contains("check")){//Failure Detection original message received
                        Log.d("test","Server: inside check;0");
                        query(uriobject,null,recmsg,null,null);
                    }
                    else if(recmsg.contains("chfound")){//Failure detection response received
                        Log.d("test","Server: inside chfound");
                        String[] split=recmsg.split(";");
                        if(split[1].compareTo("0")==0){
                            startcounter++;
                            Log.d("test","Server - startcounter: "+startcounter+" failcounter: "+failcounter);
                            if(startcounter==4){
                                stateblo.put("0");
                                startcounter=0;
                                stateblofailcounter=1;
                            }
                        }
                        else if(split[1].compareTo("1")==0){
                            Log.d("test","Server - stateblofailcounter: "+stateblofailcounter);
                            if(stateblofailcounter==0) {
                                stateblo.put("1");
                                Log.d("test","Server - putting 1 in stateblo");
                                startcounter = 0;
                                stateblofailcounter=1;
                            }
                        }
                    }
                    else if(recmsg.contains("insert")){//Insert received from some other avd
                        ostream.writeObject("Received "+recmsg);
                        ostream.flush();
                        ostream.close();
                        instream.close();
                        Log.d("test","Server: inside insert message is: "+recmsg);
                        String[] split=recmsg.split(";");
                        ContentValues cont=new ContentValues();
                        cont.put("key",split[2]);
                        cont.put("value",split[3]+";"+split[1]);
                        insert(uriobject,cont);
                    }
                    else if(recmsg.contains("query;0")){//Query received from some other avd to which response needs to be sent
                        Log.d("test","Server: inside query;0 message is: "+recmsg);
                        String[] split=recmsg.split(";");
                        String back=null;
                        FileInputStream in;
                        String input = "";
                        try {
                            StringBuilder bui = new StringBuilder();
                            in = getContext().openFileInput(split[2]);
                            int eof;
                            while ((eof = in.read()) != -1) {
                                bui.append((char) eof);
                            }
                            input = bui.toString();
                            back=input;
                            in.close();
                        } catch (FileNotFoundException e) {
                            back="0000;00:00:00.000";
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        Log.d("test","Server: sending back for individual query: "+back+" myPort: "+myPort);
                        ostream.writeObject(back);
                        ostream.flush();
                        ostream.close();
                        instream.close();
                    }
                    else if(recmsg.contains("query;*;0")){//Query * received from some other avd to which response needs to be sent
                        ostream.writeObject("Received "+recmsg);
                        ostream.flush();
                        ostream.close();
                        instream.close();
                        Log.d("test","Server: inside query;*;0 message is: "+recmsg);
                        String[] split=recmsg.split(";");
                        query(uriobject,null,split[1]+";"+split[2]+";"+split[3],null,null);
                    }
                    else if(recmsg.contains("query;*;1")){//Response * received from contacted avd for query
                        ostream.writeObject("Received "+recmsg);
                        ostream.flush();
                        ostream.close();
                        instream.close();
                        Log.d("test","Server: inside query;*;1 message is: "+recmsg);
                        String[] split=recmsg.split(";");
                        String toput="0";
                        for(int i=3;i<split.length;i+=2){
                            toput+=";"+split[i]+";"+split[i+1];
                        }
                        Log.d("test","Putting in queblo: "+toput);
                        queblo.put(toput);
                    }
                    else if(recmsg.contains("delete;1")){
                        ostream.writeObject("Received "+recmsg);
                        ostream.flush();
                        ostream.close();
                        instream.close();
                        Log.d("test","Server: inside delete;1 message is: "+recmsg);
                        String[] split=recmsg.split(";");
                        delinfiloop= Integer.parseInt(split[1]);
                        delete(uriobject,split[2],null);
                    }
                    else if(recmsg.contains("delete;@")){
                        ostream.writeObject("Received "+recmsg);
                        ostream.flush();
                        ostream.close();
                        instream.close();
                        Log.d("test","Server: inside delete;@ message is: "+recmsg);
                        String[] split=recmsg.split(";");
                        delete(uriobject,"@",null);
                    }
                    else if(recmsg.contains("duplicate")){
                        Cursor cur=query(uriobject,null,recmsg,null,null);
                        String temp="";
                        int count=cur.getCount();
                        int i=0;
                        while(i<count){
                            cur.moveToNext();
                            String key=cur.getString(0);
                            String value=cur.getString(1);
                            temp+=key+";"+value+";";
                            i++;
                        }
                        cur.close();
                        Log.d("test","Server - Failure temp: "+temp+" length: "+temp.length());
                        String msg=null;
                        if(temp.length()==0){
                            msg="empty";
                        }
                        else {
                            msg = temp.substring(0, temp.length() - 1);
                        }
                        msg+="\n";
                        Log.d("test","Server - Failure sending back: "+msg);
                        ostream.writeObject(msg);
                        ostream.flush();
                        ostream.close();
                        instream.close();
                   }

                    //publishProgress(recmsg);
                } catch (OptionalDataException e) {
                    e.printStackTrace();
                } catch (StreamCorruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0].trim();
            if (msgToSend.contains("check")) {//Failure detection
                for (int i = 0; i < 5; i++) {
                    String response = null;
                    if (REMOTE_PORT[i].compareTo(myPort) == 0) {
                        continue;
                    }
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORT[i]));
                        socket.setSoTimeout(750);
                        ObjectOutputStream ostream = new ObjectOutputStream(socket.getOutputStream());
                        Log.d("test", "ClientTask before sending(if): " + msgToSend);
                        ostream.writeObject(msgToSend);
                        ObjectInputStream instream = new ObjectInputStream(socket.getInputStream());
                        response = String.valueOf(instream.readObject());
                        Log.d("test", "ClientTask after sending(if): " + msgToSend);
                        ostream.flush();
                        instream.close();
                        ostream.close();
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        Log.d("test", "Client: Inside socket exception for failure");
                        if (stateblofailcounter == 0) {
                            try {
                                stateblo.put("0");
                                Log.d("test", "Client: Value'1' put in stateblo");
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                            stateblofailcounter = 1;
                        }
                    } catch (IOException e) {
                        Log.d("test", "Client: Inside IO exception for failure");
                        if (stateblofailcounter == 0) {
                            try {
                                stateblo.put("0");
                                Log.d("test", "Client: Value'1' put in stateblo");
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                            stateblofailcounter = 1;
                        }
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        Log.d("test", "Client: Inside Class not found exception for failure");
                        if (stateblofailcounter == 0) {
                            try {
                                stateblo.put("0");
                                Log.d("test", "Client: Value'1' put in stateblo");
                            } catch (InterruptedException e1) {
                                e1.printStackTrace();
                            }
                            stateblofailcounter = 1;
                        }
                        e.printStackTrace();
                    }
                }
            } else if (msgToSend.contains("chfound")) {//Failure detection response
                String response = null;
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));
                    socket.setSoTimeout(750);
                    ObjectOutputStream ostream = new ObjectOutputStream(socket.getOutputStream());
                    Log.d("test", "ClientTask before sending(if): " + msgToSend);
                    ostream.writeObject(msgToSend);
                    ObjectInputStream instream = new ObjectInputStream(socket.getInputStream());
                    response = String.valueOf(instream.readObject());
                    ostream.flush();
                    instream.close();
                    ostream.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            } else if (msgToSend.contains("insert")) {//Insert of any kind
                String[] ports = msgs[1].split(";");
                for (int i = 0; i < ports.length; i++) {
                    String response = null;
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(ports[i]));
                        socket.setSoTimeout(750);
                        ObjectOutputStream ostream = new ObjectOutputStream(socket.getOutputStream());
                        Log.d("test", "ClientTask before sending(if): " + msgToSend + " to port: " + ports[i]);
                        ostream.writeObject(msgToSend);
                        ObjectInputStream instream = new ObjectInputStream(socket.getInputStream());
                        response = String.valueOf(instream.readObject());
                        ostream.flush();
                        instream.close();
                        ostream.close();
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } else if (msgToSend.contains("query;0")) {//Query of any kind
                String[] ports = msgs[1].split(";");
                for (int i = 0; i < ports.length; i++) {
                    String response = null;
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(ports[i]));
                        socket.setSoTimeout(750);
                        ObjectOutputStream ostream = new ObjectOutputStream(socket.getOutputStream());
                        Log.d("test", "ClientTask before sending(if): " + msgToSend + " to port: " + ports[i]);
                        ostream.writeObject(msgToSend);
                        ObjectInputStream instream = new ObjectInputStream(socket.getInputStream());
                        response = String.valueOf(instream.readObject());
                        ostream.flush();
                        instream.close();
                        ostream.close();
                        queblo.put(response);
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        try {
                            queblo.put("0000;00:00:00.000");
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } catch (IOException e) {
                        try {
                            queblo.put("0000;00:00:00.000");
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        try {
                            queblo.put("0000;00:00:00.000");
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        try {
                            queblo.put("0000;00:00:00.000");
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                        e.printStackTrace();
                    }
                }
            } else if (msgToSend.contains("query;*;0")) {//Query * of any kind
                String[] ports = msgs[1].split(";");
                for (int i = 0; i < ports.length; i++) {
                    String response = null;
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(ports[i]));
                        socket.setSoTimeout(750);
                        ObjectOutputStream ostream = new ObjectOutputStream(socket.getOutputStream());
                        Log.d("test", "ClientTask before sending(if): " + msgToSend + " to port: " + ports[i]);
                        ostream.writeObject(msgToSend);
                        ObjectInputStream instream = new ObjectInputStream(socket.getInputStream());
                        response = String.valueOf(instream.readObject());
                        ostream.flush();
                        questarcounter++;
                        Log.d("test", "ClientTask after sending(if): " + msgToSend + " questarcounter: " + questarcounter);
                        instream.close();
                        ostream.close();
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } else if (msgToSend.contains("query;*;1")) {//Query * of any kind
                String[] ports = msgs[1].split(";");
                for (int i = 0; i < ports.length; i++) {
                    String response = null;
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(ports[i]));
                        socket.setSoTimeout(750);
                        ObjectOutputStream ostream = new ObjectOutputStream(socket.getOutputStream());
                        Log.d("test", "ClientTask before sending(if): " + msgToSend + " to port: " + ports[i]);
                        ostream.writeObject(msgToSend);
                        ObjectInputStream instream = new ObjectInputStream(socket.getInputStream());
                        response = String.valueOf(instream.readObject());
                        ostream.flush();
                        instream.close();
                        ostream.close();
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } else if (msgToSend.contains("delete") || msgToSend.contains("duplicate")) {//Delete of any kind or duplicate
                String[] ports = msgs[1].split(";");
                for (int i = 0; i < ports.length; i++) {
                    String response = null;
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(ports[i]));
                        socket.setSoTimeout(750);
                        ObjectOutputStream ostream = new ObjectOutputStream(socket.getOutputStream());
                        Log.d("test", "ClientTask before sending(if): " + msgToSend + " to port: " + ports[i]);
                        ostream.writeObject(msgToSend);
                        ObjectInputStream instream = new ObjectInputStream(socket.getInputStream());
                        response = String.valueOf(instream.readObject());
                        ostream.flush();
                        instream.close();
                        ostream.close();
                        socket.close();
                        if (msgToSend.contains("duplicate")) {
                            Log.d("test", "Client: Duplicate: received message: " + response + " length: " + response.length());
                            if (response.contains("empty")) {
                                continue;
                            }
                            String[] split = response.split(";");
                            Log.d("test", "Client: Duplicate: received message: " + response + " split.length: " + split.length);
                            for (int j = 0; j < split.length; j += 3) {
                                ContentValues cont = new ContentValues();
                                cont.put("key", split[j]);
                                String value = split[j + 1] + ";" + split[j + 2];
                                Log.d("test", "Client: Duplicate: iteration: " + j + " key: " + split[j] + " value: " + value);
                                cont.put("value", value);
                                insert(uriobject, cont);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
                if (msgToSend.contains("duplicate")) {
                    createcontcount = 1;
                }
            }
            return null;
        }
    }

    class Node{

	    String id;
	    String hid;
	    String suc1;
        String suc1id;
        String suc2;
        String suc2id;
        String pred1;
        String pred1id;
        String pred2;
        String pred2id;

    }

    class Ports implements Comparable<Ports>{

	    String prt;
	    String hashprt;

        @Override
        public int compareTo(Ports o) {
            if (o.hashprt.compareTo(this.hashprt)>0){
                return -1;
            }
            else if (o.hashprt.compareTo(this.hashprt)<0){
                return 1;
            }
            return 0;
        }
    }

}