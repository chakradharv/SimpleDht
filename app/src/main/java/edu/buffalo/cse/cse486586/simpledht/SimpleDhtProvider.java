package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Iterator;

import android.database.MatrixCursor;
import android.content.Context;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private SQLiteDatabase grpMsgDb;
    private DataStorage dbStorage;
    public static final int SERVER_PORT = 10000;
    String sendPort = "11108";
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    String predecessor = null;
    String successor = null;
    final String PREDECESSOR_CONS = "pred";
    final String SUCCESSOR_CONS = "succ";
    String myPort;
    private Uri mUri;
    public static final String DB_KEY = "key";
    public static final String DB_VAL = "value";
    public static final String amb = "&";
    boolean mutex = true;
    static int count = 0;
    static HashSet<String> portSet = new HashSet<String>();
    boolean mainport = true;

    String[] portsArr = null;
    String queryData = null;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.d("deleteselection ", selection);
        return deletehandler(selection);
    }

    int deletehandler(String selection) {
        Log.d("selection ", selection);
        if (!selection.contains("@") && !selection.contains("*")) {
            if (myPort.equals(successor) && myPort.equals(predecessor)) {
                Log.d("delSelection ", selection);
                return deleteOneKey(selection);
            } else {
                String msg = selection + ";";
                Log.d("delSelectiontwo ", selection);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, sendPort);
            }
        } else if (selection.contains("@")) {
            return deleteLocalKeys();
        } else {
            if (myPort.equals(predecessor) && myPort.equals(successor)) {
                Log.d("delSelectionthree", selection);
                deleteLocalKeys();
            } else {
                Log.d("delSelectionfour ", selection);
                String[] portsArrNw = {"11108", "11112", "11116", "11120", "11124"};
                for (int i = 0; i < portsArrNw.length; i++) {
                    String msg = selection + ">";
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, portsArrNw[i]);
                }
            }
        }
        return 0;
    }


    public int deleteDHT(String selection) {
        String key = selection.replace(";", "");
        String portToSend = null;

        try {
            String hashkey = genHash(key);
            String hashvalMyPort = genHash(Integer.parseInt(myPort) / 2 + "");
            String hashvalPredPort = genHash(Integer.parseInt(predecessor) / 2 + "");
            String hashvalSuccPort = genHash(Integer.parseInt(successor) / 2 + "");


            int keyMyPortComp = hashkey.compareTo(hashvalMyPort);
            int keyPredPortComp = hashkey.compareTo(hashvalPredPort);
            int keySuccPortComp = hashkey.compareTo(hashvalSuccPort);
            int myPortPredPortComp = hashvalMyPort.compareTo(hashvalPredPort);
            int myPortSuccPortComp = hashvalMyPort.compareTo(hashvalSuccPort);


            if (myPort.equals(successor) && myPort.equals(predecessor)) {
                deleteOneKey(key);
            } else {
                if (myPortPredPortComp < 0 && myPortSuccPortComp < 0) {
                    if (keyMyPortComp > 0) {
                        if (keyPredPortComp > 0) {
                            Log.d("firstselectionIfelse ", selection);
                            return deleteOneKey(key);
                        } else {
                            Log.d("secondselectionIfelse ", selection);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection, successor);
                        }
                    } else {
                        return deleteOneKey(key);
                    }
                } else {
                    if (keyMyPortComp > 0) {
                        Log.d("thirdselectionIfelse ", selection);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection, successor);
                    } else if (keyPredPortComp > 0) {
                        Log.d("fourthselectionIfelse ", selection);
                        return deleteOneKey(key);
                    } else {
                        Log.d("fifthselectionIfelse ", selection);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, selection, predecessor);
                    }
                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException Exception");
        }
        return 0;
    }

    int deleteOneKey(String selection) {
        Log.d("delselection ", selection);
     /*   String query =
                "DELETE from " + TABLE_NAME
                        + " where " + DB_KEY + "='" + selection
                        + "'";*/
        SQLiteDatabase dbk = dbStorage.getWritableDatabase();
        return dbk.delete(TABLE_NAME, "key='" + selection + "'", null);
    }

    int deleteLocalKeys() {
        Log.d("delselectionLocal ", myPort);
//        String query = "DELETE * from " + TABLE_NAME;
        SQLiteDatabase dbak = dbStorage.getWritableDatabase();
        return dbak.delete(TABLE_NAME, null, null);
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key = (String) values.getAsString(DB_KEY);
        String value = (String) values.getAsString(DB_VAL);
        Log.d("FinalRingCt", myPort + "#" + predecessor + "#" + successor);
        Log.d("portSetmyPort ", portSet.toString() + "#" + myPort);

        try {
            String hashkey = genHash(key);
            String hashvalMyPort = genHash(Integer.parseInt(myPort) / 2 + "");
            String hashvalPredPort = genHash(Integer.parseInt(predecessor) / 2 + "");
            String hashvalSuccPort = genHash(Integer.parseInt(successor) / 2 + "");


            int keyMyPortComp = hashkey.compareTo(hashvalMyPort);
            int keyPredPortComp = hashkey.compareTo(hashvalPredPort);
            int keySuccPortComp = hashkey.compareTo(hashvalSuccPort);
            int myPortPredPortComp = hashvalMyPort.compareTo(hashvalPredPort);
            int myPortSuccPortComp = hashvalMyPort.compareTo(hashvalSuccPort);


            if (myPort.equals(successor) && myPort.equals(predecessor)) {
                return writeIntoDB(uri, values);
            } else {
                if (myPortPredPortComp < 0 && myPortSuccPortComp < 0) {
                    if (keyMyPortComp > 0) {
                        if (keyPredPortComp > 0) {
                            Log.d("FirstForwardingMsg ", key);
                            return writeIntoDB(uri, values);
                        } else {
                            String msg = key + "---" + value;
                            Log.d("SecondForwardingMsg ", msg + "#" + successor);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor);
                        }
                    } else {
                        return writeIntoDB(uri, values);
                    }
                } else {
                    if (keyMyPortComp > 0) {
                        String msg = key + "---" + value;
                        Log.d("ThirdsForwardingMsg ", msg + "#" + successor);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor);
                    } else if (keyPredPortComp > 0) {
                        Log.d("FourthingMsg ", key);
                        return writeIntoDB(uri, values);
                    } else {
                        String msg = key + "---" + value;
                        Log.d("FifthForwardingMsg ", msg + "#" + predecessor);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, predecessor);
                    }
                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException Exception");
        }
        return null;
    }


    Uri writeIntoDB(Uri uri, ContentValues values) {

        String key = (String) values.getAsString(DB_KEY);
        Log.d("writeIntoDB ", key);
        grpMsgDb = dbStorage.getWritableDatabase();
        long row_id = grpMsgDb.insertWithOnConflict(TABLE_NAME, null, values, 5);

        if (row_id != -1) {
            return uri.withAppendedPath(uri, Long.toString(row_id));
        }
        return uri;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreate() {
        dbStorage = new DataStorage(getContext());
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(
                Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        portSet.add(sendPort);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            successor = myPort;
            predecessor = myPort;
            if (!myPort.equals(sendPort)) {
                Log.d(TAG, "portStr: " + portStr);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort + amb, "11108");
            }

        } catch (IOException e) {
            Log.e("Not5554", e.getMessage());
            Log.e(TAG, "Can't create a ServerSocket");
        }
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    BufferedReader brReader = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));

                    String recMsg = brReader.readLine().trim();

                    Log.d(TAG, "iN SERVER TASK MESSAGE RECEIVED IS " + recMsg);
                    if (recMsg.contains("---")) {//For Insertion
                        Log.d("InServInsert", myPort + "#" + recMsg.split("---")[0]);
                        ContentValues cv = new ContentValues();
                        cv.put(DB_KEY, recMsg.split("---")[0]);
                        cv.put(DB_VAL, recMsg.split("---")[1]);
                        insert(mUri, cv);
                    } else if (recMsg.contains("__")) {//For Querying single key
                        Log.d("severQueryDht", myPort + "#" + recMsg);
                        queryDHT(recMsg);
                    } else if (recMsg.contains("%")) {//Result came from   single avd
                        queryData = recMsg;
                        mutex = false;
                    } else if (recMsg.contains("$")) {
                        queryAllMsgs(recMsg);
                    } else if (recMsg.contains("~~")) {//Result came from   single avd
                        queryData = recMsg;
                        mutex = false;
                    } else if (recMsg.contains(";")) {//For deletion single key
                        Log.d("serverDel", recMsg);
                        deleteDHT(recMsg);
                    } else if (recMsg.contains(">")) {//For deletion from single avd
                        Log.d("serverDelAll", recMsg);
                        deleteLocalKeys();
                    } else if (recMsg.contains("::")) {
                        getAllPorts(recMsg);
                    } else if (recMsg.contains("!!")) {
                        portsArr = recMsg.split("!!");
                        mainport = false;
                    } else if (recMsg.contains(amb)) {
                        if (!recMsg.contains("#"))
                            portSet.add(recMsg.replace(amb, ""));
                        Log.d(TAG, "nodeformaion");
                        nodeFormation(recMsg);
                    }
                    brReader.close();
                    socket.close();
                }

            } catch (IOException e) {
                Log.d(TAG, e.getMessage());
            }
            return null;
        }
    }

    void getAllPorts(String recvMsg) {
        Log.d("getAllPorts ", recvMsg);
        String portsd = recvMsg.replace("::", "");
        String portStr = "";
        Iterator<String> it = portSet.iterator();
        Log.d("getAllPorts portSet", portSet.toString());
        while (it.hasNext()) {
            portStr += it.next() + "!!";
        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portStr, portsd);

    }

    void nodeFormation(String recMsg) {

        try {
            Log.d("nodeFormationRecMsg ", recMsg);
            recMsg = recMsg.replace(amb, "");
            if (recMsg.contains("#")) {
                if (recMsg.contains(PREDECESSOR_CONS)) {
                    predecessor = recMsg.split("#")[0];
                } else if (recMsg.contains(SUCCESSOR_CONS)) {
                    successor = recMsg.split("#")[1];
                } else {
                    predecessor = recMsg.split("#")[0];
                    successor = recMsg.split("#")[1];
                }
                Log.d("predSucc ", myPort + "#" + predecessor + "#" + successor);
                return;
            } else {
                String recvPort = recMsg;

                Log.d(TAG, "In ELSE nodeFormationRecMsg ");
                String hashvalRecevPort = genHash(Integer.parseInt(recvPort) / 2 + "");
                String hashvalMyPort = genHash(Integer.parseInt(myPort) / 2 + "");

                Log.d("hashvalRecevPort ", hashvalRecevPort);

                Log.d("hashvalMyPort ", hashvalMyPort);


              /*  if (hashvalRecevPort.compareTo(hashvalMyPort) == 0) {
                    predecessor = sendPort;
                    successor = sendPort;
                    return;
                } else {*/
                String hashvalSuccPort = genHash(Integer.parseInt(successor) / 2 + "");
                String hashvalPredPort = genHash(Integer.parseInt(predecessor) / 2 + "");

                int recPortMyPortComp = hashvalRecevPort.compareTo(hashvalMyPort);
                int recPortPredPortComp = hashvalRecevPort.compareTo(hashvalPredPort);
                int myPortPredPortComp = hashvalMyPort.compareTo(hashvalPredPort);
                int recPortSuccPortComp = hashvalRecevPort.compareTo(hashvalSuccPort);
                int myPortSuccPortComp = hashvalMyPort.compareTo(hashvalSuccPort);

                if (myPort.equals(successor) && myPort.equals(predecessor)) {
                    successor = recvPort;//2 case
                    predecessor = recvPort;
                    String msg = myPort + "#" + myPort + amb;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, recvPort);
                } else {
                    if (recPortMyPortComp > 0) {
                        if (recPortSuccPortComp < 0) {//3 case
                            String msg = myPort + "#" + successor + amb;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, recvPort);
                            msg = recvPort + "#" + PREDECESSOR_CONS + amb;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor);
                            successor = recvPort;
                        } else if (myPortSuccPortComp > 0) {//4 case
                            String msg = myPort + "#" + successor + amb;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, recvPort);
                            msg = recvPort + "#" + PREDECESSOR_CONS + amb;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor);
                            successor = recvPort;
                        } else {//5 case
                            String msg = recvPort + amb;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor);
                        }
                    } else if (recPortMyPortComp < 0) {
                        if (recPortPredPortComp > 0) {//6 case
                            String msg = predecessor + "#" + myPort + amb;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, recvPort);
                            msg = SUCCESSOR_CONS + "#" + recvPort + amb;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, predecessor);
                            predecessor = recvPort;
                        } else if (myPortPredPortComp < 0) {// 7 case
                            String msg = predecessor + "#" + myPort + amb;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, recvPort);
                            msg = SUCCESSOR_CONS + "#" + recvPort + amb;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, predecessor);
                            predecessor = recvPort;
                        } else {
                            String msg = recvPort + amb;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, predecessor);
                        }

                    }
                }
//                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.d(TAG, e.getMessage());
        }


    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            try {

                Log.d(TAG, "Inclienttaskmsgstosend " + msgs[0] + " msgs[1]: to server of " + msgs[1]);
                String port = msgs[1];

                Log.d(TAG, ":port1 " + msgs[1]);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));

                String msgToSend = msgs[0];

                Log.d(TAG, "Message sending from client: " + msgToSend);


                PrintWriter pw =
                        new PrintWriter(socket.getOutputStream(), true);
                pw.write(msgToSend);
                pw.flush();
                pw.close();


                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.d("TAG ", msgs[1]);
                Log.e(TAG, "ClientTask socket IOException" + e.getMessage());
            }
            return null;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        String recvmsg = selection;

        return queryHandler(selection);
    }

    void queryAllMsgs(String recMsg) {
        String portTosd = recMsg.replace("$", "");
        Log.d("portTosd ", portTosd);
        String str = "select * from " + TABLE_NAME;
        SQLiteDatabase db = dbStorage.getReadableDatabase();
        Cursor cursor = db.rawQuery(str, null);

        String keyValPairs = "";


        while (cursor.moveToNext()) {
            keyValPairs += (cursor.getString(0) + "," + cursor
                    .getString(1));
            keyValPairs += "~~";
        }


        if (keyValPairs == "") {
            Log.d("EMpty keyvalues pairs ", myPort);
            keyValPairs = "~~";
        }


        new ClientTask().executeOnExecutor(
                AsyncTask.SERIAL_EXECUTOR, keyValPairs,
                portTosd);

    }

    Cursor queryHandler(String recvmsg) {
        if (!recvmsg.contains("@") && !recvmsg.contains("*")) {
            String key = recvmsg;
            if (myPort.equals(successor) && myPort.equals(predecessor)) {
                return getCursorKey(recvmsg);
            } else {
                String msg = key + "__" + myPort;
                Log.d("queryMsg", msg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        msg, sendPort);
                while (mutex) {

                }
                Log.d("queryData", queryData);
                String[] keyval = queryData.split("%");
                String keyff = keyval[0];
                String val11 = keyval[1];
                String arr[] = new String[]{DB_KEY, DB_VAL};
                String rowa[] = new String[]{keyff, val11};
                MatrixCursor mc = new MatrixCursor(arr);
                mc.addRow(rowa);
                mutex = true;
                queryData = null;
                return mc;
            }
        } else if (recvmsg.contains("@")) {
            String str = "select * from " + TABLE_NAME;
            SQLiteDatabase db = dbStorage.getReadableDatabase();
            Cursor cursor = db.rawQuery(str, null);
            return cursor;
        } else {
            if (recvmsg.contains("*")) {
                if (myPort.equals(predecessor) && myPort.equals(successor)) {
                    grpMsgDb = dbStorage.getReadableDatabase();
                    String str = "select * from " + TABLE_NAME;
                    Cursor cursor = grpMsgDb.rawQuery(str, null);
                    return cursor;
                } else {
                    String matArr[] = new String[]{DB_KEY, DB_VAL};
                    MatrixCursor matCursor = new MatrixCursor(matArr);
                    String msg1 = myPort + "::";
                    mainport = true;
                    Log.d("queryHandler", myPort);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, sendPort);
                    while (mainport) {

                    }


                    for (int n = 0; n < portsArr.length; n++) {
                        queryData = null;
                        mutex = true;
                        String msg = myPort + "$";
                        Log.d("queryTask", myPort);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, portsArr[n]);
                        while (mutex) {

                        }

                        if (!queryData.equals("~~")) {
                            Log.d("queryDataFInal", queryData);
                            String[] arr = queryData.split("~~");
                            String ky;
                            String val;
                            for (int k = 0; k < arr.length; k++) {
                                ky = arr[k].split(",")[0];
                                val = arr[k].split(",")[1];
                                String[] stm = new String[]{ky, val};
                                matCursor.addRow(stm);
                            }
                        }
                    }
                    mutex = true;
                    queryData = null;
                    Log.d("cursolen ", matCursor.getCount() + "");
                    portsArr = null;
                    return matCursor;
                }
            }
        }
        return null;
    }

    Cursor queryDHT(String recvmsg) {
        String key = recvmsg.split("__")[0];
        String portToSend = null;
        boolean flag = recvmsg.contains("__");
        Log.d("recvmsg", recvmsg);
        try {
            if (flag) {
                portToSend = recvmsg.split("__")[1];
            }
            Log.d("portToSend ", portToSend);
            String hashkey = genHash(key);
            String hashvalMyPort = genHash(Integer.parseInt(myPort) / 2 + "");
            String hashvalPredPort = genHash(Integer.parseInt(predecessor) / 2 + "");
            String hashvalSuccPort = genHash(Integer.parseInt(successor) / 2 + "");


            int keyMyPortComp = hashkey.compareTo(hashvalMyPort);
            int keyPredPortComp = hashkey.compareTo(hashvalPredPort);
            int keySuccPortComp = hashkey.compareTo(hashvalSuccPort);
            int myPortPredPortComp = hashvalMyPort.compareTo(hashvalPredPort);
            int myPortSuccPortComp = hashvalMyPort.compareTo(hashvalSuccPort);


            if (myPortPredPortComp < 0 && myPortSuccPortComp < 0) {
                if (keyMyPortComp > 0) {
                    if (keyPredPortComp > 0) {
                        Log.d("firstIf", key);
                        Cursor cs = getCursorKey(key);
                        cs.moveToFirst();
                        String keyf = cs.getString(0);
                        String valuef = cs.getString(1);
                        String keyval = keyf + "%" + valuef;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyval, portToSend);
                    } else {
                        Log.d("SecIf", key);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recvmsg, successor);
                    }
                } else {
                    Log.d("Thirdf", key);
                    Cursor cs = getCursorKey(key);
                    cs.moveToFirst();
                    String keyf = cs.getString(0);
                    String valuef = cs.getString(1);
                    String keyval = keyf + "%" + valuef;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyval, portToSend);
                }
            } else {
                if (keyMyPortComp > 0) {
                    Log.d("fourthif", key);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recvmsg, successor);
                } else if (keyPredPortComp > 0) {
                    Log.d("Fifthif", key);
                    Cursor cs = getCursorKey(key);
                    cs.moveToFirst();
                    String keyf = cs.getString(0);
                    String valuef = cs.getString(1);
                    String keyval = keyf + "%" + valuef;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyval, portToSend);
                } else {
                    Log.d("Sixthhif", key);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recvmsg, predecessor);
                }
            }

        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException");
        }
        return null;
    }

    public Cursor getCursorKey(String selection) {
        grpMsgDb = dbStorage.getReadableDatabase();


        String str = "select * from " + TABLE_NAME
                + " where " + DB_KEY + "='" + selection
                + "'";
        Cursor cursor = grpMsgDb.rawQuery(str, null);
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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


    static String TABLE_NAME = "GroupMsgTable";
    static int DB_VERSION = 2;


    private static class DataStorage extends SQLiteOpenHelper {
        static String DB_NAME = "GroupDB";
        static String KEY = "key";
        static String VALUE = "value";
        String CREATE_DB_TABLE =
                " CREATE TABLE " + TABLE_NAME +
                        " ( key TEXT NOT NULL UNIQUE, value TEXT NOT NULL);";

        DataStorage(Context context) {
            super(context, DB_NAME, null, DB_VERSION);
            Log.d("DataStorage", "db");
            Log.d("create", CREATE_DB_TABLE);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {


            db.execSQL(CREATE_DB_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion,
                              int newVersion) {
            db.execSQL(TABLE_NAME);
            onCreate(db);
        }
    }
}
