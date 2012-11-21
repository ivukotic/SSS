package edu.uchicago.SSSserver;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class dbreader{

    private Connection conn;
    private final String dbhost;
    private final String dbusername;
    private final String dbpass;
    private final String filter;
    private final String sitename;

    private long oldout;
    private long oldin;
    private long oldconntime;
    private long tos;
    private int pid;
    
    dbreader(String dbhost, String dbusername, String dbpass, String filter, String sitename){
        conn = null;
        this.dbhost=dbhost;
        this.dbusername=dbusername;
        this.dbpass=dbpass;
	    this.oldout=0;
	    this.oldin=0;
	    this.oldconntime=0;
        this.filter=filter;
        this.sitename=sitename;
        tos=System.currentTimeMillis() / 1000L;
        try{
        	pid = Integer.parseInt(new File("/proc/self").getCanonicalFile().getName());
        }catch(Exception e){
        	System.err.println("could not get PID from /proc/self. Setting it to 123456.");
        	pid=123456;
        }
    }

    public void connect(){
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://"+dbhost+"/billing", dbusername, dbpass);
        } catch (ClassNotFoundException e) {
            System.out.println("Could not find the database driver!");
        } catch (SQLException e) {
            System.out.println("Could not connect to the database");
            System.err.println(e);
        }

    }

    public String sel(){
        try {
            Statement stmt = conn.createStatement();
            ResultSet rset = stmt.executeQuery("SELECT protocol, isnew, fullsize, transfersize, connectiontime from billinginfo where datestamp>(CURRENT_TIMESTAMP - interval '1 minutes') and initiator like '"+filter+"'");
            long tod=System.currentTimeMillis() / 1000L - 60;   

            while (rset.next()){
                System.out.println( rset.getString(1) +"\t"+ rset.getBoolean(2)+"\t"+rset.getLong(3)+"\t"+rset.getLong(4) +"\t"+rset.getLong(5) );
                boolean write=rset.getBoolean(2);
                if (write==true) oldin+=rset.getLong(3); else oldout+=rset.getLong(3);
                oldconntime+=rset.getLong(5);
            }
            String info="<stats id=\"info\"><host>xxx."+sitename+"</host><port>1094</port><name>anon</name></stats>";
            String sgen="<stats id=\"sgen\"><as>1</as><et>60000</et><toe>"+Long.toString(tod+60)+"</toe></stats>";
            String link="<stats id=\"link\"><num>1</num><maxn>1</maxn><tot>20</tot><in>"+Long.toString(oldin)+"</in><out>"+Long.toString(oldout)+"</out><ctime>"+Long.toString(oldconntime)+"</ctime><tmo>0</tmo><stall>0</stall><sfps>0</sfps></stats>";
            String result="<statistics tod=\""+Long.toString(tod)+"\" ver=\"v1.9.12.21\" src=\"xxx."+sitename+"\" tos=\""+Long.toString(tos)+"\" pgm=\"xrootd\" ins=\"anon\" pid=\""+pid+"\">"+info+sgen+link+"</statistics>";

            rset.close();
            stmt.close();
            return result;
        }
        catch (Exception e) {
            System.err.println(e);
            return "Error encountered.";
        }
    }

}
