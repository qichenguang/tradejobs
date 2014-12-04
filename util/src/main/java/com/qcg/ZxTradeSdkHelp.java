package com.qcg;
//thrift
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
//comm
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.Random;
//gen java thrift code
import com.autotrade.zxtradesdk.*;
//java encode
import java.io.UnsupportedEncodingException;
import java.io.*;
import java.nio.ByteBuffer;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.HTMLLayout;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.*;
import org.slf4j.LoggerFactory;
class SdkEntrustStatus {
    public String business_price;
    public String business_amount;
    public String entrust_date;
    public String entrust_no;
    public String entrust_status;
    private static Logger log = Logger.getLogger("SdkEntrustStatus.class");
    public void showMsg(){
        log.debug("business_price=" + business_price);
        log.debug("business_amount=" + business_amount);
        log.debug("entrust_date=" + entrust_date);
        log.debug("entrust_no=" + entrust_no);
        log.debug("entrust_status=" + entrust_status);
    }
}
class SdkResult {
    public int error_code;
    public ByteBuffer error_msg;
    public String sh_account;
    public String sz_account;
    public String current_balance;
    public String entrust_no;
    public List<SdkEntrustStatus> entrust_status = new ArrayList<SdkEntrustStatus>();
    private static Logger log = Logger.getLogger("SdkResult.class");
    public void showMsg(){
        log.debug("error_code=" + error_code);
        // 构建一个byte数组
        byte [] content = new byte[error_msg.limit()];
        // 从ByteBuffer中读取数据到byte数组中
        error_msg.get(content);
        // 把byte数组的内容写到标准输出
        log.debug("error_msg=" + new String(content));
        log.debug("sh_account=" + sh_account);
        log.debug("sz_account=" + sz_account);
        log.debug("current_balance=" + current_balance);
        log.debug("entrust_no=" + entrust_no);
        int pos=0;
        for(SdkEntrustStatus item : entrust_status){
            log.debug("pos=" + pos);
            item.showMsg();
            pos++;
        }
    }
}
public class ZxTradeSdkHelp
{
    //
    public TTransport transport = null;
    public TSocket socket = null;
    public TProtocol protocol = null;
    public ZxTradeSdk.Client client = null;
    //
    public String thrift_server_ip = "127.0.0.1";
    public int thrift_server_port = 9090;
    public int thrift_server_timeout = 30000;
    //
    private static Logger log = Logger.getLogger("ZxTradeSdkHelp.class");
    //Ctor
    public ZxTradeSdkHelp(){
    }
    public ZxTradeSdkHelp(String thrift_server_ip,int thrift_server_port,int thrift_server_timeout){
        this.thrift_server_ip = thrift_server_ip;
        this.thrift_server_port = thrift_server_port;
        this.thrift_server_timeout = thrift_server_timeout;
    }
    public void init(){
        try {
            transport = new TSocket(thrift_server_ip, thrift_server_port, thrift_server_timeout);
            // 协议要和服务端一致
            protocol = new TBinaryProtocol(transport);
            // TProtocol protocol = new TCompactProtocol(transport);
            // TProtocol protocol = new TJSONProtocol(transport);
            //
            client = new ZxTradeSdk.Client(protocol);
            transport.open();
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
    public void fini(){
        try {
            transport.close();
            transport = null;
        }  catch (Exception e){
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
    }
    public void showMsg(ZxTradeAnsData in_ans_data){
        log.debug(in_ans_data.error_code);
        log.debug(in_ans_data.error_msg);
        int pos=0;
        for(Map<String,String> a_ansData : in_ans_data.ans_data){
            log.debug("pos=" + pos);
            for(Map.Entry<String,String> entry : a_ansData.entrySet()) {
                log.debug("key=" + entry.getKey() + ",val=" + entry.getValue());
            }
            pos++;
        }
    }
    public SdkResult get_sdk_ret_msg(String cmd,ZxTradeAnsData in_ans_data){
        SdkResult out_data = new SdkResult();
        out_data.error_code = in_ans_data.error_code;
        out_data.error_msg = in_ans_data.error_msg;
        if("LOGIN" == cmd){
            //
        } else if("SEARCH_STOCKHOLDER" == cmd){
            for(Map<String,String> a_ansData : in_ans_data.ans_data){
                String exchange_type = "";
                String stock_account = "";
                for(Map.Entry<String,String> entry: a_ansData.entrySet()) {
                    if("exchange_type".equals(entry.getKey())){
                        exchange_type = entry.getValue();
                    }
                    if("stock_account".equals(entry.getKey())){
                        stock_account = entry.getValue();
                    }
                }
                if("1".equals(exchange_type)){
                    out_data.sh_account = stock_account;
                } else if("2".equals(exchange_type)){
                    out_data.sz_account = stock_account;
                }
            }
        } else if("SEARCH_FUND_MSG" == cmd){
            String current_balance = "";
            for(Map<String,String> a_ansData : in_ans_data.ans_data){
                for(Map.Entry<String,String> entry: a_ansData.entrySet()) {
                    if("current_balance".equals(entry.getKey())){
                        current_balance = entry.getValue();
                    }
                }
            }
            out_data.current_balance = current_balance;
        } else if("RUN_NORMAL_ENTRUST" == cmd){
            String entrust_no = "";
            for(Map<String,String> a_ansData : in_ans_data.ans_data){
                for(Map.Entry<String,String> entry: a_ansData.entrySet()) {
                    if("entrust_no".equals(entry.getKey())){
                        entrust_no = entry.getValue();
                    }
                }
            }
            out_data.entrust_no = entrust_no;
        } else if("SEARCH_ENTRUST_STATUS" == cmd){
            for(Map<String,String> a_ansData : in_ans_data.ans_data){
                SdkEntrustStatus entrus_status = new SdkEntrustStatus();
                for(Map.Entry<String,String> entry: a_ansData.entrySet()){
                    if("entrust_status".equals(entry.getKey())){
                        entrus_status.entrust_status = entry.getValue();
                        //log.debug("entrus_status.entrust_status=" + entrus_status.entrust_status);
                    }
                    if("entrust_no".equals(entry.getKey())){
                        entrus_status.entrust_no = entry.getValue();
                        //log.debug("entrus_status.entrust_no=" + entrus_status.entrust_no);
                    }
                    if("entrust_date".equals(entry.getKey())){
                        entrus_status.entrust_date = entry.getValue();
                        //log.debug("entrus_status.entrust_date=" + entrus_status.entrust_date);
                    }
                    if("business_amount".equals(entry.getKey())){
                        entrus_status.business_amount = entry.getValue();
                        //log.debug("entrus_status.business_amount=" + entrus_status.business_amount);
                    }
                    if("business_price".equals(entry.getKey())){
                        entrus_status.business_price = entry.getValue();
                        //log.debug("entrus_status.business_price=" + entrus_status.business_price);
                    }

                }
                out_data.entrust_status.add(entrus_status);
            }
        } else if("RUN_UNDO_ENTRUST" == cmd){
            //
        }
        return out_data;
    }
    public SdkResult sdk_comm_cmd(String cmd,String account,String pwd_stockCode_entrustNo, String shorsz, String num, String price, String buyorsell){
        ZxTradeAnsData sdk_ret_data = null;
        try{
            this.init();
            //
            if("LOGIN" == cmd){
                sdk_ret_data = this.client.login(account,pwd_stockCode_entrustNo);
            } else if ("SEARCH_STOCKHOLDER" == cmd){
                sdk_ret_data = this.client.search_stockholder(account);
            } else if ("SEARCH_FUND_MSG" == cmd){
                sdk_ret_data = this.client.search_fund_msg(account);
            } else if ("RUN_NORMAL_ENTRUST" == cmd){
                sdk_ret_data = this.client.run_normal_entrust(account, pwd_stockCode_entrustNo, shorsz, num, price, buyorsell);
            } else if ("SEARCH_ENTRUST_STATUS" == cmd){
                sdk_ret_data = this.client.serach_entrust_status(account, pwd_stockCode_entrustNo);
            } else if ("RUN_UNDO_ENTRUST" == cmd){
                sdk_ret_data = this.client.run_undo_entrust(account, pwd_stockCode_entrustNo);
            }
            //
            //this.showMsg(sdk_ret_data);
            //
            this.fini();
        }catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        //
        SdkResult out_data = this.get_sdk_ret_msg(cmd,sdk_ret_data);
        return out_data;
    }
    public SdkResult login(String account,String pwd){
        return this.sdk_comm_cmd("LOGIN",account,pwd,null,null,null,null);
    }
    public SdkResult  search_stockholder(String account){
        return this.sdk_comm_cmd("SEARCH_STOCKHOLDER", account, null, null, null, null, null);
    }
    public SdkResult  search_fund_msg(String account){
        return this.sdk_comm_cmd("SEARCH_FUND_MSG", account,null,null,null,null,null);
    }
    public SdkResult  run_normal_entrust(String account, String stock_code, String shorsz, String num, String price, String buyorsell){
        return this.sdk_comm_cmd("RUN_NORMAL_ENTRUST", account, stock_code, shorsz, num, price, buyorsell);
    }
    public SdkResult  serach_entrust_status(String account, String entrust_no){
        return this.sdk_comm_cmd("SEARCH_ENTRUST_STATUS", account, entrust_no, null, null, null, null);
    }
    public SdkResult run_undo_entrust(String account, String entrust_no){
        return this.sdk_comm_cmd("RUN_UNDO_ENTRUST", account, entrust_no, null, null, null, null);
    }
    public static void main( String[] args ) {
        PropertyConfigurator.configure("E:\\project\\IDEA\\autotrade\\Log4j.properties");

        ZxTradeSdkHelp help = new ZxTradeSdkHelp();
        SdkResult result = null;
        result = help.login("600511005", "606869");
        result.showMsg();

        result = help.search_stockholder("600511005");
        result.showMsg();

        result = help.search_fund_msg("600511005");
        result.showMsg();

        result = help.run_normal_entrust("600511005","600696", "1", "120", "20.00", "1");
        result.showMsg();

        result = help.serach_entrust_status("600511005","2903");
        result.showMsg();

/*        result = help.run_undo_entrust("600511005","4277");
        result.showMsg();*/
    }
}


