package com.qcg;
//fourinone
import com.fourinone.Contractor;
import com.fourinone.MigrantWorker;
import com.fourinone.WareHouse;
import com.fourinone.WorkerLocal;
//beanstalk
import com.surftools.BeanstalkClient.BeanstalkException;
import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import com.surftools.BeanstalkClientImpl.*;
//redis
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import redis.clients.jedis.Jedis;
//json
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
//comm
import java.util.*;

//
public class JobWorker extends MigrantWorker{
    //
    public static final int OK = 1;
    public static final int NOW_NOT_HAVE_JOB = 2;
    public static final int BEANSTALK_ERROR = 3;
    public static final int OTHER_ERROR = 4;
    public static final int BEANSTALK_PUT_JOB_ERROR = 5;
    public static final int TUBE_NAME_ERROR = 6;
    public static final int JOB_STATUS_RUN = 2;
    public static final int JOB_STATUS_PAUSE = 3;
    public static final int JOB_STATUS_RESTART = 4;
    public static final int JOB_STATUS_STOP = 5;
    //
    /**
     *委托状态。
     '0'	未报
     '1'	待报
     '2'	已报
     '3'	已报待撤
     '4'	部成待撤
     '5'	部撤
     '6'	已撤
     '7'	部成
     '8'	已成
     '9'	废单
     */

    public String beanstalk_server_ip = "172.16.193.114";
    public int beanstalk_server_port = 11300;
    public String redis_server_ip = "127.0.0.1";
    public int redis_server_port = 6379;
    public String thrift_server_ip = "127.0.0.1";
    public int thrift_server_port = 9090;
    public int thrift_server_timeout = 30000;
    private String work_name;
    //log4j
    private static Logger log = Logger.getLogger("JobWorker.class");
    //Ctor
    public JobWorker(){
    }
    public JobWorker(String work_name)
    {
        this.work_name = work_name;
    }
    public JobWorker(String beanstalk_server_ip, int beanstalk_server_port,
                   String redis_server_ip,int redis_server_port,
                   String thrift_server_ip,int thrift_server_port,int thrift_server_timeout){
        this.beanstalk_server_ip = beanstalk_server_ip;
        this.beanstalk_server_port = beanstalk_server_port;
        this.redis_server_ip = redis_server_ip;
        this.redis_server_port = redis_server_port;
        this.thrift_server_ip = thrift_server_ip;
        this.thrift_server_port = thrift_server_port;
        this.thrift_server_timeout = thrift_server_timeout;
    }

    public int delet_old_and_put_new_job_to_tube(Client beanstalk_client,String tube_name,JobItemDetail job_item,
                                                 long need_del_it,
                                                 String ins_tube_name){
        int err_num = 0;
        while(err_num < 3) {
            JSONObject jsonObject = JSONObject.fromObject(job_item);
            beanstalk_client.useTube(tube_name);
            //1.delete old msg
            boolean is_suc = beanstalk_client.delete(need_del_it);
            //2.add new msg
            long jobId = 0;
            if(ins_tube_name == null){
                jobId = beanstalk_client.put(1024, 0, 120, (jsonObject.toString()).getBytes());
            }else{
                beanstalk_client.useTube(ins_tube_name);
                jobId = beanstalk_client.put(1024, 0, 120, (jsonObject.toString()).getBytes());
            }
            if(jobId > 0) {
                break;
            }else{
                err_num++;
            }
        }
        if(err_num >= 3){
            return BEANSTALK_PUT_JOB_ERROR;
        }
        return OK;
    }
    public void add_msg_to_jobitemdetail_obj(JobItemDetail job_item,
                                             String pro_id,String job_id,String stock_code,String buyorsell){
        job_item.pro_id = pro_id;
        job_item.job_id = job_id;
        job_item.stock_code = stock_code;
        job_item.buyorsell = buyorsell;
    }
    public void add_error_num_and_put_to_error_queue(JobItemDetail job_item,
                                                     String pro_id,String job_id,String stock_code,String buyorsell,
                                                     Client beanstalk_client,String tube_name,Job job){
        try {
            job_item.err_num++;
            if(job_item.err_num > 3){
                //add need msg
                add_msg_to_jobitemdetail_obj(job_item,pro_id,job_id,stock_code,buyorsell);
                //4.put it to error queue.
                delet_old_and_put_new_job_to_tube(beanstalk_client,tube_name,job_item,job.getJobId(),"error");
            }else{
                //add err num,put it back to queue.
                delet_old_and_put_new_job_to_tube(beanstalk_client,tube_name,job_item,job.getJobId(),null);
            }
        } catch (BeanstalkException be) {
            log.debug(be.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage());
        }
    }
    public JobItemDetail do_job_work(Client beanstalk_client,String tube_name,Job job){
        //
        String[] tube_name_array = tube_name.split(";");
        String pro_id = tube_name_array[0];
        String job_id = tube_name_array[1];
        String percent = tube_name_array[2];
        String stock_code = tube_name_array[3];
        String exchange_type = tube_name_array[4];
        String buyorsell = tube_name_array[5];
        String price = tube_name_array[6];
        //
        ZxTradeSdkHelp help = new ZxTradeSdkHelp();
        SdkResult result = null;
        JobItemDetail job_item = new JobItemDetail();
        try {
            //
            //1.parse json job string to beans.
            String jobstring = new String(job.getData());
            JSONObject job_obj = JSONObject.fromObject(jobstring);
            job_item = (JobItemDetail)JSONObject.toBean(job_obj, JobItemDetail.class);
            log.debug("job_item.entrust_no=" + job_item.entrust_no);
            boolean find_status = true;
            if(job_item.job_msg_id <= 0){
                find_status = false;
            }
            if(job_item.entrust_no == null){
                find_status = false;
            }else{
                if(job_item.entrust_no.trim().length() == 0 ){
                    find_status = false;
                }else if(Integer.parseInt(job_item.entrust_no) <= 0){
                    find_status = false;
                }
            }
            if(find_status == false){
                //1.first msg ,send
                job_item.job_msg_id = job.getJobId();
                String yj_buy_num = Integer.toString(job_item.yj_buy_num);
                log.debug(yj_buy_num);
                result = help.run_normal_entrust(job_item.account,stock_code, exchange_type, yj_buy_num,price,buyorsell);
                //2.put new a job msg to beanstalk queue.
                if(result.error_code == 0){
                    //add need msg
                    add_msg_to_jobitemdetail_obj(job_item,pro_id,job_id,stock_code,buyorsell);
                    //3.SUCCESS. put entrust_no to queue
                    job_item.entrust_no = result.entrust_no;
                    job_item.begin_sec = new Date().getTime()/1000;
                    delet_old_and_put_new_job_to_tube(beanstalk_client,tube_name,job_item,job.getJobId(),null);
                }else{
                    add_error_num_and_put_to_error_queue(job_item,
                            pro_id,job_id,stock_code,buyorsell,
                            beanstalk_client,tube_name,job);
                }
            }else{
                result = help.serach_entrust_status(job_item.account, job_item.entrust_no);
                if(result.error_code == 0) {
                    for (SdkEntrustStatus item : result.entrust_status) {
                        //public String business_price;
                        //public String business_amount;
                        //public String entrust_date;
                        //public String entrust_no;
                        //public String entrust_status;
                        if(item.entrust_status.equals("8")){
                            job_item.sj_buy_num = Double.valueOf(item.business_amount).intValue();;
                            job_item.sj_buy_price = Double.parseDouble(item.business_price);
                            job_item.end_date = item.entrust_date;
                            //
                            beanstalk_client.useTube(tube_name);
                            boolean del_suc = beanstalk_client.delete(job.getJobId());
                            log.debug(del_suc);
                        }else{
                            //add need msg
                            add_msg_to_jobitemdetail_obj(job_item,pro_id,job_id,stock_code,buyorsell);
                            //4.put it to waiting queue.
                            delet_old_and_put_new_job_to_tube(beanstalk_client,tube_name,job_item,job.getJobId(),"waiting");
                        }
                    }
                }else{
                    add_error_num_and_put_to_error_queue(job_item,
                            pro_id,job_id,stock_code,buyorsell,
                            beanstalk_client,tube_name,job);
                }
            }
        } catch (BeanstalkException be) {
            log.debug(be.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage());
        }
        return job_item;
    }
    public WareHouse doTask(WareHouse inhouse) {
        //1.get parm : tube name
        String tube_name = (String)inhouse.getObj("tube_name");
        //
        String[] str_array=tube_name.split(";");
        if( str_array.length < 7) {
            inhouse.setObj("worker_error_code",TUBE_NAME_ERROR);
            return inhouse;
        }

        String worker_result = work_name + ":" + "tube_name => " + tube_name + ",";
        List msg_ret_list = new ArrayList();
        try {

            //1. get a job from jobs queue, tube name : jobs
            Client beanstalk_client = new ClientImpl(beanstalk_server_ip,beanstalk_server_port);
            beanstalk_client.watch(tube_name);
            beanstalk_client.useTube(tube_name);
            while(!isInterrupted()) {
                //get job with 2 sec timeout
                Job job = beanstalk_client.reserve(2);
                if (job == null) {
                    break;
                }else{
                    JobItemDetail job_item = do_job_work(beanstalk_client,tube_name,job);
                    if(job_item.sj_buy_num > 0){
                        JSONObject jo = JSONObject.fromObject(job_item);
                        msg_ret_list.add(jo.toString());
                    }
                }
            }
            //
            beanstalk_client.ignore(tube_name);
            beanstalk_client.close();
            //
        } catch (BeanstalkException be) {
            log.debug(be.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage());
        }
        //
        inhouse.setObj("worker_error_code",OK);
        inhouse.setObj("worker_result", msg_ret_list);
        log.debug(msg_ret_list);
        return inhouse;
    }
    public static void main( String[] args ){
        //
        PropertyConfigurator.configure("E:\\project\\IDEA\\autotrade\\Log4j.properties");
        //
        String beanstalk_server_ip = System.getProperty("beanstalk.server.ip");
        String beanstalk_server_port = System.getProperty("beanstalk.server.port");
        String redis_server_ip = System.getProperty("redis.server.ip");
        String redis_server_port = System.getProperty("redis.server.port");
        String thrift_server_ip = System.getProperty("thrift.server.ip");
        String thrift_server_port = System.getProperty("thrift.server.port");
        String thrift_server_timeout = System.getProperty("thrift.server.timeout");
        String worker_name = System.getProperty("worker.name");
        String worker_ip = System.getProperty("worker.ip");
        String worker_port = System.getProperty("worker.port");
        String b_ip = (beanstalk_server_ip != null) ? beanstalk_server_ip : "127.0.0.1";
        int b_port = (beanstalk_server_port != null) ? Integer.getInteger(beanstalk_server_port) : 11300;
        String r_ip = (redis_server_ip != null) ? redis_server_ip : "127.0.0.1";
        int r_port = (redis_server_port != null) ? Integer.getInteger(redis_server_port) : 6379;
        String t_ip = (thrift_server_ip != null) ? thrift_server_ip : "127.0.0.1";
        int t_port = (thrift_server_port != null) ? Integer.getInteger(thrift_server_port) : 9090;
        int t_timeout = (thrift_server_timeout != null) ? Integer.getInteger(thrift_server_timeout) : 30000;
        //
        String w_n = (worker_name != null) ? worker_name : "worker";
        String w_ip = (worker_ip != null) ? worker_ip : "127.0.0.1";
        log.debug("worker_port=" + worker_port);
        int w_port = (worker_port != null) ? Integer.parseInt(worker_port) : 9001;
        log.debug("w_port=" + w_port);
        JobWorker jobworker = new JobWorker(w_n);
        jobworker.waitWorking(w_ip,w_port,"jobworker");
        //
        //JobWorker jobworker = new JobWorker(b_ip,b_port,r_ip,r_port,t_ip,t_port,t_timeout);
        //JobWorker jobworker = new JobWorker();
        //jobworker.waitWorking("jobworker");
        //jobworker.waitWorking("127.0.0.1",9876,"jobworker");

/*        WareHouse inhouse = new WareHouse();
        String tube_name = "PRO_1;10;10;600696;1;1;20;20141203143558";
        inhouse.setObj("tube_name", tube_name);
        JobWorker jobworker = new JobWorker();
        WareHouse outhouse = jobworker.doTask(inhouse);
        log.debug(outhouse);*/
    }
}
