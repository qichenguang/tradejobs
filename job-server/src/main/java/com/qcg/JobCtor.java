package com.qcg;

//fourinone
import com.fourinone.Contractor;
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

public class JobCtor extends Contractor{

    public static final int OK = 1;
    public static final int NOW_NOT_HAVE_JOB = 2;
    public static final int BEANSTALK_ERROR = 3;
    public static final int NO_WAITING_WORKERS_ERROR = 4;
    public static final int OTHER_ERROR = 5;
    public static final int BEANSTALK_PUT_JOB_ERROR = 6;
    //
    public static final int JOB_STATUS_RUN = 2;
    public static final int JOB_STATUS_PAUSE = 3;
    public static final int JOB_STATUS_RESTART = 4;
    public static final int JOB_STATUS_STOP = 5;

    public String beanstalk_server_ip = "172.16.193.114";
    public int beanstalk_server_port = 11300;
    public String redis_server_ip = "127.0.0.1";
    public int redis_server_port = 6379;
    //log4j
    private static Logger log = Logger.getLogger("JobCtor.class");
    //Ctor
    public JobCtor(){
    }
    public JobCtor(String beanstalk_server_ip, int beanstalk_server_port,
                   String redis_server_ip,int redis_server_port){
        this.beanstalk_server_ip = beanstalk_server_ip;
        this.beanstalk_server_port = beanstalk_server_port;
        this.redis_server_ip = redis_server_ip;
        this.redis_server_port = redis_server_port;
    }

    public WorkerLocal[] get_waiting_workers(String worker_type){
        WorkerLocal[] worker_arr = null;
        try {
            worker_arr = getWaitingWorkers(worker_type);
            log.debug("worker_arr.length:" + ((worker_arr == null) ? 0 :worker_arr.length));
        }catch(Exception e){
            log.debug(e.getMessage());
        }
        return worker_arr;
    }

    public WareHouse giveTask(WareHouse inhouse){
        //1.get parm : tube name
        String tube_name = (String)inhouse.getObj("tube_name");
        String job_id = (String)inhouse.getObj("job_id");
        Jedis jedis = (Jedis)inhouse.getObj("redis_obj");
        Client beanstalk_client = (Client)inhouse.getObj("beanstalk_obj");
        WorkerLocal[] worker_arr = (WorkerLocal[])inhouse.getObj("workers_array_obj");
        //
        log.debug(tube_name);
        //2.get job workers, now from parameter
        //3.start up worker task.
        WareHouse[] wh_result_arr = new WareHouse[worker_arr.length];
        for(int i=0;i<worker_arr.length;i++){
            WareHouse wh_cur = new WareHouse();
            wh_cur.setObj("tube_name", tube_name);
            //
            wh_result_arr[i] = worker_arr[i].doTask(wh_cur);
        }
        //4.check jobs result.
        int worker_ok_num = 0;
        List msg_ret_list_all = new ArrayList();
        while(true){
            //5.check redis job status.
            //连接redis服务
            String job_status = jedis.hget("jobs:status",job_id);
            log.debug("job id=" + job_id + ",status=" + job_status);
            if(JOB_STATUS_PAUSE == Integer.parseInt(job_status) || JOB_STATUS_STOP == Integer.parseInt(job_status)){
                //停止所有工人计算
                for (WorkerLocal aWorker_arr : worker_arr) {
                    aWorker_arr.interrupt();
                }
            }
            //6.休眠等待 1 秒
            try{
                Thread.sleep(1000L);
            }catch(Exception ex){
                log.debug(ex.getMessage());
            }
            //7.检查 worker 结果
            for(int i=0;i<worker_arr.length;i++){//检查结果是否完成
                if(wh_result_arr[i]!=null){
                    if(wh_result_arr[i].getStatus() == WareHouse.READY) {
                        List cur_job_result = (List) wh_result_arr[i].getObj("worker_result");
                        msg_ret_list_all.addAll(cur_job_result);
                        wh_result_arr[i] = null;
                        worker_ok_num++;
                    }else if(wh_result_arr[i].getStatus() == WareHouse.EXCEPTION){
                        log.debug("Workers:" + i + " error!");
                        wh_result_arr[i] = null;
                        worker_ok_num++;
                    }
                }
            }
            //
            if(worker_ok_num >= worker_arr.length){
                //8.清空 job queue
                if(JOB_STATUS_STOP == Integer.parseInt(job_status)){
                    remove_all_msg_of_tube(beanstalk_client,tube_name);
                }
                break;
            }
        }
        //8.返回所有 workers 结果
        inhouse.setObj("workers_result",msg_ret_list_all);
        return inhouse;
    }
    public void remove_all_msg_of_tube(Client beanstalk_client,String tube_name){
        try {
            //1. get a job from jobs queue, tube name : jobs
            beanstalk_client.watch(tube_name);
            beanstalk_client.useTube(tube_name);
            while(true) {
                //get job with 2 sec timeout
                Job job = beanstalk_client.reserve(2);
                if (job == null) {
                    return;
                }
                //delete job
                beanstalk_client.delete(job.getJobId());
            }
        } catch (BeanstalkException be) {
            log.debug(be.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage());
        }
        return ;
    }
    public String general_tube_name_from_map(Map job_map){
        return  job_map.get("pro_id") + ";" +
                job_map.get("id") + ";" +
                job_map.get("percent") + ";" +
                job_map.get("stock_code") + ";" +
                job_map.get("exchange_type") + ";" +
                job_map.get("buyorsell") + ";" +
                job_map.get("price") + ";" +
                job_map.get("createtime");
    }
    public int put_result_to_redis(Jedis jedis,Map<String, String> job_map,WareHouse job_parm){
        try{
            //
            String buyorsell = job_map.get("buyorsell").equals("1") ? "buy" : "sell";
            String stock_code = job_map.get("stock_code");
            String pro_id = job_map.get("pro_id");
            //6.返回 job 结果
            List workers_result = (List) job_parm.getObj("workers_result");
            log.debug(job_parm.getObj("workers_result"));
            //
            int cur_job_all_sj_num = 0;
            double cur_job_all_sj_price = 0.0;
            //
            jedis.sadd("cur:stocks:" + pro_id,stock_code);
            Map<String, Integer> cur_account_num_map = new HashMap<String, Integer>();
            Map<String, Double> cur_account_price_map = new HashMap<String, Double>();
            //
            for (Object it : workers_result) {
                log.debug(it.toString());
                //用于job 子表
                jedis.rpush("jobs:" + buyorsell + ":result:" + job_map.get("id"), it.toString());
                //calc to redis
                JSONObject job_obj = JSONObject.fromObject(it);
                JobItemDetail job_item = new JobItemDetail();
                job_item = (JobItemDetail) JSONObject.toBean(job_obj, JobItemDetail.class);
                //
                String cur_account = job_item.getAccount();
                Integer prev_num = cur_account_num_map.get(cur_account);
                Double prev_price = cur_account_price_map.get(cur_account);
                if(buyorsell.equals("buy")) {
                    cur_job_all_sj_num += job_item.getSj_buy_num();
                    cur_job_all_sj_price += job_item.getSj_buy_price()*job_item.getSj_buy_num();
                    if(null == prev_num){
                        cur_account_num_map.put(cur_account,job_item.getSj_buy_num());
                    }else{
                        cur_account_num_map.put(cur_account,job_item.getSj_buy_num() + prev_num);
                    }
                    if(null == prev_price){
                        cur_account_price_map.put(cur_account,job_item.getSj_buy_price()*job_item.getSj_buy_num());
                    }else{
                        cur_account_price_map.put(cur_account,job_item.getSj_buy_price()*job_item.getSj_buy_num() + prev_price);
                    }
                }else{
                   //sell
                }
            }
            //用于 job 表2字段
            jedis.hincrBy("jobs:" + buyorsell + ":result:all_sj_num", job_map.get("id"), cur_job_all_sj_num);
            //jedis.hincrByFloat("jobs:" + buyorsell + ":result:all_sj_price", job_map.get("id"), cur_job_all_sj_price);
            //jedis.hincrBy("jobs:" + buyorsell + ":result:all_sj_price", job_map.get("id"), (int)cur_job_all_sj_price);
            String old_price = jedis.hget("jobs:" + buyorsell + ":result:all_sj_price", job_map.get("id"));
            if(old_price == null){
                old_price = "0";
            }
            jedis.hset("jobs:" + buyorsell + ":result:all_sj_price", job_map.get("id"), Double.toString(cur_job_all_sj_price + Double.parseDouble(old_price)));
            //用于 项目当前库存表
            for (Iterator<String> it = cur_account_num_map.keySet().iterator(); it.hasNext();) {
                String account = it.next();
                Integer num = cur_account_num_map.get(account);
                Double  price = cur_account_price_map.get(account);
                String n_p = jedis.hget("cur:stocks:" + pro_id +":" + stock_code, account);
                log.debug("n_p=" + n_p);
                if(null == n_p){
                    jedis.hset("cur:stocks:" + pro_id +":" + stock_code,account,num + "|" + price);
                }else{
                    String n_p_array[] = n_p.split("\\|");
                    if(buyorsell.equals("buy")){
                        num += Integer.parseInt(n_p_array[0]);
                        price += Double.parseDouble(n_p_array[1]);
                    }else{
                        num -= Integer.parseInt(n_p_array[0]);
                        price -= Double.parseDouble(n_p_array[1]);
                    }
                    jedis.hset("cur:stocks:" + pro_id +":" + stock_code,account,num + "|" + price);
                }
            }

        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return OK;
    }
    public void run(){
        while(true){
            try {
                //1.init
                Client beanstalk_client = new ClientImpl(beanstalk_server_ip, beanstalk_server_port);
                Jedis jedis = new Jedis(redis_server_ip, redis_server_port);
                Map<String, String> job_map = new HashMap<String, String>();
                Map<String, String> pro_account_csje_map = new HashMap<String, String>();
                WorkerLocal[][] workers = new  WorkerLocal[1][];
                //2.get a job from beanstalk queue, will return NOW_NOT_HAVE_JOB in timeout second.
                int ret = get_a_job_from_jobs_tube_and_can_use_workers(beanstalk_client, job_map, pro_account_csje_map, workers);
                //3.put job to workers tube queue.
                if (OK == ret) {
                    ret = general_tube_jobs_for_workers(beanstalk_client, job_map, pro_account_csje_map);
                    if (OK == ret) {
                        //4.set parm : tube_name
                        String tube_name = general_tube_name_from_map(job_map);
                        WareHouse job_parm = new WareHouse();
                        job_parm.setObj("tube_name", tube_name);
                        job_parm.setObj("job_id", job_map.get("id"));
                        job_parm.setObj("redis_obj", jedis);
                        job_parm.setObj("beanstalk_obj", beanstalk_client);
                        job_parm.setObj("workers_array_obj", workers[0]);
                        //
                        //5.start up workers.
                        giveTask(job_parm);
                        //6.返回 job 结果,设置 redis
                        ret = put_result_to_redis(jedis,job_map,job_parm);
                        if(OK != ret){
                            log.debug("put_result_to_redis error!");
                        }
                    }
                }
                //7.close conn obj.
                beanstalk_client.close();
                jedis.close();
            }catch (BeanstalkException be) {
                log.debug(be.getMessage());
            } catch (Exception e) {
                log.debug(e.getMessage());
            }
            //8.休眠等待 1 秒
            try{
                Thread.sleep(1000L);
            }catch(Exception ex){
                System.out.println(ex.getMessage());
            }
        }
    }
    public int general_buy_jobs(List msg_list,Map job_map,Map pro_account_csje_map){
        try
        {
            float percent = Float.parseFloat((String)job_map.get("percent"));
            float price = Float.parseFloat((String)job_map.get("price"));
            //3.geneal list : account => num
            Set<String> keys = pro_account_csje_map.keySet();
            for (Iterator it = keys.iterator(); it.hasNext();) {
                //account => 100% csje
                String account = (String) it.next();
                float csje = Float.parseFloat((String) pro_account_csje_map.get(account));
                //cur csje : percent% csje
                float cur_csje = csje * percent / 100;
                int num_100 = (int)((cur_csje / price) / 100);
                while(num_100 > 0){
                    Random rand = new Random();
                    //rand num => (0 - 4)  => 1 - 5
                    int buy_num = rand.nextInt(5) + 1;
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("account", account);
                    if(num_100 <= buy_num){
                        jsonObject.put("yj_buy_num", num_100 * 100);
                        msg_list.add(jsonObject.toString());
                        break;
                    }else{
                        jsonObject.put("yj_buy_num", buy_num * 100);
                        msg_list.add(jsonObject.toString());
                        num_100 -= buy_num;
                    }
                }
            }
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return OK;
    }
    public int general_tube_jobs_for_workers(Client beanstalk_client,Map job_map,Map pro_account_csje_map){
        //1.general tube name
        String tube_name = general_tube_name_from_map(job_map);
        try
        {
            log.debug("tube_name=" + tube_name);
            //2.calc stock num
            List msg_list = new ArrayList();
            float percent = Float.parseFloat((String)job_map.get("percent"));
            float price = Float.parseFloat((String)job_map.get("price"));
            String buyorsell = (String)job_map.get("buyorsell");
            if(buyorsell.equals("1")){
                //buy
                general_buy_jobs(msg_list,job_map,pro_account_csje_map);
            }else{
                //sell

            }

            //4.put jobs to beanstalk
            beanstalk_client.useTube(tube_name);
            for(int i = 0;i < msg_list.size(); i++){
                int err_num = 0;
                while(err_num < 3) {
                    long jobId = beanstalk_client.put(1024, 0, 120, ((String) msg_list.get(i)).getBytes());
                    if(jobId > 0) {
                        break;
                    }else{
                        err_num++;
                    }
                }
                if(err_num >= 3){
                    return BEANSTALK_PUT_JOB_ERROR;
                }
            }
        }catch (BeanstalkException be) {
            log.debug(be.getMessage());
            return BEANSTALK_ERROR;
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }finally {
            beanstalk_client.ignore(tube_name);
        }
        //
        return OK;
    }
    public int get_a_job_from_jobs_tube_and_can_use_workers(Client beanstalk_client,Map job_map,Map pro_account_csje_map,WorkerLocal[][] workers) {
        try {
            //1. get a job from jobs queue, tube name : jobs
            beanstalk_client.watch("jobs");
            beanstalk_client.useTube("jobs");
            //get job with 2 sec timeout
            Job job = beanstalk_client.reserve(2);
            if(job == null){
                return NOW_NOT_HAVE_JOB;
            }
            long newJobId = job.getJobId();
            String dstString = new String(job.getData());
            log.debug(dstString);
            //get job detail,set to map
            JSONObject job_obj = JSONObject.fromObject(dstString);
            //
            for (Iterator iter = job_obj.keys(); iter.hasNext();)
            {
                String key = (String) iter.next();
                String value = job_obj.get(key).toString();
                job_map.put(key, value);
            }
            //get account=>csje, set to map
            JSONObject pro_account_csje_obj = job_obj.getJSONObject("pro_account_csje");
            //
            for (Iterator iter = pro_account_csje_obj.keys(); iter.hasNext();)
            {
                String key = (String) iter.next();
                String value = pro_account_csje_obj.get(key).toString();
                pro_account_csje_map.put(key, value);
            }
            WorkerLocal[] worker_arr = get_waiting_workers("jobworker");
            if(worker_arr != null && worker_arr.length > 0){
                //delete job
                beanstalk_client.delete(job.getJobId());
                workers[0] = worker_arr;
            }else{
                return NO_WAITING_WORKERS_ERROR;
            }

        } catch (BeanstalkException be) {
            log.debug(be.getMessage());
            return BEANSTALK_ERROR;
        } catch (Exception e) {
            log.debug(e.getMessage());
            return OTHER_ERROR;
        }
        return OK;
    }
    public static void main( String[] args ){
        //
        PropertyConfigurator.configure("E:\\project\\IDEA\\autotrade\\Log4j.properties");
        //
        String beanstalk_server_ip = System.getProperty("beanstalk.server.ip");
        String beanstalk_server_port = System.getProperty("beanstalk.server.port");
        String redis_server_ip = System.getProperty("redis.server.ip");
        String redis_server_port = System.getProperty("redis.server.port");
        String b_ip = (beanstalk_server_ip != null) ? beanstalk_server_ip : "127.0.0.1";
        int b_port = (beanstalk_server_port != null) ? Integer.getInteger(beanstalk_server_port) : 11300;
        String r_ip = (redis_server_ip != null) ? redis_server_ip : "127.0.0.1";
        int r_port = (redis_server_port != null) ? Integer.getInteger(redis_server_port) : 6379;
        //JobCtor jobctor = new JobCtor(b_ip,b_port,r_ip,r_port);
        JobCtor jobctor = new JobCtor();
        jobctor.run();
    }
}
