package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

/*
*类名和方法不能修改
 */
public class Schedule {


    private TreeMap<Integer,ArrayList<TaskInfo>> pool ;
    private ArrayList<TaskInfo> waitTaskList;


    public int init() {
        pool = new TreeMap<Integer,ArrayList<TaskInfo>>();
        waitTaskList = new ArrayList<TaskInfo>();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {

        if(null!=pool.get(nodeId)){
            return ReturnCodeKeys.E005;

        }else if(nodeId<0){
            return ReturnCodeKeys.E004;
        }else{
            ArrayList<TaskInfo> taskList = new ArrayList<TaskInfo>();
            pool.put(nodeId,taskList);
            return ReturnCodeKeys.E003;
        }

    }

    public int unregisterNode(int nodeId) {
        if(null==pool.get(nodeId)){
            return ReturnCodeKeys.E007;

        }else if(nodeId<=0){
            return ReturnCodeKeys.E004;
        }else{
            ArrayList<TaskInfo> taskList = pool.get(nodeId);
            for(TaskInfo task : taskList){
                waitTaskList.add(task);
            }
            pool.remove(nodeId);
            return ReturnCodeKeys.E006;
        }
    }


    public int addTask(int taskId, int consumption) {

        if(taskId<=0){
            return ReturnCodeKeys.E009;
        }
        for(TaskInfo task:waitTaskList){
            if(task.getTaskId()==taskId){
                return ReturnCodeKeys.E010;
            }
        }


        for(Integer nodeid : pool.keySet()){
            for(TaskInfo task : pool.get(nodeid)){
                if(task.getTaskId()==taskId){
                    return ReturnCodeKeys.E010;
                }
            }
        }



        TaskInfo task = new TaskInfo();
        task.setTaskId(taskId);
        task.setConsumption(consumption);
        waitTaskList.add(task);
        return ReturnCodeKeys.E008;


    }


    public int deleteTask(int taskId) {
        if(taskId<=0){
            return ReturnCodeKeys.E009;
        }


        for(Integer nodeid : pool.keySet()){
            for(TaskInfo task : pool.get(nodeid)){
                if(task.getTaskId()==taskId){
                    pool.get(nodeid).remove(task);
                    return ReturnCodeKeys.E011;
                }
            }
        }
        for(TaskInfo task : waitTaskList){
            if(task.getTaskId()==taskId){
                waitTaskList.remove(task);
                return ReturnCodeKeys.E011;
            }
        }


        return ReturnCodeKeys.E012;
    }


    public int scheduleTask(int threshold) {
        if(threshold<=0){
            return ReturnCodeKeys.E002;
        }


        ArrayList<TaskInfo> allTask  = new ArrayList<TaskInfo>();
        int allCost = 0;
        ArrayList<TaskInfo> ascTask  = new ArrayList<TaskInfo>();
        for(Integer nodeid : pool.keySet()){
            for(TaskInfo task : pool.get(nodeid)){
                allTask.add(task);
                allCost=allCost+task.getConsumption();
            }
        }
        for(TaskInfo task : waitTaskList){
            allTask.add(task);
            allCost=allCost+task.getConsumption();
        }

        int nodeCost = allCost/pool.size();

        TaskInfo[]  tasks = new TaskInfo[allTask.size()];
        for(int i=0;i<allTask.size();i++){
            tasks[i]=allTask.get(i);
        }
        for(int i =0;i < tasks.length - 1;i++)
        {
            for(int j = 0;j <  tasks.length - 1-i;j++)// j开始等于0，
            {
                if(tasks[j].getConsumption() > tasks[j+1].getConsumption())
                {
                    TaskInfo temp = tasks[j];
                    tasks[j] = tasks[j+1];
                    tasks[j+1] = temp;
                }else if(tasks[j].getConsumption() == tasks[j+1].getConsumption()){
                    if(tasks[j].getTaskId()<tasks[j+1].getTaskId()){
                        TaskInfo temp = tasks[j];
                        tasks[j] = tasks[j+1];
                        tasks[j+1] = temp;
                    }
                }
            }
        }

        int start=0;

        for(Integer nodeid : pool.keySet()){
            int cost = 0;
            for(int i = start;i<tasks.length;i++){
                tasks[i].setNodeId(nodeid);
                pool.get(nodeid).add(tasks[i]);
                cost=cost+tasks[i].getConsumption();
                start++;
                if(cost>nodeCost){
                    cost=0;
                    continue;
                }
            }
        }
        if(start<tasks.length-1){
            for(int i = start;i<tasks.length;i++){
                pool.lastEntry().getValue().add(tasks[i]);
            }
        }

        return ReturnCodeKeys.E013;
    }






    public int queryTaskStatus(List<TaskInfo> tasks) {
        if(null == tasks){
            return ReturnCodeKeys.E016;
        }

        for(Integer nodeid : pool.keySet()){
            for(TaskInfo task : pool.get(nodeid)){
                tasks.add(task);
            }
        }
        for(TaskInfo task : waitTaskList){
            tasks.add(task);
        }
        return ReturnCodeKeys.E015;
    }

}
