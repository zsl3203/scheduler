import pandas as pd
import time
import re
import os
from threading import Thread
import sys


import logging


logging.basicConfig(filename="main.log")


def get_jobs():
    info = pd.read_csv("./scheduler.csv")
    jobs = {}
    for i in range(info.shape[0]):
        job = info.loc[i,"name"]
        second = int(info.loc[i,"seconds"])
        yaml = info.loc[i,"yaml"]
        jobs[job] = (second,yaml)
    return jobs

def run_jobs(jobs,max):
    for i in range(max+1):
        for job in jobs:
            if jobs[job][0] == i:
                yaml = jobs[job][1]
                os.popen('kubectl ' + 'apply ' + '-f ' + yaml)
        time.sleep(1)

def get_pod_node_pair():
    pair_info = os.popen(
        "kubectl get pod -o=custom-columns=NODE:.spec.nodeName,NAME:.metadata.name").read().split("\n")
    dic = {}
    for i in range(1, len(pair_info) - 1):
        line = pair_info[i]
        line = " ".join(line.split())
        match = re.match(r"(.*) (.*)", line)
        node = match.group(1)
        pod = match.group(2)
        dic[pod] = node
    return dic





class job_list(object):
    def __init__(self,joblist):
        self.jobs = pd.read_csv(joblist)
        self.stop = self.jobs.seconds.max()
        self.job_plans = [i for i in self.jobs["name"]]
        self.monitor_job = self.job_plans
        self.job_dict = {}
        self.run_job_list()
    # run job list start at a specific time
    def run_job_list(self):
        self.init_job_dict()
        for i in range((self.stop)+1):
            jobs_i = self.jobs[self.jobs.seconds == i]
            if jobs_i.shape[0] > 0:
                for index, job_i in jobs_i.iterrows():
                    yaml = job_i['yaml']
                    os.popen('kubectl '+'apply '+'-f '+yaml)
            self.renew_job_dict(self.job_dict,self.job_plans)
            time.sleep(1)
    # initialize job_dict : add all current nodes into this dict
    def init_job_dict(self):
        job_dict = {}
        node_info = os.popen("kubectl get nodes").read().split("\n")
        for i in range(1,len(node_info)-1):
            line = node_info[i]
            line = " ".join(line.split())
            match = re.match(r"(.*) (.*) (.*) (.*) (.*)",line)
            node = match.group(1)
            job_dict[node] = []
            self.job_dict = job_dict
    # renew_job_dict : add value of pods with job_list
    def renew_job_dict(self,current_job_dict,job_plans):
        pair_info = os.popen("kubectl get pod -o=custom-columns=NODE:.spec.nodeName,NAME:.metadata.name").read().split("\n")
        for i in range(1,len(pair_info)-1):
            line = pair_info[i]
            line = " ".join(line.split())
            match = re.match(r"(.*) (.*)", line)
            node = match.group(1)
            pod = match.group(2)
            if (node in current_job_dict) and (pod in job_plans):
                if pod not in current_job_dict[node]:
                    current_job_dict[node].append(pod)
    def get_job_dict(self):
        return self.job_dict


def init_job_time(scheduler):
    jobs = pd.read_csv(scheduler)
    job_time = [i for i in jobs["seconds"]]
    job_name = [i for i in jobs["name"]]
    job_time_dict = {}
    for i in range(len(job_time)):
        job_time_dict[job_name[i]] = [job_time[i]]
    monitor_list = job_name
    return job_time_dict,monitor_list

def check_complete(job_time_dict,monitor_list,init_time):
    while True:
        pod_info = os.popen("kubectl get pods").read().split("\n")
        if len(monitor_list) ==0:
            return job_time_dict
        for i in range(1,len(pod_info)-1):
            line = pod_info[i]
            line = " ".join(line.split())
            match = re.match(r"(.*) (.*) (.*) (.*) (.*)", line)
            pod = match.group(1)
            status = match.group(3)
            if pod in monitor_list and status == "Completed":
                job_time_dict[pod].append(float(time.time()))
                monitor_list.remove(pod)

def get_pod_time_dic():
    info = pd.read_csv("./scheduler.csv")
    jobs = {}
    for i in range(info.shape[0]):
        job = info.loc[i,"name"]
        second = int(info.loc[i,"seconds"])
        yaml = info.loc[i,"yaml"]
        jobs[job] = second
    return jobs



class run_job(Thread):
    def __init__(self, max):
        Thread.__init__(self)
        self.max = max

    def run(self):
        jobs = get_jobs()
        run_jobs(jobs,self.max)


class record_time(Thread):
    def __init__(self,init_time):
        Thread.__init__(self)
        self.result = {}
        self.init_time = init_time
    def run(self):
        job_time_dict, monitor_list = init_job_time("scheduler.csv")
        self.result = check_complete(job_time_dict,monitor_list,self.init_time)

    def get_result(self):
        return self.result


if __name__ == "__main__":
    max = int(sys.argv[1])
    init_log = open("./init_time.txt","w")
    init_time = time.time()
    init_log.write(str(init_time))
    init_log.close()
    jobs = get_jobs()
    run_jobs(jobs,max)
    pod_time_dict = get_pod_time_dic()
    print("node,job,start_time,init_time")
    pod_node_pair = get_pod_node_pair()
    for pod in pod_node_pair:
        node_name = pod_node_pair[pod]
        node_name = node_name.replace(".", " ")
        match = re.match(r"(.*) (.*) (.*) (.*) (.*) (.*)", node_name)
        node_name = match.group(1)
        print("{},{},{},{}".format(node_name, pod, str(pod_time_dict[pod]),str(init_time)))
