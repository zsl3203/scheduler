import time
import sys

from kubernetes import client, config, watch

config.load_kube_config()
v1 = client.CoreV1Api()

f = open("end_time.csv","w")
f.write("job,end_time\n")

def record_time():
    completed_jobs = []
    while True:
        total_pods = v1.list_namespaced_pod(namespace="default").items
        for pod in total_pods:
            if pod.metadata.name not in completed_jobs:
                if pod.status.phase == "Succeeded":
                    f.write("{},{}\n".format(pod.metadata.name,str(time.time())))
                    f.flush()
                    completed_jobs.append(pod.metadata.name)

if __name__ == "__main__":
    record_time()
