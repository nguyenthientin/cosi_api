"""
This file contains the jobmanager that starts jobs, keeps track of them
and terminates them if requested
"""

import time
import io
import logging
from collections import OrderedDict
from logging.handlers import QueueListener
from multiprocessing import Lock, Queue, Process
from queue import Empty
import services

MAX_RECENT = 10

class JobManager():
    """
    The main jobmanager functionality
    """
    def __init__(self):
        self.lock = Lock()
        self.running_jobs = OrderedDict()
        self.recent_jobs = OrderedDict()
        self.job_count = 0

    def run_job(self, data):
        """
        start a job in a separate process, regularly checking that
        it is still running
        """
        try:
            run_worker = services.services[data["service"]]
        except KeyError:
            return {"error": "unknown service '%s'" % data["service"]}

        # setup logging destination
        log_output = io.StringIO()
        log_handler = logging.StreamHandler(stream=log_output)
        formatter = logging.Formatter("%(asctime)s " + data["service"] + ": %(message)s")
        formatter.converter = time.localtime
        log_handler.setFormatter(formatter)

        # setup logging transfer from the worker process to here
        log_queue = Queue()
        listener = QueueListener(log_queue, log_handler)
        listener.start()

        queue = Queue()
        job = Process(target=run_worker, args=(data, queue, log_queue))
        start = time.time()

        self.lock.acquire()
        try:
            if not self.running_jobs and self.job_count > 9999:
                # reset the job counter
                self.job_count = 0
            self.job_count += 1
            job_id = self.job_count
            self.running_jobs[job_id] = (job, data, start, log_output)
        finally:
            self.lock.release()

        job.start()
        while True:
            try:
                result = queue.get(timeout=2)
            except Empty:
                if not job.is_alive():
                    result = {"error": "job terminated"}
                    break
            else:
                break

        if "error" in result:
            # TODO: also add log to successful jobs?
            result["log"] = log_output.getvalue()
        else:
            # if there were no errors the result is the requested data
            result = {"data": result}

        self.lock.acquire()
        try:
            duration = time.time() - start
            self.recent_jobs[job_id] = (data, duration, log_output)
            if len(self.recent_jobs) > MAX_RECENT:
                # delete the first item if the list becomes too long
                first_item = next(iter(self.recent_jobs))
                del self.recent_jobs[first_item]
            del self.running_jobs[job_id]
        finally:
            self.lock.release()

        return result

    def list_jobs(self):
        """
        generate a list of running jobs and services
        """
        jobs = []
        services = {}
        recent = []
        self.lock.acquire()
        try:
            for job_id, (_, data, start, _) in self.running_jobs.items():
                run_time = time.time() - start
                service = data["service"]
                description = "Job %s: running %.2f s, service '%s'" % (job_id, run_time, service)
                jobs.append((job_id, description))
                if service in services:
                    services[service] += 1
                else:
                    services[service] = 1
            for job_id, (data, duration, _) in self.recent_jobs.items():
                service = data["service"]
                description = "Job %s: duration %.2f s, service '%s'" % (job_id, duration, service)
                recent.append((job_id, description))
        finally:
            self.lock.release()
        return jobs, services, recent

    def kill_job(self, job):
        """
        terminate a single job or all jobs of a service
        input argument 'job' can be an integer job id or a service name
        """
        try:
            job = int(job)
            result = "job id not found"
            what_to_kill = "id"
        except ValueError:
            result = "no jobs of this service"
            what_to_kill = "service"
        self.lock.acquire()
        try:
            for job_id, (job_process, data, _, _) in self.running_jobs.items():
                if what_to_kill == "id" and job_id == job:
                    if job_process.is_alive():
                        job_process.terminate()
                        result = "job terminated"
                    else:
                        result = "job no longer running"
                    break
                if data["service"] == job:
                    if job_process.is_alive():
                        job_process.terminate()
                        result = "job(s) terminated"
        finally:
            self.lock.release()
        return result

    def get_log(self, job_id):
        """
        get the logging belonging to a specific job
        """
        try:
            job_id = int(job_id)
        except ValueError:
            return "job id '%s' not found" % job_id

        self.lock.acquire()
        try:
            if job_id in self.running_jobs:
                (_, _, _, log_output) = self.running_jobs[job_id]
            elif job_id in self.recent_jobs:
                (_, _, log_output) = self.recent_jobs[job_id]
            else:
                raise KeyError()
            result = log_output.getvalue()
        except KeyError:
            result = "job id '%s' not found" % job_id
        finally:
            self.lock.release()
        return result

    # pylint: disable=no-self-use
    def get_info(self, service):
        """
        get the info page belonging to a specific service
        """
        try:
            info_text = services.info[service]()
        except KeyError:
            return {"error": "unknown service '%s'" % service}
        return info_text
