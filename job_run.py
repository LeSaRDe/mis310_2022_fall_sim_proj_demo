
import time

from numpy import random

from scheduler import Scheduler
from job_repo import JobRepo


class JobRun:

    m_ref_sched = None
    m_ref_job_repo = None
    m_l_run_job_ids = None

    def __init__(self):
        # TODO
        self.m_l_run_job_ids = []
        pass

    def set_ref_sched(self, ref_sched):
        if ref_sched is None or type(ref_sched) is not Scheduler:
            raise Exception('[JobRun:set_ref_sched] ref_sched is invalid!')
        self.m_ref_sched = ref_sched
        pass

    def set_ref_job_repo(self, ref_job_repo):
        if ref_job_repo is None or type(ref_job_repo) is not JobRepo:
            raise Exception('[JobRun:set_ref_job_repo] ref_job_repo is invalid!')
        self.m_ref_job_repo = ref_job_repo
        pass

    def update_job_statuses(self):
        # TODO

        # Step #1: Update statuses of all waiting jobs.
        l_wait_job_ids = self.m_ref_sched.get_wait_job_ids()
        # Update the waiting times of jobs
        for jid in l_wait_job_ids:
            # Retrieve the start time of job.
            start_t = self.m_ref_job_repo.get_job_statuses(jid, ['start_t'])
            # Compute the new waiting time.
            new_wait_t = time.time_ns() - start_t
            # Update the new waiting time.
            self.m_ref_job_repo.update_job_status(jid, ['wait_t'], [new_wait_t])

        # Step #2: Update statuses of all running jobs.
        for jid in self.m_l_run_job_ids:
            # Retrieve CPU usage range
            cpu_range = self.m_ref_job_repo.get_job_statuses(jid, ['cpu_range'])
            # Compute currently used CPU
            act_cpu = random.uniform(low=cpu_range[0], high=cpu_range[1], size=1)
            # Retrieve Memory usage range
            mem_range = self.m_ref_job_repo.get_job_statuses(jid, ['mem_range'])
            # Compute currently used Memory
            act_mem = random.uniform(low=mem_range[0], high=mem_range[1], size=1)
            # Update the status of job.
            self.m_ref_job_repo.update_job_status(jid, ['act_cpu', 'act_mem'], [act_cpu, act_mem])