
import numpy as np
from numpy import random

from job_repo import JobRepo
from scheduler import Scheduler
from hyperparams import *


class JobGen:

    # The reference of the instance of Scheduler.
    m_ref_sched = None
    m_ref_job_repo = None
    m_cur_job_id = None

    def __init__(self):
        # TODO
        m_cur_job_id = 0
        pass

    def set_ref_sched(self, ref_sched):
        if ref_sched is None or type(ref_sched) != Scheduler:
            raise Exception('[JobGen:set_ref_sched] ref_sched is invalid!')

        self.m_ref_sched = ref_sched

    def set_ref_job_repo(self, ref_job_repo):
        # TODO
        if ref_job_repo is None or type(ref_job_repo) != JobRepo:
            raise Exception('[set_ref_job_repo] ref_job_repo is invalid!')

        self.m_ref_job_repo = ref_job_repo

    def gen_job_id(self):
        # TODO
        #   Only applicable in single-thread impl.
        self.m_cur_job_id += 1
        return self.m_cur_job_id

    def job_gen(self, lam=10):
        """
        This function generates a set of new job requests for each iteration. And enqueues new jobs into the job waiting queue.
        :return: (int) The number of generated jobs.
        """
        # Generate the number of new jobs
        n_jobs = int(random.poisson(lam=g_JobGen_njobs_poisson_lam))
        if n_jobs < 1:
            n_jobs = 1
        # Generate each job request
        l_job_req = []
        for i in range(n_jobs):
            # Generate Job ID:
            jid = self.gen_job_id()

            # Generate CPU request:
            #   - Step #1: Generate CPU Estimation Ratio
            #   - Step #2: Generate the range of actually used CPU
            #   - Step #3: Request CPU = CPU Estimation Ratio * Max CPU Usage(or Min CPU Usage)

            # Step #1
            #   We utilize the Beta distribution. The range of Beta samples is [0, 1]. <=0.5 represents underestimated, and >0.5 represents overestimated. Underestimated samples are scaled to [g_JobGen_cpu_est_rat_low, 1], and overestimated samples are scaled to [1, g_JobGen_cpu_est_rat_high].
            is_overest = True
            cpu_est_rat = random.beta(a=g_JobGen_cpu_est_rat_beta_a, b=g_JobGen_cpu_est_rat_beta_b)
            if cpu_est_rat <= 0.5:
                cpu_est_rat = g_JobGen_cpu_est_rat_low + (cpu_est_rat - 0.0) * (1.0 - g_JobGen_cpu_est_rat_low) / 0.5
                is_overest = False
            else:
                cpu_est_rat = 1.0 + (cpu_est_rat - 0.5) * (g_JobGen_cpu_est_rat_high - 1.0) / (1.0 - 0.5)

            # Step #2
            #   Draw two samples from a Beta. The lower bound of the CPU usage cannot be lower than 1.
            cpu_range = random.beta(a=g_JobGen_cpu_range_beta_a, b=g_JobGen_cpu_range_beta_b, size=2)
            cpu_range = np.round(cpu_range * g_JobGen_cpu_range_max)
            cpu_range_low = np.min(cpu_range)
            if cpu_range_low < 1:
                cpu_range_low = 1
            cpu_range_high = np.max(cpu_range)
            if cpu_range_high < 1:
                cpu_range_high = 1

            # Step #3:
            # Overestimated jobs may request CPUs more than its CPU usage upper bound, and underestimated jobs may request CPUs less than its CPU usage lower bound.
            if is_overest:
                cpu_req = np.round(cpu_est_rat * cpu_range_high)
            else:
                cpu_req = np.round(cpu_est_rat * cpu_range_low)
            if cpu_req < 1:
                cpu_req = 1

            # Generate Memory request
            is_overest = True
            mem_est_rat = random.beta(a=g_JobGen_mem_est_rat_beta_a, b=g_JobGen_mem_est_rat_beta_b, size=2)
            if mem_est_rat <= 0.5:
                mem_est_rat = g_JobGen_mem_est_rat_low + (cpu_est_rat - 0.0) * (1.0 - g_JobGen_mem_est_rat_low) / 0.5
                is_overest = False
            else:
                mem_est_rat = 1.0 + (cpu_est_rat - 0.5) * (g_JobGen_mem_est_rat_high - 1.0) / (1.0 - 0.5)

            mem_range = random.beta(a=g_JobGen_mem_range_beta_a, b=g_JobGen_mem_range_beta_b, size=2)
            mem_range = np.round(mem_range * g_JobGen_cpu_range_max)
            mem_range_low = np.min(mem_range)
            if mem_range_low < 1:
                mem_range_low = 1
            mem_range_high = np.max(mem_range)
            if mem_range_high < 1:
                mem_range_high = 1

            if is_overest:
                mem_req = np.round(mem_est_rat * mem_range_high)
            else:
                mem_req = np.round(mem_est_rat * mem_range_low)
            if cpu_req < 1:
                mem_req = 1

            # Generate Expected Running Time
            exp_rt = random.beta(a=g_JobGen_rt_beta_a, b=g_JobGen_rt_beta_b)
            exp_rt = g_JobGen_rt_low + (exp_rt - 0.0) * (g_JobGen_rt_high - g_JobGen_rt_low) / 1.0
            exp_rt = np.round(exp_rt)

            # Compose a job request
            job_req = [jid, cpu_req, mem_req, exp_rt, cpu_range_high, cpu_range_low, mem_range_high, mem_range_low]

            l_job_req.append(job_req)

        # Add jobs to Job Repo.
        self.m_ref_job_repo.add_jobs(l_job_req)

        # Enqueue new job requests to the job waiting queue.
        self.m_ref_sched.add_wait_jobs(l_job_req)