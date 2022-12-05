import time

from numpy import random

from scheduler import Scheduler
from job_repo import JobRepo


class JobRun:
    """
    This class is responsible for updating statuses for all running jobs.
    """
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

    def set_ref_job_repo(self, ref_job_repo):
        if ref_job_repo is None or type(ref_job_repo) is not JobRepo:
            raise Exception('[JobRun:set_ref_job_repo] ref_job_repo is invalid!')
        self.m_ref_job_repo = ref_job_repo

    def update_run_job_statuses(self):
        # TODO
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

    def add_run_jobs(self, l_job_prof):
        """
        Add a list of new running jobs into JobRun.
        :param l_job_prof: (list of tuples) The list of new running Job profiles.
        :return: (int) The number of new running jobs added into JobRun.
        """
        if l_job_prof is None or len(l_job_prof) <= 0:
            return 0
        # Update the jobs with JobRepo
        # TODO
        # Add in new running Job IDs.
        self.m_l_run_job_ids += [job_prof[0] for job_prof in l_job_prof]
        return len(l_job_prof)