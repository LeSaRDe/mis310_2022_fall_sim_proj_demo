import time

from PriorityQueue import PriQ
from job_repo import JobRepo
from machine_man import MachineMan
from job_run import JobRun
from hyperparams import JOB_STATE

class Scheduler:

    # Job waiting queue
    m_wait_jobs = None
    # JobRepo reference
    m_ref_job_repo = None
    # MachineMan reference
    m_ref_mchn_man = None
    # JobRun reference
    m_ref_job_run = None

    def __init__(self):
        # TODO
        self.m_wait_jobs = PriQ()
        pass

    def set_ref_job_repo(self, ref_job_repo):
        if ref_job_repo is None or type(ref_job_repo) != JobRepo:
            raise Exception('[Scheduler:set_ref_job_repo] ref_job_repo is invalid!')
        self.m_ref_job_repo = ref_job_repo

    def set_ref_mchn_man(self, ref_mchn_man):
        if ref_mchn_man is None or type(ref_mchn_man) != MachineMan:
            raise Exception('[Scheduler:set_ref_mchn_man] ref_mchn_man is invalid!')
        self.m_ref_mchn_man = ref_mchn_man

    def set_ref_job_run(self, ref_job_run):
        if ref_job_run is None or type(ref_job_run) != JobRun:
            raise Exception('[Scheduler:set_ref_job_run] ref_job_run is invalid!')
        self.m_ref_job_run = ref_job_run

    def job_sched(self):
        """
        Try to place waiting jobs to machines.
        :return: (int) The number of jobs that have been placed to machines for running.
        """
        l_wait_job_id = [job[0] for job in self.m_wait_jobs.get_items_by_pri()]
        # TODO
        #   Determine what job attributes should be considered here.
        l_attr = ['cpu_req', 'mem_req']
        l_job_prof = self.m_ref_job_repo.query_jobs_by_jids(l_wait_job_id, l_attr)
        for job_prof in l_job_prof:
            mid = self.m_ref_mchn_man.find_mchn_for_job(job_prof)
            if mid is not None:
                job_prof = {'jid': job_prof[0], 'mid': mid, 'start_t': time.time_ns(), 'state': JOB_STATE.RUN}
                # Tell JobRun that there is a new running job
                self.m_ref_job_run.add_run_jobs([job_prof])

    def compute_priority(self, job_req):
        """
        Compute the priority for a given job request.
        :param job_req: (tuple) Job request fields excluding Job ID.
        :return: (int) The priority.
        """
        # TODO
        return 0

    def add_wait_jobs(self, l_job_req):
        """
        Add new jobs into the job waiting queue.
        :param l_job_req: (list of tuples)
        :return: (int) The number of enqueued jobs.
        """
        # Add new jobs to Job Repo.
        self.m_ref_job_repo.add_jobs(l_job_req)
        # Add job IDs and priorities to the job waiting queue.
        l_new_wait_job = [(job_req[0], self.compute_priority(job_req[1:])) for job_req in l_job_req]
        self.m_wait_jobs.insert_batch(l_new_wait_job)

    def get_wait_job_ids(self):
        """
        Retrieve job ids for all waiting jobs.
        :return: (list of int)
        """
        return self.m_wait_jobs.get_job_ids()

    def update_wait_job_statuses(self):
        """
        Update the waiting time for each waiting job.
        :return: (int) The number of updated jobs.
        """
        if len(self.m_wait_jobs.get_job_ids()) <= 0:
            return 0

        s_entry_t = self.m_ref_job_repo.query_jobs_by_jids(self.m_wait_jobs.get_job_ids(), ['entry_t'])
        cur_t = time.time_ns()
        return self.m_ref_job_repo.update_jobs(self.m_wait_jobs.get_job_ids(), ['wait_t'], cur_t - s_entry_t)

