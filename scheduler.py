import time

from PriorityQueue import PriQ
from job_repo import JobRepo

class Scheduler:

    # Job waiting queue
    m_wait_jobs = None
    # JobRepo reference
    m_ref_job_repo = None

    def __init__(self):
        # TODO
        self.m_wait_jobs = PriQ()
        pass

    def set_ref_job_repo(self, ref_job_repo):
        if ref_job_repo is None or type(ref_job_repo) != JobRepo:
            raise Exception('[Scheduler:set_ref_job_repo] ref_job_repo is invalid!')
        self.m_ref_job_repo = ref_job_repo

    def job_sched(self):
        """
        Try to place waiting jobs to machines.
        :return: (int) The number of jobs that have been placed to machines for running.
        """
        # TODO
        pass

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

        s_start_t = self.m_ref_job_repo.query_jobs_by_jids(self.m_wait_jobs.get_job_ids(), ['start_t'])
        cur_t = time.time_ns()
        return self.m_ref_job_repo.update_jobs(self.m_wait_jobs.get_job_ids(), ['wait_t'], cur_t - s_start_t)

