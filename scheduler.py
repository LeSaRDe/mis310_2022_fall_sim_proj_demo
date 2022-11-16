
from job_wait_queue import JobWaitQ

class Scheduler:

    # Job waiting queue
    m_wait_jobs = None

    def __init__(self):
        # TODO
        self.m_wait_jobs = JobWaitQ()
        pass

    def job_sched(self):
        # TODO
        pass

    def add_jobs_to_wait_queue(self, l_job_req):
        """
        Add new jobs into the job waiting queue.
        :param l_job_req: (list of tuples)
        :return: (int) The number of enqueued jobs.
        """
        # TODO
        self.m_wait_jobs.add_job(l_job_req)


    def get_wait_job_ids(self):
        """
        Retrieve job ids for all waiting jobs.
        :return: (list of int)
        """
        # TODO
        pass
