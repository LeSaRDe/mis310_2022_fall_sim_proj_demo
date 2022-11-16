import time

import pandas as pd

class JobRepo:
    """
    This class is used to store all jobs.
    """
    # Job DataFrame
    m_job_df = None

    def __init__(self):
        m_job_df = pd.DataFrame([], columns=['jid', 'mid', 'start_t', 'cpu_req', 'mem_req', 'exp_rt', 'state', 'cpu_range', 'mem_range', 'wait_t', 'act_cpu', 'act_mem'])

    def add_jobs(self, l_job_req):
        """
        Add in new jobs.
        :param l_job_req: (list) The list of job requests
        :return: (int) The number of jobs successfully added.
        """
        # TODO
        start_t = time.time_ns()
        pass

    def query_job_by_id(self, jid):
        """
        Query the profile of a job by its job ID.
        :param jid: (int) Job ID.
        :return:
        """
        # TODO
        pass

    def query_jobs_by_ids(self, l_jid):
        """
        Query the profiles of multiple jobs.
        :param l_jid: (list of int) The list of Job IDs.
        :return:
        """
        # TODO
        pass

    def get_job_statuses(self, jid, l_jpro_attr):
        """
        Retrieve the job profile attribute values for a given jobs.
        :param jid: (int) Job ID.
        :param l_jpro_attr: (list of strs) The list of job profile attributes in consideration.
        :return: (list)
        """
        # TODO
        if l_jpro_attr is None or len(l_jpro_attr) == 0:
            raise Exception('[JobRepo:get_job_statuses] l_jpro_attr is invalid!')
        try:
            ret = list(self.m_job_df.loc[jid, l_jpro_attr])
            return ret
        except Exception as err:
            raise Exception('[JobRepo:get_job_statuses] %s' % err)


    def update_job_status(self, jid, l_jpro_attr, l_jpro_val):
        """
        Update the status of a job by giving pairs of (attribute name, attribute value).
        :param: jid: (int) Job ID.
        :param l_jpro_attr: (list of str) The list of job profile attributes for the update.
        :param l_jpro_val: (list) The list of values corresponding to the attributes.
        :return:
        """
        if l_jpro_attr is None or l_jpro_val is None or len(l_jpro_attr) == 0 or len(l_jpro_val) == 0 \
                or len(l_jpro_attr) != len(l_jpro_val):
            raise Exception('[JobRepo:update_job_status] Inputs are invalid!')

        try:
            self.m_job_df.loc[jid, l_jpro_attr] = l_jpro_val
        except Exception as err:
            raise Exception('[JobRepo:update_job_status] %s' % err)


    def get_job_statuses_by_mid(self, mid, l_jpro_attr):
        """
        Retrieve the profiles of jobs running on the machine `mid`.
        :param mid: (int) Machine ID.
        :param l_jpro_attr: (list of str) Job profile attributes in consideration.
        :return: (list of tuples) The list of job profiles.
        """
        # TODO
        df_ret = self.m_job_df[self.m_job_df['mid'] == mid][l_jpro_attr]
        # Convert df_ret to a list of tuples
        l_ret = []
        return l_ret


