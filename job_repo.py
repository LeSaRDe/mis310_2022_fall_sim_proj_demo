import logging
import time

import numpy as np
import pandas as pd

from hyperparams import JOB_STATE


class JobRepo:
    """
    This class is used to store all jobs.
    """
    # Job DataFrame
    m_job_df = None
    m_col_name = ['jid', 'mid', 'entry_t', 'start_t', 'wait_t', 'cpu_req', 'mem_req', 'exp_rt', 'state',
                  'cpu_range_low', 'cpu_range_high', 'mem_range_low', 'mem_range_high', 'act_cpu', 'act_mem']

    def __init__(self):
        self.m_job_df = pd.DataFrame([], columns=self.m_col_name)
        self.m_job_df = self.m_job_df.set_index(self.m_col_name[0])

    def add_jobs(self, l_job_req):
        """
        Add in new jobs from job requests.
        :param l_job_req: (list) The list of job requests
        :return: (int) The number of jobs successfully added.
        """
        # TODO
        l_job = []
        entry_t = time.time_ns()
        for job_req in l_job_req:
            jid, cpu_req, mem_req, exp_rt, cpu_range_high, cpu_range_low, mem_range_high, mem_range_low = job_req
            l_job.append([jid, entry_t, np.nan, 0, cpu_req, mem_req, exp_rt, JOB_STATE.WAIT,
                          cpu_range_low, cpu_range_high, mem_range_low, mem_range_high, np.nan, np.nan])
        df_new_job = pd.DataFrame(l_job, columns=self.m_col_name)
        df_new_job = df_new_job.set_index(self.m_col_name[0])
        self.m_job_df = pd.concat([self.m_job_df, df_new_job])
        if self.m_job_df.index.has_duplicates:
            raise Exception('[JobRepo:add_jobs] Duplicate indexes occur!')
        return len(df_new_job)

    def query_jobs_by_jids(self, l_jid, l_attr):
        """
        Query the profiles of multiple jobs.
        :param l_jid: (list of int) The list of Job IDs.
        :param l_attr: (list of str) List of attribute names.
        :return: (list of dict or None) The list of data records. Each record corresponds to a jid w.r.t. l_attr.
        """
        if l_jid is None or len(l_jid) == 0:
            return None
        for jid in l_jid:
            if not jid in self.m_job_df:
                raise Exception('[JobRepo:query_jobs_by_jids] jid: %s is not in JobRepo!' % jid)
        if l_attr is None or len(l_attr) <= 0:
            raise Exception('[JobRepo:query_jobs_by_jids] l_attr is invalid!')
        return self.m_job_df.loc[l_jid][l_attr].to_dict(orient='records')

    def update_jobs(self, l_jid, l_attr, new_vals):
        """
        Update the profile of a job by giving pairs of (attribute name, attribute value).
        :param: l_jid: (list of int) The list of Job IDs.
        :param l_attr: (list of str) The list of job profile attributes for the update.
        :param new_vals: (list (1-D) or list of list (>1-D)) Each element list gives the new values for the corresponding jid w.r.t. l_attr.
        :return: (int or None) The number of jobs that have been updated.
        """
        if l_jid is None or len(l_jid) == 0:
            return None
        if l_attr is None or l_attr is None or len(l_attr) == 0 or len(new_vals) == 0 or len(l_jid) != len(new_vals):
            raise Exception('[JobRepo:update_jobs] l_attr and l_val are invalid!')

        try:
            self.m_job_df.loc[l_jid, l_attr] = new_vals
        except Exception as err:
            raise Exception('[JobRepo:update_jobs] %s' % err)
        return len(l_jid)

    def query_jobs_by_mid(self, mid, l_attr):
        """
        Retrieve the profiles of jobs running on the machine `mid`.
        :param mid: (int) Machine ID.
        :param l_attr: (list of str) Job profile attributes in consideration.
        :return: (list of dicts or None) The list of job profiles. Each element is a record for a job w.r.t. l_attr.
        """
        if mid not in self.m_job_df['mid'].values:
            raise Exception('[JobRepo:get_jobs_by_mid] mid %s is not in JobRepo!')
        if l_attr is None or len(l_attr) == 0:
            return None

        return self.m_job_df[self.m_job_df['mid'] == mid][l_attr].to_dict(orient='records')


