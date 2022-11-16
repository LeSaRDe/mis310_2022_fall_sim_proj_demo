
from machine_zoo import MachineZoo
from job_repo import JobRepo

class MachineMan:

    m_mchn_zoo = None
    m_ref_job_repo = None

    def __init__(self):
        # TODO
        m_mchn_zoo = MachineZoo()
        pass

    def set_ref_job_repo(self, ref_job_repo):
        if ref_job_repo is None or type(ref_job_repo) is not JobRepo:
            raise Exception('[MachineMan:set_ref_job_repo] ref_job_repo is invalid!s')
        self.m_ref_job_repo = ref_job_repo


    def update_mchn_statuses(self):
        # TODO

        for mid in self.m_mchn_zoo.get_mchns():
            # Get machine profile for mid
            tol_cpu =
            tol_mem =

            # Get job profiles of jobs running on mid
            l_jpros = self.m_ref_job_repo.get_job_statuses_by_mid(mid)
            sum_cur_cpu = 0
            sum_cur_mem = 0
            for jpro in l_jpros:
                act_cpu = jpro['act_cpu']
                sum_cur_cpu += act_cpu
                act_mem = jpro['act_mem']
                sum_cur_mem += act_mem

            # Determine if the machine is overwhelmed
            if sum_cur_mem > tol_mem:
                # Set this machine to be down.

            elif sum_cur_cpu > tol_cpu:
                # Recompute the running time of each running job.
                # Or, set this machine to be down.

            # Update available computing resources
            avl_cpu = tol_cpu - sum_cur_cpu
            avl_mem = tol_mem - sum_cur_mem
            self.m_mchn_zoo.update(mid, ['avl_cpu', 'avl_mem'], [avl_cpu, avl_mem])