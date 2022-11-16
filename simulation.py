################################################################################
#   Implementation Notes
#   1. Single-thread version
################################################################################
import logging

from job_repo import JobRepo
from job_gen import JobGen
from job_run import JobRun
from scheduler import Scheduler
from machine_man import MachineMan


class Simulation:
    """
    This class is a wrapper for the simulation framework.
    """
    # Job Repository
    m_job_repo = None
    # Job Generator
    m_job_gen = None
    # Job Runner
    m_job_run = None
    # Scheduler
    m_sched = None
    # Machine Manager
    m_mchn_man = None

    def __init__(self):
        # TODO
        # Create building blocks
        try:
            m_job_repo = JobRepo()
            m_job_gen = JobGen()
            m_job_run = JobRun()
            m_sched = Scheduler()
            m_mchn_man = MachineMan()

            # Set references
            m_job_gen.set_ref_sched(m_sched)

        except Exception as err:
            logging.error('[Simulation:__init__] Error occurred: %s' % err)
        pass

    def start(self):
        """
        This function carries the main loop for the simulation.
        :return: (None)
        """
        # TODO
        while True:
            # Generate new jobs and Enqueue those new jobs
            self.m_job_gen.job_gen()
            # Update statuses of all jobs
            self.m_job_run.update_job_statuses()
            # Update statuses of all machines
            self.m_mchn_man.update_mchn_statuses()
            # Schedule waiting jobs to machines
            self.m_sched.job_sched()
            # Log time series of job statuses and machine statuses

            # Check terminal conditions



