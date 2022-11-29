from enum import Enum

################################################################################
#   Job Generation
################################################################################
# Mean for the Poisson for the number of jobs to be generated
g_JobGen_njobs_poisson_lam = 10

# Parameters for the Beta for generating CPU estimated ratio for each job
g_JobGen_cpu_est_rat_beta_a = 2
g_JobGen_cpu_est_rat_beta_b = 2

# Scaling parameters for generating CPU estimated ratio for each job
g_JobGen_cpu_est_rat_low = 0.1
g_JobGen_cpu_est_rat_high = 10

# Parameters for the Beta for generating CPU usage range for each job
g_JobGen_cpu_range_beta_a = 2
g_JobGen_cpu_range_beta_b = 5

# The max for CPU usage range
g_JobGen_cpu_range_max = 40

# Parameters for the Beta for generating Memory estimated ratio for each job
g_JobGen_mem_est_rat_beta_a = 4
g_JobGen_mem_est_rat_beta_b = 2

# Scaling parameters for generating Memory estimated ratio for each job
g_JobGen_mem_est_rat_low = 0.1
g_JobGen_mem_est_rat_high = 10

# Parameters for the Beta for generating Memory usage range for each job
g_JobGen_mem_range_beta_a = 2
g_JobGen_mem_range_beta_b = 5

# Parameters for the Beta for generating the initial expected job running time
g_JobGen_rt_beta_a = 2
g_JobGen_rt_beta_b = 10

# Scaling parameters for generating the initial expected job running time
# The unit is second
g_JobGen_rt_low = 1
g_JobGen_rt_high = 24 * 60 * 60


################################################################################
#   Job Profile
################################################################################
class JOB_STATE(Enum):
    WAIT = 1
    RUN = 2
    DONE = 3
    TERM = 4