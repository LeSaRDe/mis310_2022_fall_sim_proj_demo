import logging
import  sys
from simulation import Simulation



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    # Create the instance of Simulation
    ins_sim = Simulation()
    if ins_sim is not None:
        ins_sim.start()
    else:
        logging.error('[main] ins_sim is None!')
        sys.exit(-1)
