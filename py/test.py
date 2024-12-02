import sys
sys.path.append('./site-packages')

import numpy as np

from myLogger import set_logger, getLogger
set_logger()
logger = getLogger(__name__)

def test():
    a = np.array([1,2,3])
    b = np.array([4,5,6])

    print(a + b)
    print(sys.argv)

    logger.info("完了")