from myLogger import set_logger, getLogger
import test
set_logger()
logger = getLogger(__name__)

# ログを書き込むとき
logger.debug("デバッグ用ログ")
logger.info("普通のログ")
logger.warning("やば目のログ")

test.test()