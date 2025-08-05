import os
import lib.dx as dx

dx.reload()

data_dir = dx.io.get_data_dir()
bravo_uri = os.getenv("BRAVO_URI")
bravo_log_uri = os.getenv("BRAVO_LOG_URI")
sm_uri = os.getenv("SM_URI")

mobiwork_login_base_url = "http://servicesdms.adiagri.com"
mobiwork_visit_base_url = "https://dms.adiagri.com:3052"
mobiwork_email = os.getenv("DMS_USER_EMAIL")
mobiwork_password = os.getenv("DMS_USER_PASSWORD")
