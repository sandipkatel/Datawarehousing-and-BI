from lib.Variables import Variables
import datetime
from os import path
v=Variables()
v.set("script_name","Nightly_Batch")

class Logger:
    def log_initialize():
        import os
        log_path = v.get("log_path")
        script_name = v.get("script_name")
        current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        log_file_name = str(script_name) + "_" + current_time + ".log"
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        log_file = path.join(log_path, log_file_name)
        log_file = open(log_file, 'w')
        v.set("log_file", log_file_name)
        v.set("log_file_handle", log_file)
        v.set("log_cur_datetime", current_time)

    def log_message(msg):
        now = datetime.datetime.now()
        log_file=v.get("log_file_handle")
        if log_file is None:
            Logger.log_initialize()
            log_file = v.get("log_file_handle")
        msg = str(msg)
        # print(msg)
        log_file.write(str(now))
        log_file.write(": ")
        log_file.write(msg)
        log_file.write("\n")
        log_file.flush()

    def close():
        log_file = v.get("log_file_handle")
        log_file.close()