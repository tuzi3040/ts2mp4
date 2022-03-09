import json

class gcp_logentry_logging:
    __global_log_fields = {}

    class LOG_SEVERITY:
        severity = ""

    class LOG_DEFAULT(LOG_SEVERITY):
        severity = "DEFAULT"

    class LOG_DEBUG(LOG_SEVERITY):
        severity = "DEBUG"

    class LOG_INFO(LOG_SEVERITY):
        severity = "INFO"

    class LOG_NOTICE(LOG_SEVERITY):
        severity = "NOTICE"

    class LOG_WARNING(LOG_SEVERITY):
        severity = "WARNING"

    class LOG_ERROR(LOG_SEVERITY):
        severity = "ERROR"

    class LOG_CRITICAL(LOG_SEVERITY):
        severity = "CRITICAL"

    class LOG_ALERT(LOG_SEVERITY):
        severity = "ALERT"

    class LOG_EMERGENCY(LOG_SEVERITY):
        severity = "EMERGENCY"

    def __init__(self, from_gs_url: str, to_bucket: str):
        self.__global_log_fields['from_gs_url'] = from_gs_url
        self.__global_log_fields['to_bucket'] = to_bucket

    def __logger(self, severity: LOG_SEVERITY, message: str):
        entry = dict(
            severity = severity.severity,
            message = message,
            **self.__global_log_fields,
        )
        print(json.dumps(entry))

    def default(self, message: str):
        return self.__logger(self.LOG_DEFAULT, message)

    def debug(self, message: str):
        return self.__logger(self.LOG_DEBUG, message)

    def info(self, message: str):
        return self.__logger(self.LOG_INFO, message)

    def notice(self, message: str):
        return self.__logger(self.LOG_NOTICE, message)

    def warning(self, message: str):
        return self.__logger(self.LOG_WARNING, message)

    def error(self, message: str):
        return self.__logger(self.LOG_ERROR, message)

    def critical(self, message: str):
        return self.__logger(self.LOG_CRITICAL, message)

    def alert(self, message: str):
        return self.__logger(self.LOG_ALERT, message)

    def emergency(self, message: str):
        return self.__logger(self.LOG_EMERGENCY, message)
