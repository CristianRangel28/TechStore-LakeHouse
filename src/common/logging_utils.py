"""Adaptador del logger de AWS Glue a la interfaz de logging de Python."""


class GlueLoggerAdapter:
    def __init__(self, glue_logger):
        self._glue_logger = glue_logger

    @staticmethod
    def _format(msg, args):
        if args:
            try:
                return str(msg % args)
            except Exception:
                pass
        return str(msg)

    def info(self, msg, *args):
        self._glue_logger.info(self._format(msg, args))

    def warning(self, msg, *args):
        self._glue_logger.warn(self._format(msg, args))

    def debug(self, msg, *args):
        self._glue_logger.info(self._format(msg, args))

    def error(self, msg, *args):
        self._glue_logger.error(self._format(msg, args))

    def exception(self, msg, *args):
        self._glue_logger.error(self._format(msg, args))
