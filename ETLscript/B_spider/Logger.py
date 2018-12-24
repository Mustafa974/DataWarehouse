import datetime


class Logger:
    """
    记录异常情况日志
    """
    def __init__(self, log_file_path='log.txt'):
        self.log_file = log_file_path

    def log(self, msg):
        file = open(self.log_file, 'a+')
        msg = '[' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '] ' + msg + '\n'
        file.write(msg)
        file.close()


class AmazonLogger(Logger):
    def __init__(self, log_file_path='log.txt', table_num=0):
        super().__init__(log_file_path)
        self.table_num = table_num

    def log_block(self):
        msg = '机器人验证多次:线程 ' + str(self.table_num) + ' 异常结束'
        self.log(msg)

    def log_404(self, url):
        msg = 'page not found ' + url
        self.log(msg)

    def log_robot(self):
        msg = '机器人验证 ' + str(self.table_num)
        self.log(msg)

    def log_finish(self):
        msg = '----finish---- ' + str(self.table_num)
        self.log(msg)

    def log_start(self, thread_num=None, loop=None):
        msg = '----START---- '
        if thread_num is not None:
            msg += str(thread_num) + ' threads * '
        if loop is not None:
            msg += str(loop) + ' loop ----'
            self.log(msg)
