import Scheduler
from Config import Config
# from threading import Thread
# import time

def main():
    scheduler = Scheduler.Scheduler(Config.MOVIE_WITH_ATTR, Config.MOVIE_WITH_ATTR, 0)
    scheduler.run()


if __name__ == '__main__':
    main()


# class myThread(Thread):
#     def __init__(self, src, dest, tag):
#         Thread.__init__(self)
#         self.src = src
#         self.dest = dest
#         self.tag = tag
#
#     def run(self):
#         print("开始", (self.tag+1), "号线程")
#         my_spider = Scheduler.Scheduler(self.src, self.dest, self.tag)
#         my_spider.schedule()
#
#
# my_threads = []
#
# for i in range(14, 16):
#     my_thread = myThread(Config.ATTRS[i], Config.MOVIE_WITH_ATTR, i)
#     my_threads.append(my_thread)
#     my_threads[i-14].start()
#     time.sleep(2)