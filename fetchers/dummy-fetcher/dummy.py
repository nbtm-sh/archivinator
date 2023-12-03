from archivinator import Fetcher
import time

class FetcherModule(Fetcher):
    def __init__(self):
        pass

    def start(self, **kwargs):
        print("Dummy running " + str(self._job_id))
        time.sleep(10)

        self.complete()
