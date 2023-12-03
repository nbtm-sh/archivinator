import archivinator, time

FETCHERS_DIR = "./fetchers"

def test_get_fetchers():
    fetcher_manager = archivinator.FetcherManager(FETCHERS_DIR)

    for fetcher in fetcher_manager.get_fetchers():
        print(fetcher.serialise())

def test_load_fetcher():
    fetcher_manager = archivinator.FetcherManager(FETCHERS_DIR)
    fetcher = fetcher_manager.get_fetchers()[0]

    test_fetcher = fetcher.load()
    print(test_fetcher)

def test_follow_path():
    test_input_dict = {
        "global": {
            "app": {
                "dummy": {
                    "apikey": "test"
                }
            }
        }
    }
    test_path = "global.app.dummy.apikey"

    print(archivinator.VariableParser.follow_path(test_path, test_input_dict))

def test_eval_variable_string():
    test_global_config = {
        "dataprefix": "/Users/nathan/Downloads/Data",
        "bind_addr": "0.0.0.0:8080"
    }
    test_app_config = {
        "apikey": "demokey"
    }
    variable_string = "${global.dataprefix}/test.txt"
    print(archivinator.VariableParser.eval_variable_string(variable_string, test_global_config, test_app_config))

    variable_string = "https://${global.bind_addr}${global.dataprefix}/Test.py"
    print(archivinator.VariableParser.eval_variable_string(variable_string, test_global_config, test_app_config))

def test_start_job_manager():
    job_manager = archivinator.JobManager()
    job_manager.start_queue_manager()
    time.sleep(1)

def test_job_queue():
    fetcher_manager = archivinator.FetcherManager(FETCHERS_DIR)
    fetcher = fetcher_manager.get_fetchers()[0]

    job_manager = archivinator.JobManager()
    job_manager.start_queue_manager()

    job_manager.queue_job(fetcher)

    for i in range(3):
        time.sleep(1)

def test_multi_job_queue():
    fetcher_manager = archivinator.FetcherManager(FETCHERS_DIR)
    fetcher = fetcher_manager.get_fetchers()[0]

    job_manager = archivinator.JobManager()
    job_manager.start_queue_manager()

    job_manager.queue_job(fetcher)
    job_manager.queue_job(fetcher)

    for i in range(3):
        time.sleep(1)


if __name__ == "__main__":
    test_get_fetchers()
    test_load_fetcher()
    test_follow_path()
    test_eval_variable_string()
    test_start_job_manager()
    test_job_queue()
    test_multi_job_queue()