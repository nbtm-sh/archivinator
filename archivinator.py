import yaml, os, sys, re, threading, time, datetime

class Fetcher:
    _job_id = -1
    _job_name = ""
    _job_status = None
    _job_started_at = None 
    _job_completed_at = None

    JOB_STATUS_RUNNING = "running"
    JOB_STATUS_FINISHED = "finished"
    JOB_STATUS_FAILED = "failed"
    JOB_STATUS_PAUSED = "paused"

    def start(self, **kwargs):
        pass

    def stop(self, **kwargs):
        pass

    def pause(self, **kwargs):
        pass

    def complete(self):
        self._job_status = Fetcher.JOB_STATUS_FINISHED
        self._job_completed_at = datetime.datetime.now()

class VariableParser:
    @staticmethod
    def follow_path(path, path_dict):
        output = path_dict
        path = path.split(".")

        while len(path) > 0:
            output = output[path[0]]
            path.pop(0)
        
        return output
    
    @staticmethod
    def eval_variable_string(variable_string, global_config, app_variables):
        app_variables["global"] = global_config
        variable_regex = r"\$\{([^\$]*)\}"
        variable_regex = re.compile(variable_regex)
        max_variables = 100

        while max_variables != 0:
            max_variables -= 1
            match = re.search(variable_regex, variable_string)
            if match:
                variable = match.group(1)
                resolved_variable = VariableParser.follow_path(variable, app_variables)
                replace_string = "${%s}" % variable 
                variable_string = variable_string.replace(replace_string, resolved_variable) 
            else:
                break
        
        return variable_string


class JobManager:
    def __init__(self):
        self.max_jobs_running = 1
        self.job_queue = []
        self.jobs_running = []
        self.jobs_completed = []
        self.queue_manager_running = False
        self.queue_manager_thread = None

        self.id_counter = 1_000_000
    
    def start_queue_manager(self):
        if self.queue_manager_thread is None:
            self.queue_manager_thread = threading.Thread(target=JobManager._queue_manager, args=(self,))
            self.queue_manager_running = True
            self.queue_manager_thread.start()
    
    def queue_job(self, job, **kwargs):
        self.job_queue.append([job, kwargs])
        self.id_counter += 1

    @staticmethod
    def count_job_status(job_list, status):
        # im like 99% sure theres a 1liner for this
        count = 0
        for job in job_list:
            count += int(job[0]._job_status == status)
        
        return count
    
    def queue_job(self, job, **kwargs):
        self.job_queue.append([job, kwargs])

    @staticmethod
    def _queue_manager(self):
        # yes yes i know self in static method sdklfjsldkfjslkdf
        while True:
            time.sleep(1)
            if len(self.jobs_running) < self.max_jobs_running and len(self.job_queue) > 0:
                queued_job = self.job_queue[0]
                self.job_queue.pop(0)

                queued_job[0] = queued_job[0].load().FetcherModule()
                queued_job[0]._job_id = self.id_counter
                queued_job[0]._job_stauts = Fetcher.JOB_STATUS_RUNNING
                queued_job[0]._job_started_at = datetime.datetime.now()

                self.jobs_running.insert(0, queued_job)
                job_thread = threading.Thread(target=self.jobs_running[0][0].start, args=(self.jobs_running[0][1]))
                job_thread.start()
                self.jobs_running[0].extend([job_thread])

                self.id_counter = self.id_counter + 1 
                print("Job started: " + str(self.id_counter - 1))
            
            for job, index in zip(self.jobs_running, range(len(self.jobs_running))):
                if job[0]._job_status == Fetcher.JOB_STATUS_FINISHED:
                    move_job = job
                    self.jobs_running.pop(index)
                    self.jobs_completed.insert(0, move_job)
                    print("Job completed: " + str(move_job[0]._job_id))
                    break
        


class FetcherMetadata:
    def __init__(self, fetcher_manager):
        self.name = ""
        self.display_name = ""
        self.version = ""
        self.inputs = []
        self.path = ""
        self.fetcher_manager = fetcher_manager

    class InvalidFetcher(Exception):
        pass
    
    @staticmethod
    def test_fetcher(fetcher):
        should_have = [
            "_job_id",
            "_job_name",
            "_job_status",
            "_job_started_at",
            "_job_completed_at",
            "start",
            "stop",
            "pause"
        ]

        fetcher = fetcher.Fetcher()

        for test_should_have in should_have:
            if test_should_have not in dir(fetcher):
                raise FetcherMetadata.InvalidFetcher(f"{test_should_have} missing from fetcher")
        
        del fetcher

        return True
    
    def load(self):
        if not self.path.startswith('/'):
            self.path = os.path.join(os.getcwd(), self.path)
        
        sys.path.append(self.path)

        loaded_fetcher = __import__(self.name)
        FetcherMetadata.test_fetcher(loaded_fetcher)
        return loaded_fetcher
    
    class Input:
        def __init__(self):
            self.name = ""
            self.display_name = ""
            self.type = None
            self.default_value = ""
        
        @staticmethod
        def convert_string_to_type(type_string):
            mappings = {
                "string": str,
                "int": int,
                "float": float,
                "bool": bool
            }
            if type_string not in mappings.keys():
                raise ValueError("invalid type string")
            
            return mappings[type_string]
        
        @staticmethod
        def convert_type_to_string(type_):
            mappings = {
                str: "string",
                int: "int",
                float: "float",
                bool: "bool"
            }

            if type_ not in mappings.keys():
                raise ValueError("invalid type")
            
            return mappings[type_]
        
        def serialise(self):
            return {
                "name": self.name,
                "display_name": self.display_name,
                "type": FetcherMetadata.Input.convert_type_to_string(self.type),
                "default_value": self.default_value
            }
        
    def serialise(self):
        return {
            "name": self.name,
            "display_name": self.display_name,
            "version": self.version,
            "inputs": self.inputs,
            "path": self.path,
            "inputs": [i.serialise() for i in self.inputs]
        }

class FetcherManager:
    def __init__(self, fetchers_path):
        self.fetchers_path = fetchers_path
    
    def get_fetchers(self):
        fetcher_directories = os.listdir(self.fetchers_path)
        fetchers_output = []

        for directory in fetcher_directories:
            with open(os.path.join(self.fetchers_path, directory, "manifest.yaml")) as manifest_fp:
                fetcher_manifest = yaml.safe_load(manifest_fp)

                fetcher_metadata_object = FetcherMetadata(self)
                fetcher_metadata_object.name = fetcher_manifest["fetcher"]["name"]
                fetcher_metadata_object.display_name = fetcher_manifest["fetcher"]["displayname"]
                fetcher_metadata_object.version = fetcher_manifest["fetcher"]["version"]
                fetcher_metadata_object.path = os.path.join(self.fetchers_path, directory)

                for input in fetcher_manifest["fetcher"]["inputs"]:
                    input_object = FetcherMetadata.Input()
                    input_object.name = input["name"]
                    input_object.display_name = input["displayname"]
                    input_object.type = FetcherMetadata.Input.convert_string_to_type(input["type"])
                    input_object.default_value = input["defaultvalue"]
                    
                    fetcher_metadata_object.inputs.append(input_object)

                fetchers_output.append(fetcher_metadata_object)
        
        return fetchers_output