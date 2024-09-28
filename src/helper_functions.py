import os
import json

headers = json.loads(os.getenv("REQUIRED_HEADERS"))

class TriggerFile:
    def __init__(self, filename):
        self
        self.filename = filename
        self.name, self.status = os.path.splitext(filename)
        if not self.status == ".ok":
            raise ValueError("Only processes .ok files!")
        self.metadata = None
        self.required_headers = headers

    def load_metadata(self):
        with open(self.filename) as f:
            metadata = json.loads(f.read())
            self.metadata = metadata
            
    def get_metadata(self):
        if self.metadata == None:
            print(self.required_headers)
            self.load_metadata()
        return self.metadata
    
    def validate(self):
        errors = []
        if not self._exists():
            errors.append("Folder does not exist.")
        elif not self._has_data():
            errors.append("Folder is empty.")
        if not self._valid_heeaders():
            errors.append("Headers do not match config.")
        if not self._check_metadata():
            errors.append("Error parsing metadata. Are your values filled in?")

        if len(errors) == 0:
            return True
        else:
            self._set_status(".error")
            with open(self.filename, 'w') as f:
                for error in errors:
                    f.write(error)
                f.write(self._build_default_metadata())
            return False

    
    def _valid_heeaders(self):
        if self.metadata is None:
            return False
        headers = list(self.metadata.keys())
        headers.sort()
        self.required_headers.sort()
        return headers == self.required_headers
    
    def _set_status(self, new_status):
        new_name = f"{self.name}{new_status}"
        os.rename(self.filename, new_name)
        self.status = new_status
        self.filename = new_name
    
    def _exists(self):
        return os.path.exists(self.name)
    
    def _has_data(self):
        return len(os.listdir(self.name)) > 0
    
    def _check_metadata(self):
        if self.metadata == None:
            self.load_metadata()
        if self.metadata == None:
            return False
        for key in self.required_headers:
            result = self.metadata.get(key)
            if result == None or result == "" or result == "Input this value.":
                return False
        return True
    
    def _build_default_metadata(self):
        data = {}
        for key in self.required_headers:
            if self.metadata is not None and self.metadata.get(key) is not None:
                data.update({key: self.metadata.get(key)})
            else:
                data.update({key:"Input this value."})
        return json.dumps(data)