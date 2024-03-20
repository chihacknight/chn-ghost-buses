import logging
import pickle
import json
import functools
import inspect

from pathlib import Path

current_dir = Path(__file__).parent
PERSIST_PATH = current_dir / "scratch"


class FileManager:
    """
    Enables memoizing function calls across program runs by storing data to
    pickled files. This is like functools.lru_cache, but with file storage functionality.
    """
    def __init__(self):
        self.d = {}
        self.index = {}

    def store(self, method_name, key, value):
        """ Stores an item in the cache.

        Keys map one-way to filenames: each distinct key must
        map to a distinct filename, but it's not necessary to be able to map a filename back
        to the key because the filename is stored in a json-serialized index.

        Args:
            method_name:
            key:
            value:
        """
        logging.info(f'Storing in cache: {method_name}, {key}')
        self.d.setdefault(method_name, {})[key] = value
        self.serialize(method_name, key, value)

    def retrieve(self, method_name, key):
        """ Retrieves an item from the cache if it exists.

        Args:
            method_name:
            key:

        Returns: cache item
        """
        assert hash(method_name)
        assert hash(key)
        result = self.d.get(method_name, {}).get(key)
        logging.info(f'Retrieving from cache: {method_name}, {key}. Exists: {result is not None}')
        return result


    @staticmethod
    def index_filepath():
        return PERSIST_PATH / 'index.json'

    def serialize(self, method_name, k, v):
        keyname = '.'.join(k)
        filepath = PERSIST_PATH / method_name
        filepath.mkdir(parents=True, exist_ok=True)
        filename = f'{keyname}.pickle'
        self.index.setdefault(method_name, {})[filename] = tuple(k)
        with (filepath / filename).open('wb') as pf:
            pickle.dump(v, pf)
        with self.index_filepath().open('w') as jsonfile:
            json.dump(self.index, jsonfile, indent=4)

    def deserialize(self):
        if not self.index_filepath().exists():
            return
        with self.index_filepath().open() as json_index:
            self.index = json.load(json_index)
        for method_name, files in self.index.items():
            for filename, key in files.items():
                filepath = PERSIST_PATH / method_name / filename
                v = pickle.load(filepath.open('rb'))
                self.d.setdefault(method_name, {})[tuple(key)] = v


global_filemanager = FileManager()
global_filemanager.deserialize()


def memoize(f):
    """
    Decorator to enable file-based memoization.

    Decorated functions must be deterministic, and returned values should only
    depend on arguments to the function itself. Class methods may be decorated
    with this, but the caller must ensure that any use of class data members
    does not affect the cached output.

    With this implementation, decorated functions should only have scalar arguments,
    either primitive data types or simple data classes with a defined string serialization.
    Container arguments may not work well with this.
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        method_name = f.__name__
        new_args = list(map(str, args))
        # If f is a class method, ignore the 'self' argument
        signature = inspect.signature(f)
        arg_list = list(signature.parameters.items())
        if arg_list and arg_list[0][0] == 'self':
            new_args = list(map(str, args[1:]))
        for k, v in kwargs.items():
            new_args.append(k)
            new_args.append(v)
        if new_args:
            key = tuple(new_args)
        else:
            key = tuple(('no_arguments',))
        cached = global_filemanager.retrieve(method_name, key)
        if cached is not None:
            return cached
        result = f(*args, **kwargs)
        global_filemanager.store(method_name, key, result)
        return result
    return wrapper
