from pathlib import Path
import pickle


class DataManager:
    def __enter__(self):
        return self

    def __exit__(self, ex_type, message, traceback):
        self._update()

    def __getitem__(self, item):
        """This method is equivalent to load(key, strict = True).

        Args:
            item (str): The key of the object to read.

        Raises:
            ValueError: Thrown when strict is true and the key does not exist.

        Returns:
            object: The saved object.
        """
        return self.load(item, True)

    def __init__(self, file_name, *path, create_newly=True):
        """To assist in the data management of the object by the pickle.

        Args:
            file_name (str): The file name. If the extension is unknown, it is treated as "pkl".
            path (optional): File saving path.
                If not specified, it is set to the same directory as the executable file.
                If you pass multiple strings, it corresponds to the directory hierarchy.
            create_newly (bool, optional):
                Even if a management file with the same name already exists, set it to true if you want to overwrite it with a new file.
                If the management file does not exist, this option does not matter.

        Examples:
            >>> from datamanager import DataManager
            >>> with DataManager('test.pkl', 'Data', 'pkl') as dm:  # "./Data/pkl/test.pkl" is created.
            >>>     dm.add(exsapmle1=1, exsapmle2=2)
            >>>     dm.load('exsample1')
            1
            >>>     dm.rewrite('exsample1', 3)
            >>>     dm.loads('exsample2', 'exsample1')
            [2, 3]
        """
        self._setup(file_name, path, create_newly)

    def __setitem__(self, key, value):
        """This method is equivalent to rewrite(key, value).

        Args:
            key (str): The key to be rewritten.
            value (object): The object corresponding to that key.
        """
        self.rewrite(key, value)

    def _get_generator(self):
        """Acquires a generator that retrieves data stored in a management file one by one.
        """
        if not self.__file.exists():
            return
        with open(self.__file, 'rb') as f:
            while True:
                try:
                    yield pickle.load(f)
                except EOFError:
                    break

    def _setup(self, file_name, path, create_newly):
        """Perform various setup processes.

        This method is called automatically and should not be used.

        Args:
            file_name (str): The file name.
                Please refer to the __init__ method for more information.
            path (tuple): File saving path.
                Please refer to the __init__ method for more information.
            create_newly (bool): Even if a management file with the same name already exists, set it to true if you want to overwrite it with a new file.
        """
        self._setup_path(path)
        self._setup_file(file_name)
        self._setup_keys(create_newly)

    def _setup_file(self, file_name):
        """The name of the management file, and set the path.

        This method is called automatically and should not be used.

        Args:
            file_name (str): The file name.
                Please refer to the __init__ method for more information.
        """
        exp = get_extension(file_name)
        if exp is None:
            exp = 'pkl'
            if file_name[-1] != '.':
                exp = f'.{exp}'
            file_name += exp
        self.__file_name = file_name
        self.__file = self.__path / self.__file_name

    def _setup_keys(self, create_newly):
        """Sets a list of keys that already exist.

        This method is called automatically and should not be used.

        Args:
            create_newly (bool): Even if a management file with the same name already exists, set it to true if you want to overwrite it with a new file.
        """
        if not create_newly:
            try:
                keys = []
                for dict_ in self._get_generator():
                    for key in dict_:
                        keys.append(key)
            except Exception:
                keys = []
        else:
            keys = []
        self.__keys = keys
        self._update()

    def _setup_path(self, path):
        """Set the save path of the management file.

        This method is called automatically and should not be used.

        Args:
            path (tuple): File saving path.
                Please refer to the __init__ method for more information.

        Raises:
            e: Thrown when a file with the same name as the directory to be created exists.
        """
        if path:
            path_ = Path(*path)
        else:
            path_ = Path()
        try:
            path_.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise e
        self.__path = path_

    def _update(self):
        """To maintain the integrity of the management file, and update.

        This method is called automatically and should not be used.
        """
        tmp = self.__path / 'tmp.pkl'
        with open(tmp, 'wb') as f:
            for dict_ in self._get_generator():
                for key in dict_:
                    if key in self.__keys:
                        f.write(pickle.dumps(dict_))
        with open(tmp, 'rb') as i, open(self.__file, 'wb') as o:
            o.write(i.read())
        tmp.unlink()

    def add(self, **kwargs):
        """Save the objects in the management file.

        It is not possible to add a key that already exists.
        If you want to rewrite an existing key, use the rewrite method.

        Examples:
            >>> from datamanager import DataManager
            >>> with DataManager('test.pkl') as dm:
            >>>     dm.add(example="textcontent", example2=1)  # succeeds.
            >>>     dm.add(example=1)  # fails.
            >>> dm = DataManager('test.pkl')
            >>> print(dm.load('example'))
            textcontent
        """
        for key in kwargs:
            if key in self.__keys:
                err = (
                    f"Failed to add key({key}).", "Key that already exists.",
                    "Please use the \"rewrite\" method if you want to override.\n"
                )
                print("\n".join(err))
                continue
            try:
                with open(self.__file, 'ab') as f:
                    pickle.dump({key: kwargs[key]}, f)
                self.__keys.append(key)
            except Exception as e:
                err = f"Failed to add key.({key})"
                print(err)
                print(e)

    def exists(self, key):
        """Returns whether the key is included in the management file.

        Args:
            key (str): The key you want to check.

        Returns:
            bool: True if the key exists.
        """
        return key in self.__keys

    def load(self, key, strict=True):
        """True if the key exists.

        Behavior when the key does not exist is divided into the following.
        If strict is true, ValueError is thrown.
        Otherwise None is returned.

        Args:
            key (str): The key of the object to read.
            strict (bool, optional): True if strict mode is enabled. Defaults to True.

        Raises:
            ValueError: Thrown when strict is true and the key does not exist.

        Returns:
            object: The saved object.
        """
        if key in self.__keys:
            for dict_ in self._get_generator():
                if key in dict_:
                    return dict_[key]
        err = f"Failed to read key({key})."
        if strict:
            raise ValueError(err)
        else:
            print(err)
            return None

    def loads(self, *targets, strict=True):
        """Load multiple objects at once.

        It is guaranteed that the objects will be returned in key order.
        Do not give duplicate keys to the target.
        If strict is false and a nonexistent key is included, None is passed in that position.

        Args:
            targets: The key group of the object to read.
            strict (bool, optional): Whether to throw an exception if a non-existent key is included. Defaults to True.

        Raises:
            ValueError: When no target is set.
            ValueError: If the target contains duplicate keys.
            ValueError: strict is true and a nonexistent key is included.

        Returns:
            list: Object group.
        """
        err = None
        if not targets:
            err = "Please set the key one or more."
        elif len(targets) != len(list(set(targets))):
            err = "Key is a duplicate."
        elif not all(map(lambda x: x in self.__keys, targets)):
            if strict:
                err = f"It contains a key that does not exist in the target({targets})."
        if err is not None:
            raise ValueError(err)
        objects = {x: None for x in range(len(targets))}
        for dict_ in self._get_generator():
            for key in dict_:
                if key in targets:
                    index = targets.index(key)
                    objects[index] = dict_[key]
        return [x[1] for x in sorted(objects.items(), key=lambda x: x[0])]

    def remove(self, key):
        """Delete the object of the key from the management file.

        If you specify a non-existent key, nothing will happen.
        This is if there is no key in the management file, the method because it can be said to have played a role.

        Args:
            key (str): It is key that you want to delete.
        """
        if key in self.__keys:
            index = self.__keys.index(key)
            del self.__keys[index]
            self._update()

    def rewrite(self, key, value, *, should_add=True):
        """Already it overwrites the data of the key that exists.

        Because there is a risk of damaging the data you have saved, you can not be rewritten more than at the same time.

        Args:
            key (str): The key to be rewritten.
            value (object): The object corresponding to that key.
            should_add (bool, optional): Whether to add the target key if it does not exist. Defaults to True.

        Examples:
            >>> from datamanager import DataManager
            >>> with DataManager('test.pkl') as dm:
            >>>     dm.add(example1="ex1")
            >>>     dm.rewrite('example1', 'changed')
            >>>     print(dm.load('exapmle1'))
            changed
            >>>     dm.rewrite('example2', 1)
            >>>     print(dm.load('example2'))
            1
            >>> dm.rewrite('example3', 3, should_add=False)  # Not added.
            >>> print(dm.load('example3'))
            ValueError: Failed to read key(example3).

        """
        if key in self.__keys or should_add:
            self.remove(key)
            data = {key: value}
            self.add(**data)

    def show(self):
        """Output saved data correspondence table.

        Exapmles:
            >>> from datamanager import DataManager
            >>> with DataManager('test.pkl') as dm:
            >>>     dm.add(example1='ex1', example2='ex2')
            >>>     dm.show()
            example1: ex1
            example2: ex2
        """
        for obj in self._get_generator():
            for d in obj.items():
                key, value = d
                print(f"{key}: {value}")


def get_extension(file):
    """Get the extension from a string or path object.

    If the extension cannot be confirmed, None is returned.

    Args:
        file (str or Path): String or path object for the file name.

    Returns:
        str or None: Extension or None.
    """
    split_file_name = str(file).split('.')
    len_ = len(split_file_name)
    if len_ == 1 or not split_file_name[-1]:
        return None
    return split_file_name[-1]


def main():
    print("This script is not main module.")


if __name__ == '__main__':
    main()
